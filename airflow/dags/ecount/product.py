"""
# 이카운트 품목 리스트 ETL 파이프라인

## 인증(Credentials)
이카운트 API 인증 키(회사코드, 사용자ID, API 키)가 필요하다.

## 추출(Extract)
매일 오전/오후 재고 업데이트 시간에 맞춰 이카운트 API로 품목등록 리스트를 수집한다.

## 변환(Transform)
JSON 형식의 응답 본문을 파싱하여 DuckDB 테이블에 적재한다.

## 적재(Load)
- 기존 BigQuery/Postgres 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
- Dag run 실행일 기준 전일부터 현재까지의 기간을 바탕으로 후속 dbt 모델을 실행한다.
"""

from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from cosmos import DbtTaskGroup
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "ecount_product",
    schedule = MultipleCronTriggerTimetable(
        "50 10 * * 1-5",
        "20 17 * * 1-5",
        timezone = "Asia/Seoul",
    ),
    start_date = pendulum.datetime(2026, 5, 26, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:medium", "platform:ecount", "objective:product", "credentials:api-key",
        "schedule:daily", "time:morning", "time:afternoon", "write:merge", "plugin:dbt"
    ],
) as dag:

    PATH = "ecount.api.product"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, credentials="expand", tables=True)


    @task(task_id="etl_ecount_product")
    def etl_ecount_product(ti: TaskInstance, **kwargs) -> dict:
        from airflow_utils import format_datetime, today
        from linkmerce.utils.date import date_range
        partitions = date_range(format_datetime(kwargs, subdays=1), str(today().date()))
        context = {"context": {"partitions": partitions}}
        return context | main(**ti.xcom_pull(task_ids="read_configs"))

    def main(
            com_code: int | str,
            userid: str,
            api_key: str,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.ecount.api import product
        from dual_load import merge_table_from_duckdb
        source = "ecount_product"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            product(
                com_code = com_code,
                userid = userid,
                api_key = api_key,
                comma_yn = True,
                connection = conn,
                return_type = "none",
            )

            return {
                "params": {
                    "com_code": com_code,
                    "comma_yn": True,
                },
                "result": merge_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables["table"],
                    **merge["table"],
                )
            }


    @task(task_id="generate_dbt_date_range")
    def generate_dbt_date_range(result: dict) -> dict:
        from dbt_cosmos import generate_dbt_date_range as generate
        return generate(result, "context.partitions")


    @task.short_circuit(task_id="prepare_dbt_run")
    def prepare_dbt_run(ti: TaskInstance, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_dbt_date_range")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False


    def dbt_bigquery_ecount_product_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_ecount_product",
            selector = "ecount_product",
            ds_task_id = "generate_dbt_date_range",
        )


    etl_result = etl_ecount_product()

    dbt_date_range = generate_dbt_date_range(etl_result)
    dbt_run = dbt_bigquery_ecount_product_group()

    read_configs() >> etl_result
    dbt_date_range >> prepare_dbt_run() >> dbt_run
