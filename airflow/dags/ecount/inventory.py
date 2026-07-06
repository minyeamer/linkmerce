"""
# 이카운트 창고별/품목별 재고현황 ETL 파이프라인

## 인증(Credentials)
이카운트 API 인증 키(회사코드, 사용자ID, API 키)가 필요하다.

## 추출(Extract)
매일 오전/오후 재고 업데이트 시간에 맞춰 이카운트 API로 재고현황 데이터를 수집한다.

## 변환(Transform)
JSON 형식의 응답 본문을 파싱하여 DuckDB 테이블에 적재한다.

## 적재(Load)
- 데이터를 BigQuery/Postgres 테이블의 끝에 추가한다.
- 적재 과정에서 수집한 날짜 파티션 범위를 바탕으로 후속 dbt 모델을 실행한다.
"""

from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from cosmos import DbtTaskGroup
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "ecount_inventory",
    schedule = MultipleCronTriggerTimetable(
        "0 11 * * *",
        "30 17 * * *",
        timezone = "Asia/Seoul",
    ),
    start_date = pendulum.datetime(2026, 5, 27, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:ecount", "objective:stock", "credentials:api-key",
        "schedule:daily", "time:morning", "time:afternoon", "write:append", "plugin:dbt"
    ],
) as dag:

    PATH = "ecount.api.inventory"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, credentials="expand", tables=True)


    @task(task_id="etl_ecount_inventory")
    def etl_ecount_inventory(ti: TaskInstance, **kwargs) -> dict:
        from airflow_utils import format_datetime
        return main(base_date=format_datetime(kwargs), **ti.xcom_pull(task_ids="read_configs"))

    def main(
            com_code: int | str,
            userid: str,
            api_key: str,
            base_date: str,
            tables: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.ecount.api import inventory
        from dual_load import load_table_from_duckdb
        source = "ecount_inventory"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            inventory(
                com_code = com_code,
                userid = userid,
                api_key = api_key,
                base_date = base_date,
                zero_yn = True,
                connection = conn,
                return_type = "none",
            )

            return {
                "context": {
                    "partitions": sorted(map(str, conn.unique(source, "DATE(updated_at)"))),
                },
                "params": {
                    "com_code": com_code,
                    "base_date": base_date,
                    "zero_yn": True,
                },
                "result": load_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables["table"],
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


    def dbt_bigquery_ecount_inventory_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_ecount_inventory",
            selector = "ecount_inventory",
            ds_task_id = "generate_dbt_date_range",
        )


    etl_result = etl_ecount_inventory()

    dbt_date_range = generate_dbt_date_range(etl_result)
    dbt_run = dbt_bigquery_ecount_inventory_group()

    read_configs() >> etl_result
    dbt_date_range >> prepare_dbt_run() >> dbt_run
