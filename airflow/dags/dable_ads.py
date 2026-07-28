"""
# 데이블 광고 보고서 ETL 파이프라인

## 인증(Credentials)
데이블 설정에서 발급한 API KEY와 URL을 구성하는 클라이언트 명칭이 필요하다.

## 추출(Extract)
실행 시점(data_interval_end)에서 1일 전을 기준으로 광고 보고서와 캠페인 목록을 가져온다.

## 변환(Transform)
JSON 형식의 응답 본문을 파싱하여 광고 보고서와 캠페인 목록을 각각의 DuckDB 테이블에 적재한다.

## 적재(Load)
- 광고 보고서는 대응되는 BigQuery/Postgres 테이블 끝에 추가한다.
- 캠페인 목록은 기존 BigQuery/Postgres 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
- 적재 과정에서 수집한 광고 보고서의 날짜 파티션 범위를 바탕으로 후속 dbt 모델을 실행한다.
"""

from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from cosmos import DbtTaskGroup
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "dable_ads",
    schedule = "30 7 * * *",
    start_date = pendulum.datetime(2026, 7, 29, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:dable", "objective:ads", "credentials:api-key",
        "schedule:daily", "time:morning", "write:append", "write:merge", "plugin:dbt",
    ],
) as dag:

    PATH = "dable.api.report"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, credentials="expand", tables=True)


    @task(task_id="etl_dable_ads")
    def etl_dable_ads(ti: TaskInstance, **kwargs) -> dict:
        from airflow_utils import format_datetime
        configs = ti.xcom_pull(task_ids="read_configs")
        return main_dable_ads(date=format_datetime(kwargs, subdays=1), **configs)

    def main_dable_ads(
            api_key: str,
            client_name: str,
            date: str,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs,
        ) -> dict:
        from dual_load import load_table_from_duckdb, merge_table_from_duckdb
        from linkmerce.api.dable.api import daily_report
        from linkmerce.common.load import DuckDBConnection
        sources = {"report": "dable_report", "campaign": "dable_campaign"}

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            daily_report(
                api_key = api_key,
                client_name = client_name,
                start_date = date,
                end_date = date,
                group_by_campaign = True,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            return {
                "context": {
                    "partitions": sorted(map(str, conn.unique(sources["report"], "ymd"))),
                },
                "params": {
                    "date": date,
                },
                "results": {
                    "report": load_table_from_duckdb(
                        connection = conn,
                        source_table = sources["report"],
                        target_table = tables["report"],
                    ),
                    "campaign": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["campaign"],
                        target_table = tables["campaign"],
                        **merge["campaign"],
                    ),
                },
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


    def dbt_bigquery_dable_ads_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_dable_ads",
            selector = "dable_ads",
            ds_task_id = "generate_dbt_date_range",
        )


    etl_result = etl_dable_ads()

    dbt_date_range = generate_dbt_date_range(etl_result)
    dbt_run = dbt_bigquery_dable_ads_group()

    read_configs() >> etl_result
    dbt_date_range >> prepare_dbt_run() >> dbt_run
