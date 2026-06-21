"""
# 네이버 검색광고 다차원 보고서 ETL 파이프라인

## 인증(Credentials)
네이버 검색광고 API 인증 키(액세스라이선스, 비밀키)와 CUSTOMER_ID가 필요하다.

## 추출(Extract)
실행 시점(data_interval_end)에서 1일 전을 기준으로,
대용량 보고서를 생성, 조회, 삭제하는 API를 순차적으로 실행하면서
광고성과 및 전환 보고서를 수집한다.

## 변환(Transform)
TSV 형식의 응답 본문을 파싱하고, 하나의 다차원 보고서로 병합해 DuckDB 테이블에 적재한다.

## 적재(Load)
- 보고서 데이터를 BigQuery/Postgres 테이블 끝에 추가한다.
- 적재 과정에서 수집한 광고 성과일 파티션 범위를 바탕으로 후속 dbt 모델을 실행한다.

> 주의) 2026년 03월 30일(월)부터 모든 COST에 VAT가 포함된다.
"""

from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from cosmos import DbtTaskGroup
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "searchad_report_sad",
    schedule = "40 5 * * *",
    start_date = pendulum.datetime(2025, 8, 24, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:searchad", "objective:ads", "credentials:api-key",
        "schedule:daily", "time:morning", "write:append", "plugin:dbt"
    ],
) as dag:

    PATH = "searchad.api.report"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    @task(task_id="etl_searchad_report_sad", map_index_template="{{ credentials['customer_id'] }}")
    def etl_searchad_report_sad(credentials: dict, configs: dict, **kwargs) -> dict:
        from airflow_utils import format_datetime
        return main_searchad(**credentials, date=format_datetime(kwargs, subdays=1), **configs)

    def main_searchad(
            api_key: str,
            secret_key: str,
            customer_id: int | str,
            date: str,
            tables: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.searchad.api import advanced_report
        from dual_load import load_table_from_duckdb
        source = "searchad_report"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            advanced_report(
                api_key = api_key,
                secret_key = secret_key,
                customer_id = customer_id,
                start_date = date,
                end_date = date,
                connection = conn,
                return_type = "none",
            )

            return {
                "context": {
                    "partitions": sorted(map(str, conn.unique(source, "ymd"))),
                },
                "params": {
                    "customer_id": customer_id,
                    "date": date,
                },
                "result": load_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables["table"],
                )
            }


    @task(task_id="generate_dbt_date_range", trigger_rule="all_done")
    def generate_dbt_date_range(results: list[dict]) -> dict:
        from dbt_cosmos import generate_dbt_date_range as generate
        return generate(results, "context.partitions")


    @task.short_circuit(task_id="prepare_dbt_run")
    def prepare_dbt_run(ti: TaskInstance, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_dbt_date_range")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False


    def dbt_bigquery_searchad_report_sad_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_searchad_report_sad",
            selector = "searchad_report_sad",
            ds_task_id = "generate_dbt_date_range",
        )


    @task(task_id="finalize_dag_run", trigger_rule="all_done")
    def finalize_dag_run(ti: TaskInstance):
        from dbt_cosmos import raise_on_failure
        raise_on_failure(ti)


    etl_results = (etl_searchad_report_sad
        .partial(configs=read_configs())
        .expand(credentials=read_credentials()))

    dbt_date_range = generate_dbt_date_range(etl_results)
    dbt_run = dbt_bigquery_searchad_report_sad_group()

    dbt_date_range >> prepare_dbt_run() >> dbt_run >> finalize_dag_run()
