"""
# 네이버 검색광고 계약 정보 ETL 파이프라인

## 인증(Credentials)
네이버 검색광고 API 인증 키(액세스라이선스, 비밀키)와 CUSTOMER_ID가 필요하다.

## 추출(Extract)
브랜드/신제품검색 계약 API를 통해 계정별 모든 계약을 수집한다.

## 변환(Transform)
계약 유형별로 JSON 형식의 응답 본문을 파싱하여 각각의 DuckDB 테이블에 적재한 뒤,
UNION ALL로 하나의 테이블로 병합한다.

## 적재(Load)
- 병합된 테이블의 데이터를 기존 BigQuery/Postgres 테이블을 지우고 덮어쓴다.
- Dag run 실행일 기준 전일부터 현재까지의 기간을 바탕으로 후속 dbt 모델을 실행한다.
"""

from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from cosmos import DbtTaskGroup
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "searchad_contract",
    schedule = "30 5 * * 1-5",
    start_date = pendulum.datetime(2025, 10, 30, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=5),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:medium", "platform:searchad", "objective:ads", "credentials:api-key",
        "schedule:weekdays", "time:morning", "write:overwrite", "plugin:dbt"
    ],
) as dag:

    PATH = "searchad.api.contract"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    @task(task_id="etl_searchad_contract", map_index_template="{{ queries['customer_id'] }}")
    def etl_searchad_contract(queries: dict, configs: dict, **kwargs) -> dict:
        from airflow_utils import format_datetime, today
        from linkmerce.utils.date import date_range
        partitions = date_range(format_datetime(kwargs, subdays=1), str(today().date()))
        context = {"context": {"partitions": partitions}}
        return context | main(**queries, **configs)

    def main(
            api_key: str,
            secret_key: str,
            customer_id: int | str,
            tables: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.searchad.api import time_contract, brand_new_contract
        from dual_load import overwrite_table_from_duckdb
        sources = {
            "time": "searchad_contract",
            "brand_new": "searchad_contract_new",
            "contract": "searchad_contract_total",
        }

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            time_contract(
                api_key = api_key,
                secret_key = secret_key,
                customer_id = customer_id,
                connection = conn,
                return_type = "none",
            )

            brand_new_contract(
                api_key = api_key,
                secret_key = secret_key,
                customer_id = customer_id,
                connection = conn,
                return_type = "none",
            )

            conn.execute(dedent(f"""
                CREATE OR REPLACE TABLE {sources['contract']} AS
                SELECT * FROM {sources['time']}
                UNION ALL
                SELECT * FROM {sources['brand_new']}
                """).strip())

            return {
                "params": {
                    "customer_id": customer_id,
                },
                "result": overwrite_table_from_duckdb(
                    connection = conn,
                    source_table = sources["contract"],
                    target_table = tables["table"],
                    where_clause = f"customer_id = {customer_id}",
                )
            }


    @task(task_id="generate_dbt_date_range")
    def generate_dbt_date_range(results: list[dict]) -> dict:
        from dbt_cosmos import generate_dbt_date_range as generate
        return generate(results, "context.partitions")


    @task.short_circuit(task_id="prepare_dbt_run")
    def prepare_dbt_run(ti: TaskInstance, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_dbt_date_range")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False


    def dbt_bigquery_searchad_contract_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_searchad_contract",
            selector = "searchad_contract",
            ds_task_id = "generate_dbt_date_range",
        )


    etl_results = (etl_searchad_contract
        .partial(configs=read_configs())
        .expand(queries=read_credentials()))

    dbt_date_range = generate_dbt_date_range(etl_results)
    dbt_run = dbt_bigquery_searchad_contract_group()

    dbt_date_range >> prepare_dbt_run() >> dbt_run
