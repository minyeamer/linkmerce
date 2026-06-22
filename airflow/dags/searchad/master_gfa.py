"""
# 네이버 성과형 디스플레이 광고 캠페인/광고 그룹/소재 ETL 파이프라인

## 인증(Credentials)
성과형 디스플레이 광고 계정을 보유한 네이버 계정의 로그인 쿠키가 필요하다.

## 추출(Extract)
계정별 캠페인 목록, 광고 그룹 목록, 소재 목록을 수집한다.

## 변환(Transform)
JSON 형식의 응답 본문을 파싱하여 DuckDB 테이블에 적재한다.

## 적재(Load)
- 각각의 데이터를 대응되는 BigQuery/Postgres 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
- Dag run 실행일 기준 전일부터 현재까지의 기간을 바탕으로 후속 dbt 모델을 실행한다.
"""

from airflow.sdk import DAG, task
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.models.taskinstance import TaskInstance
from cosmos import DbtTaskGroup
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "searchad_master_gfa",
    schedule = "30 5 * * 1-5",
    start_date = pendulum.datetime(2025, 8, 30, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:medium", "platform:searchad", "objective:ads", "credentials:cookies",
        "schedule:weekdays", "time:morning", "write:merge", "plugin:dbt"
    ],
) as dag:

    PATH = "searchad.gfa.master"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    @task(task_id="etl_searchad_master_gfa", map_index_template="{{ credentials['account_no'] }}")
    def etl_searchad_master_gfa(credentials: dict, configs: dict, **kwargs) -> dict:
        from airflow_utils import format_datetime, today
        from linkmerce.utils.date import date_range
        types = ["campaign", "adset", "creative"]
        partitions = date_range(format_datetime(kwargs, subdays=1), str(today().date()))
        context = {"context": {"partitions": partitions}}
        return context | {api_type: main(api_type, **credentials, **configs) for api_type in types}

    def main(
            api_type: str,
            account_no: int | str,
            cookies: str,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from dual_load import merge_table_from_duckdb
        from importlib import import_module
        import logging
        extract = getattr(import_module("linkmerce.api.searchad.gfa"), api_type)
        status = [("RUNNABLE" if api_type == "campaign" else "ALL"), "DELETED"]
        source = f"searchad_{api_type}_gfa"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            extract(
                account_no = account_no,
                cookies = cookies,
                status = status,
                connection = conn,
                progress = False,
                return_type = "none",
            )
            logging.info(f"[{account_no}] request completed for downloading the {api_type} list")

            return {
                "params": {
                    "account_no": account_no,
                    "status": status,
                },
                "result": merge_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables[api_type],
                    **merge[api_type],
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


    def dbt_bigquery_searchad_master_gfa_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_searchad_master_gfa",
            selector = "searchad_master_gfa",
            ds_task_id = "generate_dbt_date_range",
        )


    @task(task_id="finalize_dag_run", trigger_rule="all_done")
    def finalize_dag_run(ti: RuntimeTaskInstance):
        from dbt_cosmos import raise_on_failure
        raise_on_failure(ti)


    etl_results = (etl_searchad_master_gfa
        .partial(configs=read_configs())
        .expand(credentials=read_credentials()))

    dbt_date_range = generate_dbt_date_range(etl_results)
    dbt_run = dbt_bigquery_searchad_master_gfa_group()

    dbt_date_range >> prepare_dbt_run() >> dbt_run >> finalize_dag_run()
