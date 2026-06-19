"""
# 네이버 검색광고 마스터 보고서 ETL 파이프라인

## 인증(Credentials)
네이버 검색광고 API 인증 키(액세스라이선스, 비밀키)와 CUSTOMER_ID가 필요하다.

## 추출(Extract)
마스터 보고서를 생성, 조회, 삭제하는 API를 순차적으로 실행하면서
계정별 캠페인, 광고그룹, 소재 목록을 수집한다.
~~(첫 번째 계정에 한해 광고매체 목록을 추가로 수집한다.)~~

## 변환(Transform)
TSV 형식의 응답 본문을 파싱하여 DuckDB 테이블에 적재한다.

## 적재(Load)
- 각각의 데이터를 대응되는 BigQuery/Postgres 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
- Dag run 실행일 기준 전일부터 현재까지의 기간을 바탕으로 후속 dbt 모델을 실행한다.
"""

from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from cosmos import DbtTaskGroup
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "searchad_master_sad",
    schedule = "40 23 * * 1-5",
    start_date = pendulum.datetime(2025, 8, 30, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:medium", "platform:searchad", "objective:ads", "credentials:api-key",
        "schedule:weekdays", "time:night", "write:merge", "plugin:dbt"
    ],
) as dag:

    PATH = "searchad.api.master"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        credentials = read_config(PATH, credentials=True)["credentials"]
        return [dict(info, with_media=(i == 0)) for i, info in enumerate(credentials)]


    @task(task_id="etl_searchad_master_sad", map_index_template="{{ credentials['customer_id'] }}")
    def etl_searchad_master_sad(credentials: dict, configs: dict, **kwargs) -> dict:
        from airflow_utils import format_datetime, today
        from linkmerce.utils.date import date_range
        types = ["campaign", "adgroup", "ad"] # + (["media"] if credentials.get("with_media") else list())
        partitions = date_range(format_datetime(kwargs, subdays=1), str(today().date()))
        context = {"context": {"partitions": partitions}}
        configs["from_date"] = today(subdays=(365*2)).format("YYYY-MM-DD")
        return context | {api_type: main(api_type, **credentials, **configs) for api_type in types}

    def main(
            api_type: str,
            api_key: str,
            secret_key: str,
            customer_id: int | str,
            from_date: str,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from dual_load import merge_table_from_duckdb
        from importlib import import_module
        import logging
        module = "master_ad" if api_type == "ad" else api_type
        extract = getattr(import_module("linkmerce.api.searchad.api"), module)
        source = f"searchad_{api_type}"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            extract(
                api_key = api_key,
                secret_key = secret_key,
                customer_id = customer_id,
                from_date = from_date,
                connection = conn,
                return_type = "none",
            )
            logging.info(f"[{customer_id}] API request completed for downloading the {api_type} master report")

            return {
                "params": {
                    "customer_id": customer_id,
                    "from_date": from_date,
                },
                "result": merge_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables[api_type],
                    **merge[api_type],
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


    def dbt_bigquery_searchad_master_sad_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_searchad_master_sad",
            selector = "searchad_master_sad",
            ds_task_id = "generate_dbt_date_range",
        )


    etl_results = (etl_searchad_master_sad
        .partial(configs=read_configs())
        .expand(credentials=read_credentials()))

    dbt_date_range = generate_dbt_date_range(etl_results)
    dbt_run = dbt_bigquery_searchad_master_sad_group()

    dbt_date_range >> prepare_dbt_run() >> dbt_run
