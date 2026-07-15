"""
# 구글 광고 ETL 파이프라인

## 인증(Credentials)
구글 광고 API 인증 정보(개발자 토큰, 계정 ID, 매니저 계정 ID)와
GCP 서비스 계정이 필요하다.

## 추출(Extract)
계정별 캠페인, 광고그룹, 소재, 애셋 목록을 수집하고,
추가로 실행 시점(data_interval_end)에서 1일 전을 기준으로 소재의 성과 데이터를 가져온다.

## 변환(Transform)
JSON 형식의 응답 본문을 파싱하여 캠페인, 광고그룹, 소재, 애셋,
그리고 성과 데이터에 대한 각각의 DuckDB 테이블에 적재한다.

## 적재(Load)
각각의 캠페인, 광고그룹, 소재, 애셋 테이블을 기존 BigQuery/Postgres 테이블과
MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
- 소재 성과 테이블은 대응되는 BigQuery/Postgres 테이블 끝에 추가한다.
- 적재 과정에서 수집한 광고 성과일 파티션 범위를 바탕으로 후속 dbt 모델을 실행한다.
"""

from airflow.sdk import DAG, task
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.models.taskinstance import TaskInstance
from cosmos import DbtTaskGroup
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "google_ads",
    schedule = "50 7 * * *",
    start_date = pendulum.datetime(2026, 2, 26, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:google-ads", "objective:ads", "credentials:service-account",
        "schedule:daily", "time:morning", "write:append", "write:merge", "plugin:dbt"
    ],
) as dag:

    PATH = "google.api.ads"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    @task(task_id="etl_google_campaign", map_index_template="{{ credentials['customer_id'] }}")
    def etl_google_campaign(credentials: dict, configs: dict, **kwargs) -> dict:
        return main_object(ad_level="campaign", **credentials, **configs)

    @task(task_id="etl_google_adgroup", map_index_template="{{ credentials['customer_id'] }}")
    def etl_google_adgroup(credentials: dict, configs: dict, **kwargs) -> dict:
        return main_object(ad_level="adgroup", **credentials, **configs)

    @task(task_id="etl_google_ad", map_index_template="{{ credentials['customer_id'] }}")
    def etl_google_ad(credentials: dict, configs: dict, **kwargs) -> dict:
        return main_object(ad_level="ad", **credentials, **configs)

    @task(task_id="etl_google_asset", map_index_template="{{ credentials['customer_id'] }}")
    def etl_google_asset(credentials: dict, configs: dict, **kwargs) -> dict:
        return main_object(ad_level="asset", **credentials, **configs)

    def main_object(
            ad_level: str,
            customer_id: int | str,
            manager_id: int | str,
            developer_token: str,
            service_account: dict,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from dual_load import merge_table_from_duckdb
        from importlib import import_module
        extract = getattr(import_module("linkmerce.api.google.api"), ad_level)
        date_range = dict() if ad_level == "asset" else {"date_range": "LAST_30_DAYS"}
        source = f"google_{ad_level}"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            extract(
                customer_id = customer_id,
                manager_id = manager_id,
                developer_token = developer_token,
                service_account = service_account,
                **date_range,
                connection = conn,
                return_type = "none",
            )

            return {
                "params": {
                    "ad_level": ad_level,
                    **date_range,
                },
                "result": merge_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables[ad_level],
                    **merge[ad_level],
                )
            }


    @task(task_id="etl_google_insight", map_index_template="{{ credentials['customer_id'] }}")
    def etl_google_insight(credentials: dict, configs: dict, **kwargs) -> dict:
        from airflow_utils import format_datetime
        return main_insight(**credentials, date=format_datetime(kwargs, subdays=1), **configs)

    def main_insight(
            customer_id: int | str,
            manager_id: int | str,
            developer_token: str,
            date: str,
            service_account: dict,
            tables: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.google.api import insight
        from dual_load import load_table_from_duckdb
        source = "google_insight"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            insight(
                customer_id = customer_id,
                manager_id = manager_id,
                developer_token = developer_token,
                service_account = service_account,
                start_date = date,
                end_date = date,
                connection = conn,
                progress = False,
                return_type = "none",
            )
            partitions = sorted(map(str, conn.unique(source, "ymd")))

            return {
                "context": {
                    "partitions": partitions,
                },
                "params": {
                    "date": date,
                },
                "result": load_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables["insight"],
                )
            }


    @task(task_id="generate_dbt_date_range", trigger_rule="all_done")
    def generate_dbt_date_range(results: list[dict]) -> dict:
        from dbt_cosmos import generate_dbt_date_range as generate
        return generate(results, "context.partitions")


    @task.short_circuit(task_id="prepare_dbt_run", ignore_downstream_trigger_rules=False)
    def prepare_dbt_run(ti: TaskInstance, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_dbt_date_range")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False


    def dbt_bigquery_google_ads_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_google_ads",
            selector = "google_ads",
            ds_task_id = "generate_dbt_date_range",
        )


    @task(task_id="finalize_dag_run", trigger_rule="all_done")
    def finalize_dag_run(ti: RuntimeTaskInstance):
        from dbt_cosmos import raise_on_failure
        raise_on_failure(ti)


    configs = read_configs()
    credentials = read_credentials()

    etl_campaign_results = (etl_google_campaign
    .partial(configs=configs)
    .expand(credentials=credentials))

    etl_adgroup_results = (etl_google_adgroup
    .partial(configs=configs)
    .expand(credentials=credentials))

    etl_ad_results = (etl_google_ad
    .partial(configs=configs)
    .expand(credentials=credentials))

    etl_asset_results = (etl_google_asset
    .partial(configs=configs)
    .expand(credentials=credentials))

    etl_insight_results = (etl_google_insight
    .partial(configs=configs)
    .expand(credentials=credentials))

    etl_object_results = [etl_campaign_results, etl_adgroup_results, etl_ad_results, etl_asset_results]
    etl_object_results >> etl_insight_results

    dbt_date_range = generate_dbt_date_range(etl_insight_results)
    dbt_run = dbt_bigquery_google_ads_group()

    dbt_date_range >> prepare_dbt_run() >> dbt_run >> finalize_dag_run()
