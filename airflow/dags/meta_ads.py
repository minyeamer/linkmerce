"""
# 메타 광고 ETL 파이프라인

## 인증(Credentials)
메타 광고 API 인증 정보인 Access Token이 필요하다.
토큰 만료 시 연장을 위해 App ID와 App Secret이 선택적으로 요구된다.

## 추출(Extract)
Access Token 권한이 있는 계정들에 대한 캠페인, 광고세트, 광고 목록을 수집하고,
추가로 실행 시점(data_interval_end)에서 1일 전을 기준으로 광고 성과 보고서를 가져온다.

## 변환(Transform)
JSON 형식의 응답 본문을 파싱하여 캠페인, 광고세트, 광고,
그리고 성과 보고서에 대한 각각의 DuckDB 테이블에 적재한다.

## 적재(Load)
각각의 캠페인, 광고세트, 광고 테이블을 기존 BigQuery/Postgres 테이블과
MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
- 성과 보고서 테이블은 대응되는 BigQuery/Postgres 테이블 끝에 추가한다.
- 적재 과정에서 수집한 광고 성과일 파티션 범위를 바탕으로 후속 dbt 모델을 실행한다.
"""

from airflow.sdk import DAG, task
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.models.taskinstance import TaskInstance
from cosmos import DbtTaskGroup
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "meta_ads",
    schedule = "40 7 * * *",
    start_date = pendulum.datetime(2026, 2, 20, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:meta-ads", "objective:ads", "credentials:api-key",
        "schedule:daily", "time:morning", "write:append", "write:merge", "plugin:dbt"
    ],
) as dag:

    PATH = "meta.api.ads"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    @task(task_id="etl_meta_campaigns", map_index_template="{{ credentials['app_id'] }}")
    def etl_meta_campaigns(credentials: dict, configs: dict, **kwargs) -> dict:
        configs = configs | {"merge": configs["merge"]["objects"]}
        return main_objects(ad_level="campaigns", **credentials, **configs)

    @task(task_id="etl_meta_adsets", map_index_template="{{ credentials['app_id'] }}")
    def etl_meta_adsets(credentials: dict, configs: dict, **kwargs) -> dict:
        configs = configs | {"merge": configs["merge"]["objects"]}
        return main_objects(ad_level="adsets", **credentials, **configs)

    @task(task_id="etl_meta_ads", map_index_template="{{ credentials['app_id'] }}")
    def etl_meta_ads(credentials: dict, configs: dict, **kwargs) -> dict:
        configs = configs | {"merge": configs["merge"]["objects"]}
        return main_objects(ad_level="ads", **credentials, **configs)

    def main_objects(
            ad_level: str,
            access_token: str,
            tables: dict[str, str],
            merge: dict[str, dict],
            account_ids: list[str] = list(),
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from dual_load import merge_table_from_duckdb
        from importlib import import_module
        extract = getattr(import_module("linkmerce.api.meta.api"), ad_level)
        source = f"meta_{ad_level}"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            extract(
                access_token = access_token,
                account_ids = account_ids,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            return {
                "params": {
                    "ad_level": ad_level,
                    "account_ids": account_ids,
                },
                "result": merge_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables[ad_level],
                    **merge[ad_level],
                )
            }


    @task(task_id="etl_meta_insights", map_index_template="{{ credentials['app_id'] }}")
    def etl_meta_insights(credentials: dict, configs: dict, **kwargs) -> dict:
        """앱 ID별 Meta 광고 일별 인사이트 지표를 수집하여 BigQuery에 적재한다."""
        from airflow_utils import format_datetime
        configs = configs | {"merge": configs["merge"]["insights"]}
        return main_insights(**credentials, date=format_datetime(kwargs, subdays=1), **configs)

    def main_insights(
            access_token: str,
            date: str,
            tables: dict[str, str],
            merge: dict[str, dict],
            account_ids: list[str] = list(),
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.meta.api import insights
        from dual_load import load_table_from_duckdb, merge_table_from_duckdb
        sources = {
            "campaigns": "meta_campaigns",
            "adsets": "meta_adsets",
            "ads": "meta_ads",
            "insights": "meta_insights",
        }

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            insights(
                access_token = access_token,
                ad_level = "ad",
                start_date = date,
                end_date = date,
                date_type = "daily",
                account_ids = account_ids,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            return {
                "context": {
                    "partitions": sorted(map(str, conn.unique(sources["insights"], "ymd"))),
                },
                "params": {
                    "ad_level": "ad",
                    "date": date,
                    "date_type": "daily",
                    "account_ids": account_ids,
                },
                "results": {
                    "campaigns": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["campaigns"],
                        target_table = tables["campaigns"],
                        **merge["campaigns"],
                    ),
                    "adsets": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["adsets"],
                        target_table = tables["adsets"],
                        **merge["adsets"],
                    ),
                    "ads": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["ads"],
                        target_table = tables["ads"],
                        **merge["ads"],
                    ),
                    "insights": load_table_from_duckdb(
                        connection = conn,
                        source_table = sources["insights"],
                        target_table = tables["insights"],
                    )
                }
            }


    configs = read_configs()
    credentials = read_credentials()

    etl_campaign_results = (etl_meta_campaigns
    .partial(configs=configs)
    .expand(credentials=credentials))

    etl_adset_results = (etl_meta_adsets
    .partial(configs=configs)
    .expand(credentials=credentials))

    etl_ad_results = (etl_meta_ads
    .partial(configs=configs)
    .expand(credentials=credentials))

    etl_insight_results = (etl_meta_insights
    .partial(configs=configs)
    .expand(credentials=credentials))

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


    def dbt_bigquery_meta_ads_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_meta_ads",
            selector = "meta_ads",
            ds_task_id = "generate_dbt_date_range",
        )


    @task(task_id="finalize_dag_run", trigger_rule="all_done")
    def finalize_dag_run(ti: RuntimeTaskInstance):
        from dbt_cosmos import raise_on_failure
        raise_on_failure(ti)


    etl_results = [etl_campaign_results, etl_adset_results, etl_ad_results, etl_insight_results]

    dbt_date_range = generate_dbt_date_range(etl_results)
    dbt_run = dbt_bigquery_meta_ads_group()

    dbt_date_range >> prepare_dbt_run() >> dbt_run >> finalize_dag_run()
