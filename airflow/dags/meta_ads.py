from airflow.sdk import DAG, task
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "meta_ads",
    schedule = "40 7 * * *",
    start_date = pendulum.datetime(2026, 2, 20, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    tags = ["priority:high", "meta:ads", "api:meta", "schedule:daily", "time:morning"],
    doc_md = dedent("""
        # 메타 광고 ETL 파이프라인

        ## 인증(Credentials)
        메타 광고 API 인증 정보인 Access Token이 필요하다.
        토큰 만료 시 연장을 위해 App ID와 App Secret이 선택적으로 요구된다.

        ## 추출(Extract)
        Access Token 권한이 있는 계정들에 대한 캠페인, 광고세트, 광고 목록을 수집하고,
        추가로 실행 시점(data_interval_end)에서 1일 전을 기준으로 광고의 성과 데이터를 가져온다.

        ## 변환(Transform)
        JSON 형식의 응답 본문을 파싱하여 캠페인, 광고세트, 광고,
        그리고 성과 데이터에 대한 각각의 DuckDB 테이블에 적재한다.

        ## 적재(Load)
        각각의 캠페인, 광고세트, 광고 테이블을 기존 BigQuery 테이블과
        MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
        광고 성과 테이블은 대응되는 BigQuery 테이블 끝에 추가한다.
    """).strip(),
) as dag:

    PATH = "meta.api.ads"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True, service_account=True)

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
            app_id: str,
            app_secret: str,
            service_account: dict,
            tables: dict[str, str],
            merge: dict[str, dict],
            account_ids: list[str] = list(),
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.extensions.bigquery import BigQueryClient
        from importlib import import_module
        extract = getattr(import_module("linkmerce.api.meta.api"), ad_level)
        source = f"meta_{ad_level}"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            extract(
                access_token = access_token,
                app_id = app_id,
                app_secret = app_secret,
                account_ids = account_ids,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "ad_level": ad_level,
                        "account_ids": account_ids,
                    },
                    "counts": {
                        "table": conn.count_table(source),
                    },
                    "status": {
                        "table": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = source,
                            staging_table = "{}_{}_0".format(tables[f"temp_{ad_level}"], app_id),
                            target_table = tables[ad_level],
                            **merge[ad_level],
                            progress = False,
                        ),
                    },
                }


    @task(task_id="etl_meta_insights", map_index_template="{{ credentials['app_id'] }}")
    def etl_meta_insights(credentials: dict, configs: dict, **kwargs) -> dict:
        """앱 ID별 Meta 광고 일별 인사이트 지표를 수집하여 BigQuery에 적재한다."""
        from airflow_utils import get_execution_date
        configs = configs | {"merge": configs["merge"]["insights"]}
        return main_insights(**credentials, date=get_execution_date(kwargs, subdays=1), **configs)

    def main_insights(
            access_token: str,
            app_id: str,
            app_secret: str,
            date: str,
            service_account: dict,
            tables: dict[str, str],
            merge: dict[str, dict],
            account_ids: list[str] = list(),
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.meta.api import insights
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = {
            "campaigns": "meta_campaigns",
            "adsets": "meta_adsets",
            "ads": "meta_ads",
            "insights": "meta_insights",
        }

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            insights(
                access_token = access_token,
                app_id = app_id,
                app_secret = app_secret,
                ad_level = "ad",
                start_date = date,
                end_date = date,
                date_type = "daily",
                account_ids = account_ids,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "ad_level": "ad",
                        "date": date,
                        "date_type": "daily",
                        "account_ids": account_ids,
                    },
                    "counts": {
                        "campaigns": conn.count_table(sources["campaigns"]),
                        "adsets": conn.count_table(sources["adsets"]),
                        "ads": conn.count_table(sources["ads"]),
                        "insights": conn.count_table(sources["insights"]),
                    },
                    "status": {
                        "campaigns": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["campaigns"],
                            staging_table = f'{tables["temp_campaigns"]}_{app_id}_1',
                            target_table = tables["campaigns"],
                            **merge["campaigns"],
                            progress = False,
                        ),
                        "adsets": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["adsets"],
                            staging_table = f'{tables["temp_adsets"]}_{app_id}_1',
                            target_table = tables["adsets"],
                            **merge["adsets"],
                            progress = False,
                        ),
                        "ads": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["ads"],
                            staging_table = f'{tables["temp_ads"]}_{app_id}_1',
                            target_table = tables["ads"],
                            **merge["ads"],
                            progress = False,
                        ),
                        "insights": client.load_table_from_duckdb(
                            connection = conn,
                            source_table = sources["insights"],
                            target_table = tables["insights"],
                            progress = False,
                        ),
                    },
                }


    configs = read_configs()
    credentials = read_credentials()

    (etl_meta_campaigns
    .partial(configs=configs)
    .expand(credentials=credentials))

    (etl_meta_adsets
    .partial(configs=configs)
    .expand(credentials=credentials))

    (etl_meta_ads
    .partial(configs=configs)
    .expand(credentials=credentials))

    (etl_meta_insights
    .partial(configs=configs)
    .expand(credentials=credentials))
