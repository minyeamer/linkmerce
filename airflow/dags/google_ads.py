from airflow.sdk import DAG, task
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "google_ads",
    schedule = "50 7 * * *",
    start_date = pendulum.datetime(2026, 2, 26, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    tags = ["priority:high", "google:ads", "api:google", "schedule:daily", "time:morning"],
    doc_md = dedent("""
        # 구글 Ads ETL 파이프라인

        ## 인증(Credentials)
        구글 Ads API 인증 정보(개발자 토큰, 계정 ID, 매니저 계정 ID)와
        GCP 서비스 계정이 필요하다.

        ## 추출(Extract)
        계정별 캠페인, 광고그룹, 소재, 애셋 목록을 수집하고,
        추가로 실행 시점(data_interval_end)에서 1일 전을 기준으로 소재의 성과 데이터를 가져온다.

        ## 변환(Transform)
        JSON 형식의 응답 본문을 파싱하여 캠페인, 광고그룹, 소재, 애셋,
        그리고 성과 데이터에 대한 각각의 DuckDB 테이블에 적재한다.

        ## 적재(Load)
        각각의 캠페인, 광고그룹, 소재, 애셋 테이블을 기존 BigQuery 테이블과
        MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
        소재 성과 테이블은 대응되는 BigQuery 테이블 끝에 추가한다.
    """).strip(),
) as dag:

    GOOGLE_PATH = "google.api.ads"

    @task(task_id="read_objects_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_objects_configs() -> dict:
        from airflow_utils import read
        return read(GOOGLE_PATH, tables=True, service_account=True)

    @task(task_id="read_objects_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_objects_credentials() -> list:
        from airflow_utils import read
        return read(GOOGLE_PATH, credentials=True)["credentials"]


    AD_OBJECTS = ["campaign", "adgroup", "ad", "asset"]

    @task(task_id="etl_google_objects", map_index_template="{{ credentials['customer_id'] }}")
    def etl_google_objects(credentials: dict, configs: dict, **kwargs) -> dict:
        return {ad_level: main_object(ad_level, **credentials, **configs) for ad_level in AD_OBJECTS}

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
        from linkmerce.extensions.bigquery import BigQueryClient
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

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "ad_level": ad_level,
                        **date_range,
                    },
                    "counts": {
                        "table": conn.count_table(source),
                    },
                    "status": {
                        "table": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = source,
                            staging_table = "{}_{}" .format(tables[f"temp_{ad_level}"], customer_id),
                            target_table = tables[ad_level],
                            **merge[ad_level],
                            progress = False,
                        ),
                    },
                }


    @task(task_id="read_insight_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_insight_configs() -> dict:
        from airflow_utils import read
        return read(GOOGLE_PATH, tables=True, service_account=True)

    @task(task_id="read_insight_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_insight_credentials() -> list:
        from airflow_utils import read
        return read(GOOGLE_PATH, credentials=True)["credentials"]


    @task(task_id="etl_google_insight", map_index_template="{{ credentials['customer_id'] }}")
    def etl_google_insight(credentials: dict, configs: dict, **kwargs) -> dict:
        from airflow_utils import get_execution_date
        return main_insight(**credentials, date=get_execution_date(kwargs, subdays=1), **configs)

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
        from linkmerce.extensions.bigquery import BigQueryClient
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

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "date": date,
                    },
                    "counts": {
                        "table": conn.count_table(source),
                    },
                    "status": {
                        "table": client.load_table_from_duckdb(
                            connection = conn,
                            source_table = source,
                            target_table = tables["insight"],
                            progress = False,
                        ),
                    },
                }


    google_objects = (etl_google_objects
        .partial(configs=read_objects_configs())
        .expand(credentials=read_objects_credentials()))


    google_insight = (etl_google_insight
        .partial(configs=read_insight_configs())
        .expand(credentials=read_insight_credentials()))


    google_objects >> google_insight
