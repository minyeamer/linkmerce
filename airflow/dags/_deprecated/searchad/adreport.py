from airflow.sdk import DAG, TaskGroup, task
from airflow.task.trigger_rule import TriggerRule
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "searchad_report",
    schedule = "40 5 * * *",
    start_date = pendulum.datetime(2025, 8, 24, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    tags = ["priority:high", "searchad:report", "login:searchad", "login:gfa", "schedule:daily", "time:morning"],
    doc_md = dedent("""
        # 네이버 검색광고 일별 보고서 ETL 파이프라인
        네이버 검색광고와 성과형 디스플레이 광고에 대해 각각 별도의 TaskGroup으로 구분해 처리한다.

        > 안내) 네이버 로그인 정책 강화로 사용 중지

        ## 인증(Credentials)
        네이버 검색광고 또는 성과형 디스플레이 광고 계정을 보유한
        네이버 계정의 로그인 쿠키가 필요하다.
        (정책 강화로 인한 CAPTCHA 인증을 통과할 수 없어 API로 대체한다.)

        ## 추출(Extract)
        실행 시점(data_interval_end)에서 1일 전을 기준으로 두 가지 보고서를 다운로드한다.
        1. 네이버 검색광고에서 소재 수준의 다차원 보고서를 다운로드한다.
        2. 네이버 성과형 디스플레이 광고(GFA)에서 캠페인과 소재 성과 리포트를 다운로드한다.

        ## 변환(Transform)
        1. 검색광고의 다차원 보고서를 TSV 데이터를 파싱해 DuckDB 테이블에 적재한다.
        2. GFA 보고서는 엑셀 바이너리 형식을 파싱해 각각의 DuckDB 테이블에 적재한다.
            - GFA 소재 성과 리포트에서 누락된 캠페인 성과 리포트를 'main_gfa' 함수에서 INSERT 문으로 추가한다.

        ## 적재(Load)
        각각의 보고서를 대응되는 BigQuery 테이블 끝에 추가한다.
    """).strip(),
) as dag:

    with TaskGroup(group_id="searchad_group") as searchad_group:

        SEARCHAD_PATH = "searchad.manage.adreport"

        @task(task_id="read_configs_searchad", retries=3, retry_delay=timedelta(minutes=1))
        def read_configs_searchad() -> dict:
            from airflow_utils import read
            return read(SEARCHAD_PATH, tables=True, service_account=True)

        @task(task_id="read_queries_searchad", retries=3, retry_delay=timedelta(minutes=1))
        def read_queries_searchad() -> list:
            from airflow_utils import read
            configs = read(SEARCHAD_PATH, credentials=True)
            params = configs["params"]
            return [dict(credential, **params[credential["customer_id"]]) for credential in configs["credentials"]]


        @task(task_id="etl_searchad_report", map_index_template="{{ queries['customer_id'] }}")
        def etl_searchad_report(queries: dict, configs: dict, **kwargs) -> dict:
            from airflow_utils import get_execution_date
            return main_searchad(**queries, date=get_execution_date(kwargs, subdays=1), **configs)

        def main_searchad(
                customer_id: int | str,
                cookies: str,
                report_id: str,
                report_name: str,
                date: str,
                userid: str,
                service_account: dict,
                tables: dict[str,str],
                **kwargs
            ) -> dict:
            from linkmerce.common.load import DuckDBConnection
            from linkmerce.api.searchad.manage import daily_report
            from linkmerce.extensions.bigquery import BigQueryClient

            with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
                daily_report(
                    customer_id = customer_id,
                    cookies = cookies,
                    report_id = report_id,
                    report_name = report_name,
                    userid = userid,
                    start_date = date,
                    end_date = date,
                    connection = conn,
                    return_type = "none",
                )

                with BigQueryClient(service_account) as client:
                    return {
                        "params": {
                            "customer_id": customer_id,
                            "report_id": report_id,
                            "report_name": report_name,
                            "date": date,
                        },
                        "counts": {
                            "data": conn.count_table("searchad_report"),
                        },
                        "status": {
                            "data": client.load_table_from_duckdb(
                                connection = conn,
                                source_table = "searchad_report",
                                target_table = tables["data"],
                                progress = False,
                            ),
                        },
                    }


        (etl_searchad_report
            .partial(configs=read_configs_searchad())
            .expand(queries=read_queries_searchad()))


    with TaskGroup(group_id="gfa_group") as gfa_group:

        GFA_PATH = "searchad.gfa.adreport"

        @task(task_id="read_configs_gfa", retries=3, retry_delay=timedelta(minutes=1), trigger_rule=TriggerRule.ALWAYS)
        def read_configs_gfa() -> dict:
            from airflow_utils import read
            return read(GFA_PATH, tables=True, service_account=True)

        @task(task_id="read_credentials_gfa", retries=3, retry_delay=timedelta(minutes=1), trigger_rule=TriggerRule.ALWAYS)
        def read_credentials_gfa() -> list:
            from airflow_utils import read
            return read(GFA_PATH, credentials=True)["credentials"]


        @task(task_id="etl_gfa_report", map_index_template="{{ credentials['account_no'] }}")
        def etl_gfa_report(credentials: dict, configs: dict, **kwargs) -> dict:
            """계정별 GFA 캀페인/크리에이티브 일별 성과 리포트를 병합하여 BigQuery에 적재한다."""
            from airflow_utils import get_execution_date
            return main_gfa(**credentials, date=get_execution_date(kwargs, subdays=1), **configs)

        def main_gfa(
                account_no: int | str,
                cookies: str,
                date: str,
                service_account: dict,
                tables: dict[str,str],
                **kwargs
            ) -> dict:
            from linkmerce.common.load import DuckDBConnection
            from linkmerce.api.searchad.gfa import campaign_report, creative_report
            from linkmerce.extensions.bigquery import BigQueryClient

            with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
                campaign_report(
                    account_no = account_no,
                    cookies = cookies,
                    start_date = date,
                    end_date = date,
                    date_type = "DAY",
                    connection = conn,
                    progress = False,
                    return_type = "none",
                )

                creative_report(
                    account_no = account_no,
                    cookies = cookies,
                    start_date = date,
                    end_date = date,
                    date_type = "DAY",
                    connection = conn,
                    progress = False,
                    return_type = "none",
                )

                conn.copy_table("searchad_creative_report", "searchad_report_gfa")
                conn.execute(insert_campaign_report())

                with BigQueryClient(service_account) as client:
                    return {
                        "params": {
                            "account_no": account_no,
                            "date": date,
                            "date_type": "DAY",
                        },
                        "counts": {
                            "data": conn.count_table("searchad_report_gfa"),
                        },
                        "status": {
                            "data": client.load_table_from_duckdb(
                                connection = conn,
                                source_table = "searchad_report_gfa",
                                target_table = tables["searchad_report_gfa"],
                                progress = False,
                            ),
                        },
                    }

        def insert_campaign_report() -> str:
            return dedent(f"""
                INSERT INTO searchad_report_gfa
                SELECT
                    campaign_no
                    , 0 AS adset_no
                    , 0 AS creative_no
                    , account_no
                    , impression_count
                    , click_count
                    , reach_count
                    , ad_cost
                    , conv_count
                    , conv_amount
                    , ymd
                FROM searchad_campaign_report AS S
                WHERE NOT EXISTS (
                    SELECT 1 FROM searchad_report_gfa AS T
                    WHERE (T.account_no = S.account_no) AND (T.campaign_no = S.campaign_no)
                )""").strip()

        (etl_gfa_report
            .partial(configs=read_configs_gfa())
            .expand(credentials=read_credentials_gfa()))


    searchad_group >> gfa_group
