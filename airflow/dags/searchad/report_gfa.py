from airflow.sdk import DAG, task
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "searchad_report_gfa",
    schedule = "20 5 * * *",
    start_date = pendulum.datetime(2025, 8, 24, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    tags = ["priority:high", "searchad:report", "login:gfa", "schedule:daily", "time:morning"],
    doc_md = dedent("""
        # 네이버 성과형 디스플레이 광고 성과 보고서 ETL 파이프라인

        ## 인증(Credentials)
        성과형 디스플레이 광고 계정을 보유한 네이버 계정의 로그인 쿠키가 필요하다.

        ## 추출(Extract)
        실행 시점(data_interval_end)에서 1일 전을 기준으로
        계정별 캠페인/소재 성과 보고서를 다운로드한다.

        ## 변환(Transform)
        엑셀 바이너리 형식의 보고서를 JSON 형식으로 변환하고
        BigQuery 테이블에 대응되는 DuckDB 테이블에 적재한다.
        소재 성과 리포트에서 누락된 캠페인 성과 리포트를 INSERT 문으로 추가한다.

        ## 적재(Load)
        보고서 데이터를 BigQuery 테이블 끝에 추가한다.
    """).strip(),
) as dag:

    PATH = "searchad.gfa.report"
    RETRY_OPTIONS = {"retries": 3, "retry_delay": timedelta(minutes=1)}

    @task(task_id="read_configs", **RETRY_OPTIONS)
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials", **RETRY_OPTIONS)
    def read_credentials() -> list:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    @task(task_id="etl_searchad_report_gfa", map_index_template="{{ credentials['account_no'] }}", **RETRY_OPTIONS)
    def etl_searchad_report_gfa(credentials: dict, configs: dict, **kwargs) -> dict:
        from airflow_utils import get_execution_date
        return main(**credentials, date=get_execution_date(kwargs, subdays=1), **configs)

    def main(
            account_no: int | str,
            cookies: str,
            date: str,
            service_account: dict,
            tables: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.searchad.gfa import campaign_report, creative_report
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = {
            "campaign": "searchad_campaign_report",
            "creative": "searchad_creative_report",
            "merged": "searchad_report_gfa",
        }

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

            conn.copy_table(sources["creative"], sources["merged"])
            conn.execute(merge_into_query(sources["campaign"], sources["merged"]))

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "account_no": account_no,
                        "date": date,
                        "date_type": "DAY",
                    },
                    "counts": {
                        "campaign": conn.count_table(sources["campaign"]),
                        "creative": conn.count_table(sources["creative"]),
                        "merged": conn.count_table(sources["merged"]),
                    },
                    "status": {
                        "merged": client.load_table_from_duckdb(
                            connection = conn,
                            source_table = sources["merged"],
                            target_table = tables["table"],
                            progress = False,
                        ),
                    },
                }

    def merge_into_query(source_table: str, target_table: str) -> str:
        return dedent(f"""
            INSERT INTO {target_table}
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
            FROM {source_table} AS S
            WHERE NOT EXISTS (
                SELECT 1 FROM {target_table} AS T
                WHERE (T.account_no = S.account_no) AND (T.campaign_no = S.campaign_no)
            )""").strip()


    (etl_searchad_report_gfa
    .partial(configs=read_configs())
    .expand(credentials=read_credentials()))
