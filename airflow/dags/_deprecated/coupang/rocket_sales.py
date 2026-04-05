from airflow.sdk import DAG, task
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "coupang_rocket_sales",
    schedule = "10 9 * * *",
    start_date = pendulum.datetime(2025, 10, 22, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:high", "coupang:rocket", "login:coupang", "schedule:daily", "time:morning"],
    doc_md = dedent("""
        # 쿠팡 로켓 정산 보고서 ETL 파이프라인

        > 안내) 쿠팡 윙 로그인 정책 강화로 사용 중지 (~ v0.6.8)

        ## 인증(Credentials)
        쿠팡 윙 로그인 쿠키가 필요하다.
        (정책 강화로 마지막으로 로그인된 쿠키만 사용할 수 있다.)

        ## 추출(Extract)
        실행 시점(data_interval_end)이 포함된 1주간 매출인식일 기준으로 집계된
        쿠팡 판매 수수료 리포트와 쿠팡 입출고비/배송비 리포트를 다운로드한다.
        (월요일부터 일요일까지를 한 주로 본다.)

        ## 변환(Transform)
        엑셀 바이너리 형식의 보고서를 JSON 형식으로 변환하고
        BigQuery 테이블에 대응되는 DuckDB 테이블에 적재한다.

        ## 적재(Load)
        각각의 보고서를 대응되는 BigQuery 테이블과 MERGE 문으로 병합한다.
        (같은 날짜에 여러 번 Dag을 재실행해도 중복된 데이터가 적재되지 않게 보장한다.
        특히, 입출고비/배송비 리포트는 파티션 날짜와 조회 기준이 다르므로 반드시 중복 처리해야 한다.)
    """).strip(),
) as dag:

    PATH = "coupang.wing.rocket_sales"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read
        return read(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read
        return read(PATH, credentials=True)["credentials"]


    @task(task_id="etl_coupang_rocket_sales", map_index_template="{{ credentials['vendor_id'] }}", pool="coupang_pool")
    def etl_coupang_rocket_sales(credentials: dict, configs: dict, **kwargs) -> dict:
        dates = dict(zip(["start_date", "end_date"], generate_sales_date(**kwargs)))
        return main(**credentials, **dates, **configs)

    def generate_sales_date(data_interval_end: pendulum.DateTime = None, **kwargs) -> tuple[str, str]:
        """실행 시점(data_interval_end)이 포함된 월요일-일요일 주간을 계산하고, 주의 시작일과 종료일을 반환한다."""
        from airflow_utils import in_timezone
        def get_last_monday(datetime: pendulum.DateTime) -> pendulum.DateTime:
            weekday = datetime.day_of_week # Monday: 0 - Sunday: 6
            return datetime if weekday == 0 else datetime.subtract(days=weekday)
        start_date = get_last_monday(in_timezone(data_interval_end, subdays=1))
        end_date = start_date.add(days=6)
        return start_date.format("YYYY-MM-DD"), end_date.format("YYYY-MM-DD")

    def main(
            cookies: str,
            vendor_id: str,
            start_date: str,
            end_date: str,
            service_account: dict,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.wing import rocket_settlement_download
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = {"sales": "coupang_rocket_sales", "shipping": "coupang_rocket_shipping"}

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            rocket_settlement_download(
                cookies = cookies,
                vendor_id = vendor_id,
                start_date = start_date,
                end_date = end_date,
                date_type = "SALES",
                progress = False,
                connection = conn,
                return_type = "none",
            )

            date_array = {table: conn.unique(f"coupang_rocket_{table}", "sales_date") for table in ["sales", "shipping"]}

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "vendor_id": vendor_id,
                        "start_date": start_date,
                        "end_date": end_date,
                        "date_type": "SALES",
                    },
                    "counts": {
                        "sales": conn.count_table(sources["sales"]),
                        "shipping": conn.count_table(sources["shipping"]),
                    },
                    "dates": date_array,
                    "status": {
                        "sales": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["sales"],
                            staging_table = tables["temp_sales"],
                            target_table = tables["sales"],
                            **merge["sales"],
                            where_clause = "({sales_date}) AND (T.vendor_id = '{vendor_id}')".format(
                                sales_date = conn.expr_date_range("T.sales_date", date_array["sales"]),
                                vendor_id = vendor_id,
                            ),
                            progress = False,
                        ),
                        "shipping": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["shipping"],
                            staging_table = tables["temp_shipping"],
                            target_table = tables["shipping"],
                            **merge["shipping"],
                            where_clause = "({sales_date}) AND (T.vendor_id = '{vendor_id}')".format(
                                sales_date = conn.expr_date_range("T.sales_date", date_array["shipping"]),
                                vendor_id = vendor_id,
                            ),
                            progress = False,
                        ),
                    },
                }


    etl_coupang_rocket_sales.partial(configs=read_configs()).expand(credentials=read_credentials())
