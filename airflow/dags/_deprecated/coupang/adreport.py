from airflow.sdk import DAG, task
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "coupang_adreport",
    schedule = "50 5 * * *",
    start_date = pendulum.datetime(2025, 11, 1, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:high", "coupang:adreport", "login:coupang", "schedule:daily", "time:morning"],
    doc_md = dedent("""
        # 쿠팡 광고 성과 보고서 ETL 파이프라인

        > 안내) 쿠팡 광고 로그인 정책 강화로 사용 중지

        ## 인증(Credentials)
        쿠팡 광고 로그인 쿠키가 필요하다.
        (정책 강화로 마지막으로 로그인된 쿠키만 사용할 수 있다.)

        ## 추출(Extract)
        실행 시점(data_interval_end)에서 1일 전을 기준으로
        매출 성장 광고 보고서(PA) 및 신규 구매 고객 확보 광고 보고서(NCA)를 다운로드한다.

        ## 변환(Transform)
        엑셀 바이너리 형식의 보고서를 JSON 형식으로 변환하고
        BigQuery 테이블에 대응되는 DuckDB 테이블에 적재한다.

        ## 적재(Load)
        각각의 보고서를 대응되는 BigQuery 테이블의 끝에 추가한다.
    """).strip(),
) as dag:

    PATH = "coupang.advertising.adreport"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read
        return read(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read
        return read(PATH, credentials=True)["credentials"]


    @task(task_id="etl_coupang_adreport", map_index_template="{{ credentials['vendor_id'] }}", pool="coupang_pool")
    def etl_coupang_adreport(credentials: dict, configs: dict, **kwargs) -> dict:
        """기본으로 매출 성장 광고 보고서(PA)를 가져온다.   
        인증 정보에 `nca=True` 설정된 업체는 신규 구매 고객 확보 광고 보고서(NCA)를 추가로 다운로드한다."""
        from airflow_utils import get_execution_date
        date = get_execution_date(kwargs, subdays=1)
        if credentials.get("nca"):
            return {
                "pa": main(**credentials, report_type="pa", date=date, **configs),
                "nca": main(**credentials, report_type="nca", date=date, **configs),
            }
        else:
            return main(**credentials, report_type="pa", date=date, **configs)

    def main(
            cookies: str,
            vendor_id: str,
            report_type: str,
            date: str,
            service_account: dict,
            tables: dict[str,str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.advertising import adreport
        from linkmerce.extensions.bigquery import BigQueryClient
        report_level = "creative" if report_type == "nca" else "vendorItem"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            adreport(
                cookies = cookies,
                vendor_id = vendor_id,
                start_date = date,
                end_date = date,
                report_type = report_type,
                date_type = "daily",
                report_level = report_level,
                connection = conn,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "vendor_id": vendor_id,
                        "date": date,
                        "report_type": report_type,
                        "date_type": "daily",
                        "report_level": report_level,
                    },
                    "counts": {
                        "data": conn.count_table(f"coupang_adreport_{report_type}"),
                    },
                    "status": {
                        "data": client.load_table_from_duckdb(
                            connection = conn,
                            source_table = f"coupang_adreport_{report_type}",
                            target_table = tables[report_type],
                            progress = False,
                        ),
                    }
                }


    etl_coupang_adreport.partial(configs=read_configs()).expand(credentials=read_credentials())
