from airflow.sdk import DAG, task
from airflow.models.dagrun import DagRun
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "coupang_adreport",
    schedule = None, # `coupang` DAG 실행 후 트리거 (20 8 * * *)
    start_date = pendulum.datetime(2025, 11, 1, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    tags = ["priority:high", "coupang:adreport", "login:coupang", "schedule:daily", "time:morning", "manual:dagrun"],
    doc_md = dedent("""
        # 쿠팡 광고 성과 보고서 ETL 파이프라인

        > 안내) 쿠팡 통합 ETL을 제어하는 `coupang` DAG 실행 중 트리거된다.

        ## 인증(Credentials)
        `coupang` DAG에서 Playwright 브라우저로 쿠팡 광고 로그인 후 쿠키를 추출한다.
        쿠키(cookies)와 업체코드(vendor_id)를 딕셔너리로 묶어 'dag_run.conf'를 통해 전달받는다.

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
        from airflow_utils import read_config
        return read_config(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials")
    def read_credentials(dag_run: DagRun) -> dict:
        return {
            "cookies": dag_run.conf["cookies"]["ads"],
            "vendor_id": dag_run.conf["vendor_id"],
            "nca": dag_run.conf.get("nca", False),
        }


    @task(task_id="etl_coupang_adreport", map_index_template="{{ credentials['vendor_id'] }}")
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
        return main(**credentials, report_type="pa", date=date, **configs)

    def main(
            cookies: str,
            vendor_id: str,
            report_type: str,
            date: str,
            service_account: dict,
            tables: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.extensions.bigquery import BigQueryClient
        from importlib import import_module
        extract = getattr(import_module("linkmerce.api.coupang.advertising"), f"adreport_{report_type}")
        report_level = "creative" if report_type == "nca" else "vendorItem"
        source = f"coupang_adreport_{report_type}"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            extract(
                cookies = cookies,
                vendor_id = vendor_id,
                start_date = date,
                end_date = date,
                date_type = "daily",
                report_level = report_level,
                connection = conn,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "vendor_id": vendor_id,
                        "report_type": report_type,
                        "date": date,
                        "date_type": "daily",
                        "report_level": report_level,
                    },
                    "counts": {
                        "table": conn.count_table(source),
                    },
                    "status": {
                        "table": client.load_table_from_duckdb(
                            connection = conn,
                            source_table = source,
                            target_table = tables[report_type],
                            progress = False,
                        ),
                    }
                }


    etl_coupang_adreport(
        configs = read_configs(),
        credentials = read_credentials(),
    )
