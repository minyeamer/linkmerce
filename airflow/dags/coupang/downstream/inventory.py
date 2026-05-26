from airflow.sdk import DAG, task
from airflow.models.dagrun import DagRun
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "coupang_inventory",
    schedule = None, # `coupang` Dag 실행 후 트리거 (0 9,17 * * *)
    start_date = pendulum.datetime(2026, 5, 26, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    tags = [
        "priority:high", "coupang:inventory", "login:coupang", "schedule:daily",
        "time:morning", "time:afternoon", "manual:dagrun"
    ],
    doc_md = dedent("""
        # 쿠팡 로켓그로스 재고 ETL 파이프라인

        > 안내) 쿠팡 통합 ETL을 제어하는 'coupang' Dag 실행 중 트리거된다.

        ## 인증(Credentials)
        'coupang' Dag에서 Playwright 브라우저로 쿠팡 광고 로그인 후 쿠키를 추출한다.
        쿠키(cookies)와 업체코드(vendor_id)를 딕셔너리로 묶어 'dag_run.conf'를 통해 전달받는다.

        ## 추출(Extract)
        매일 오전/오후 재고 업데이트 시간에 맞춰 쿠팡 업체별 로켓그로스 재고 목록을 수집한다.

        ## 변환(Transform)
        JSON 형식의 응답 본문을 파싱하여 DuckDB 테이블에 적재한다.

        ## 적재(Load)
        데이터를 BigQuery 테이블의 끝에 추가한다.
    """).strip(),
) as dag:

    PATH = "coupang.wing.inventory"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials")
    def read_credentials(dag_run: DagRun) -> dict:
        return {
            "cookies": dag_run.conf["cookies"]["wing"],
            "vendor_id": dag_run.conf["vendor_id"],
        }


    @task(task_id="etl_coupang_inventory", map_index_template="{{ credentials['vendor_id'] }}")
    def etl_coupang_inventory(credentials: dict, configs: dict, **kwargs) -> dict:
        return main(**credentials, **configs)

    def main(
            cookies: str,
            vendor_id: str,
            service_account: dict,
            tables: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.wing import rocket_inventory
        from linkmerce.extensions.bigquery import BigQueryClient
        source = "coupang_rocket_inventory"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            rocket_inventory(
                cookies = cookies,
                hidden_status = None,
                vendor_id = vendor_id,
                connection = conn,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "vendor_id": vendor_id,
                        "hidden_status": None,
                    },
                    "counts": {
                        "table": conn.count_table(source),
                    },
                    "status": {
                        "table": client.load_table_from_duckdb(
                            connection = conn,
                            source_table = source,
                            target_table = tables["table"],
                        ),
                    },
                }


    etl_coupang_inventory(
        configs = read_configs(),
        credentials = read_credentials(),
    )
