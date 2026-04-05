from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "sabangnet_invoice",
    schedule = MultipleCronTriggerTimetable(
        "30 10 * * 1-5",
        "30 14 * * 1-5",
        "50 23 * * 1-5",
        timezone = "Asia/Seoul",
    ),
    start_date = pendulum.datetime(2025, 11, 27, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    tags = ["priority:low", "sabangnet:order", "login:sabangnet", "schedule:weekdays", "time:allday"],
    doc_md = dedent("""
        # 사방넷 운송장 ETL 파이프라인

        ## 인증(Credentials)
        사방넷 아이디, 비밀번호와 시스템 도메인 번호가 필요하다.
        Task를 실행할 때마다 로그인하고, 쿠키와 `access_token`을 발급받아 활용한다.

        ## 추출(Extract)
        영업일 중 오전, 오후 배송 접수 후 사방넷 주문 내역을 다운로드 받는다.
        매 영업일 종료 시점에 주문 등록일 기준으로 주문 내역을 다시 조회해 누락을 검증한다.

        ## 변환(Transform)
        엑셀 바이너리 형식의 사방넷 주문 내역에서
        운송장 번호를 포함한 발주 정보를 추출해 DuckDB 테이블에 적재한다.

        ## 적재(Load)
        기존 BigQuery 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
    """).strip(),
) as dag:

    PATH = "sabangnet.admin.invoice"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read
        return read(PATH, credentials="expand", tables=True, service_account=True)


    LAST_SCHEDULE = "23:50"
    LAST_7_DAYS = 7
    TODAY = None

    @task(task_id="etl_sabangnet_invoice", pool="sabangnet_pool")
    def etl_sabangnet_invoice(ti: TaskInstance, **kwargs) -> dict:
        from airflow_utils import get_execution_date
        date_type = "dlvery_rcv_dt" if get_execution_date(kwargs, fmt="HH:mm") == LAST_SCHEDULE else "reg_dm"
        start_date = get_execution_date(kwargs, subdays=(TODAY if date_type == "dlvery_rcv_dt" else LAST_7_DAYS))
        end_date = get_execution_date(kwargs)
        return main(start_date=start_date, end_date=end_date, date_type=date_type, **ti.xcom_pull(task_ids="read_configs"))

    def main(
            userid: str,
            passwd: str,
            domain: int,
            download_no: int,
            start_date: str,
            end_date: str,
            date_type: str,
            service_account: dict,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.sabangnet.admin import order_download
        from linkmerce.extensions.bigquery import BigQueryClient
        source = "sabangnet_invoice"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            order_download(
                userid = userid,
                passwd = passwd,
                domain = domain,
                download_no = download_no,
                download_type = "invoice",
                start_date = start_date,
                end_date = end_date,
                date_type = date_type,
                connection = conn,
                return_type = "none",
            )

            date_array = conn.unique("sabangnet_invoice", "DATE(order_dt)")

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "start_date": start_date,
                        "end_date": end_date,
                        "date_type": date_type,
                        "download_no": download_no,
                        "download_type": "invoice",
                    },
                    "counts": {
                        "table": conn.count_table(source),
                    },
                    "dates": {
                        "table": sorted(map(str, date_array)),
                    },
                    "status": {
                        "table": (client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = source,
                            staging_table = tables["temp_invoice"],
                            target_table = tables["invoice"],
                            **merge["invoice"],
                            where_clause = conn.expr_date_range("DATE(T.order_dt)", date_array),
                            progress = False,
                        ) if date_array else True),
                    },
                }


    read_configs() >> etl_sabangnet_invoice()
