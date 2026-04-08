from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "sabangnet_order",
    schedule = "30 23 * * *", # 담당자가 주문 확인 후 Streamlit UI에서 API 요청을 보내 트리거
    start_date = pendulum.datetime(2025, 9, 11, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:high", "sabangnet:order", "login:sabangnet", "schedule:weekdays", "time:daytime", "manual:api"],
    doc_md = dedent("""
        # 사방넷 주문 ETL 파이프라인

        > 안내) 담당자가 주문 확인 후 Streamlit UI에서 DAG 실행 요청을 보낸다.
        > 실행 회차가 같이 전달되며, 1차 실행 후 재고 현황을 알리는 `ecount_stock_report` DAG을 트리거한다.

        ## 인증(Credentials)
        사방넷 아이디, 비밀번호와 시스템 도메인 번호가 필요하다.
        Task를 실행할 때마다 로그인하고, 쿠키와 `access_token`을 발급받아 활용한다.

        ## 추출(Extract)
        담당자의 주문 확인 후 사방넷 주문 내역을 다운로드 받는다.
        매일 밤에 주문 등록일 기준으로 주문 내역을 다시 조회해 누락을 검증한다.

        ## 변환(Transform)
        3가지 다운로드 양식으로 받은 엑셀 바이너리 형식의 사방넷 주문 내역를 파싱한다.
        - 주문번호, 결제금액 등을 포함하는 주문 내역을 DuckDB 주문 테이블에 적재한다.
        - 상품명, 옵션명 등을 포함하는 주문 옵션 목록을 DuckDB 옵션 테이블에 적재한다.
        - 이름, 주소 등을 포함하는 고객의 주문/배송 정보를 DuckDB 발송 테이블에 적재한다.

        ## 적재(Load)
        각각의 테이블을 대응되는 BigQuery 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
    """).strip(),
) as dag:

    PATH = "sabangnet.admin.order"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, credentials="expand", tables=True, service_account=True)

    def get_order_date_pair(
            data_interval_start: pendulum.DateTime,
            data_interval_end: pendulum.DateTime,
            fmt: str = "YYYYMMDDHHmmss",
            **kwargs
        ) -> dict[str, str]:
        from airflow_utils import format_date
        return {
            "start_date": format_date(data_interval_start, fmt),
            "end_date": format_date(data_interval_end, fmt),
        }


    @task(task_id="etl_sabangnet_order", pool="sabangnet_pool")
    def etl_sabangnet_order(ti: TaskInstance, **kwargs) -> dict:
        configs = ti.xcom_pull(task_ids="read_configs")
        kwargs["fmt"] = "YYYYMMDDHHmmss" if ti.run_id.startswith("api__") else "YYYYMMDD"
        return main(download_type="order", **get_order_date_pair(**kwargs), **configs)

    @task(task_id="etl_sabangnet_dispatch", pool="sabangnet_pool")
    def etl_sabangnet_dispatch(ti: TaskInstance, **kwargs) -> dict:
        configs = ti.xcom_pull(task_ids="read_configs")
        kwargs["fmt"] = "YYYYMMDDHHmmss" if ti.run_id.startswith("api__") else "YYYYMMDD"
        return main(download_type="dispatch", **get_order_date_pair(**kwargs), **configs)

    @task(task_id="etl_sabangnet_option", pool="sabangnet_pool")
    def etl_sabangnet_option(ti: TaskInstance, **kwargs) -> dict:
        configs = ti.xcom_pull(task_ids="read_configs")
        kwargs["fmt"] = "YYYYMMDDHHmmss" if ti.run_id.startswith("api__") else "YYYYMMDD"
        return main(download_type="option", **get_order_date_pair(**kwargs), **configs)

    def main(
            userid: str,
            passwd: str,
            domain: int,
            download_no: dict[str, int],
            download_type: str,
            start_date: str,
            end_date: str,
            service_account: dict,
            tables: dict[str, str],
            merge: dict[str, dict],
            date_type: str = "reg_dm",
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.sabangnet.admin import order_download
        from linkmerce.extensions.bigquery import BigQueryClient
        source = f"sabangnet_{download_type}"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            order_download(
                userid = userid,
                passwd = passwd,
                domain = domain,
                download_no = download_no[download_type],
                download_type = download_type,
                start_date = start_date,
                end_date = end_date,
                date_type = date_type,
                connection = conn,
                return_type = "none",
            )

            if download_type == "order":
                date_column, date_array = "DATE(T.order_dt)", conn.unique(source, "DATE(order_dt)")
            elif download_type == "dispatch":
                date_column, date_array = "DATE(T.register_dt)", conn.unique(source, "DATE(register_dt)")
            else:
                date_column, date_array = None, None

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "start_date": start_date,
                        "end_date": end_date,
                        "date_type": date_type,
                        "download_no": download_no[download_type],
                        "download_type": download_type,
                    },
                    "counts": {
                        download_type: conn.count_table(source),
                    },
                    **({"dates": {
                        download_type: sorted(map(str, date_array))
                    }} if date_column else dict()),
                    "status": {
                        download_type: (client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = source,
                            staging_table = tables[f"temp_{download_type}"],
                            target_table = tables[download_type],
                            **merge[download_type],
                            where_clause = (conn.expr_date_range(date_column, date_array) if date_column else None),
                            progress = False,
                        ) if (not date_column) or date_array else True),
                    },
                }


    def branch_condition(ti: TaskInstance, **kwargs) -> str | None:
        if ti.run_id.startswith("api__1st__"):
            return "ecount_stock_report"
        else:
            return None

    branch_dagrun_trigger = BranchPythonOperator(
        task_id = "branch_dagrun_trigger",
        python_callable = branch_condition,
    )


    trigger_ecount_stock_report = TriggerDagRunOperator(
        task_id = "trigger_ecount_stock_report",
        trigger_dag_id = "ecount_stock_report",
        trigger_run_id = "{{ run_id }}",
        logical_date = "{{ logical_date }}",
        reset_dag_run = True,
        wait_for_completion = False,
    )


    configs = read_configs()

    configs >> etl_sabangnet_order() >> branch_dagrun_trigger
    configs >> etl_sabangnet_dispatch() >> branch_dagrun_trigger
    configs >> etl_sabangnet_option() >> branch_dagrun_trigger

    branch_dagrun_trigger >> [trigger_ecount_stock_report]
