"""
# 사방넷 발주 내역 ETL 파이프라인

## 인증(Credentials)
사방넷 아이디, 비밀번호와 시스템 도메인 번호가 필요하다.
Task를 실행할 때마다 로그인하고, 쿠키와 `access_token`을 발급받아 활용한다.

## 추출(Extract)
영업일 중 오전/오후 배송 접수 후, 수집일 기준 최근 7일간 사방넷 발주 내역을 다운로드 받는다.
매 영업일 종료 시점에 송장등록일 기준으로 당일 발주 내역을 다시 조회해 누락을 검증한다.

## 변환(Transform)
엑셀 바이너리 형식의 사방넷 발주 내역에서
운송장 번호를 포함한 발주 내역을 추출해 DuckDB 테이블에 적재한다.

## 적재(Load)
- 기존 BigQuery/Postgres 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
- 적재 과정에서 수집한 주문 결제일 파티션 범위를 바탕으로 후속 dbt 모델을 실행한다.
"""

from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from cosmos import DbtTaskGroup
from datetime import timedelta
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
    doc_md = __doc__,
    tags = [
        "priority:low", "platform:sabangnet", "objective:delivery", "credentials:userid",
        "schedule:weekdays", "time:morning", "time:afternoon", "time:night", "write:merge", "plugin:dbt"
    ],
) as dag:

    PATH = "sabangnet.admin.invoice"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, credentials="expand", tables=True)


    LAST_SCHEDULE = "23:50"
    LAST_7_DAYS = 7
    TODAY = None

    @task(task_id="etl_sabangnet_invoice", pool="sabangnet_pool")
    def etl_sabangnet_invoice(ti: TaskInstance, **kwargs) -> dict:
        from airflow_utils import format_datetime
        date_type = "dlvery_rcv_dt" if format_datetime(kwargs, fmt="HH:mm") == LAST_SCHEDULE else "reg_dm"
        start_date = format_datetime(kwargs, subdays=(TODAY if date_type == "dlvery_rcv_dt" else LAST_7_DAYS))
        end_date = format_datetime(kwargs)
        dates = {"start_date": start_date, "end_date": end_date, "date_type": date_type}
        return main(**dates, **ti.xcom_pull(task_ids="read_configs"))

    def main(
            userid: str,
            passwd: str,
            domain: int,
            download_no: int,
            start_date: str,
            end_date: str,
            date_type: str,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.sabangnet.admin import order_download
        from dual_load import merge_table_from_duckdb
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

            partitions = conn.unique("sabangnet_invoice", "DATE(order_dt)")

            return {
                "context": {
                    "partitions": sorted(map(str, partitions)),
                },
                "params": {
                    "start_date": start_date,
                    "end_date": end_date,
                    "date_type": date_type,
                    "download_no": download_no,
                    "download_type": "invoice",
                },
                "result": merge_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables["table"],
                    **merge["table"],
                    where_clause = conn.expr_datetime_range("T.order_dt", partitions),
                    execute = bool(partitions),
                )
            }


    @task(task_id="generate_dbt_date_range")
    def generate_dbt_date_range(result: dict) -> dict:
        from dbt_cosmos import generate_dbt_date_range as generate
        return generate(result, "context.partitions")


    @task.short_circuit(task_id="prepare_dbt_run")
    def prepare_dbt_run(ti: TaskInstance, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_dbt_date_range")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False


    def dbt_bigquery_sabangnet_invoice_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_sabangnet_invoice",
            selector = "sabangnet_invoice",
            ds_task_id = "generate_dbt_date_range",
        )


    etl_result = etl_sabangnet_invoice()

    dbt_date_range = generate_dbt_date_range(etl_result)
    dbt_run = dbt_bigquery_sabangnet_invoice_group()

    read_configs() >> etl_result
    dbt_date_range >> prepare_dbt_run() >> dbt_run
