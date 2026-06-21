"""
# 스마트스토어 주문 배송 정보 ETL 파이프라인

## 인증(Credentials)
스마트스토어 커머스 API 인증 키(애플리케이션 ID/시크릿)가 필요하다.

## 추출(Extract)
영업일 중 오전/오후 배송 접수 후, 결제일 기준 전일의 스마트스토어 상품 주문 내역을 다운로드 받는다.
매일 새벽에 발송일 기준으로 상품 주문 내역을 다시 조회해 누락을 검증한다.

## 변환(Transform)
JSON 형식의 응답 본문에서 주문 배송 정보를 추출해 DuckDB 테이블에 적재한다.

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
    dag_id = "smartstore_invoice",
    schedule = MultipleCronTriggerTimetable(
        "0 3 * * *",
        "30 10 * * 1-5",
        "0 15 * * 1-5",
        timezone = "Asia/Seoul",
    ),
    start_date = pendulum.datetime(2025, 10, 25, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:smartstore", "objective:delivery", "credentials:api-key",
        "schedule:daily", "time:morning", "time:afternoon", "time:night", "write:merge", "plugin:dbt"
    ],
) as dag:

    PATH = "smartstore.api.invoice"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    FIRST_SCHEDULE = "03:00"

    @task(task_id="etl_smartstore_invoice", map_index_template="{{ credentials['channel_seq'] }}")
    def etl_smartstore_invoice(credentials: dict, configs: dict, **kwargs) -> dict:
        from airflow_utils import format_datetime
        if format_datetime(kwargs, fmt="HH:mm") == FIRST_SCHEDULE:
            range_type = "DISPATCHED_DATETIME"
        else:
            range_type = "PAYED_DATETIME"
        return main(**credentials, date=format_datetime(kwargs, subdays=1), range_type=range_type, **configs)

    def main(
            client_id: str,
            client_secret: str,
            channel_seq: str,
            date: str,
            range_type: str,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.smartstore.api import order as smartstore_order
        from dual_load import merge_table_from_duckdb
        source = "smartstore_delivery"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            smartstore_order(
                client_id = client_id,
                client_secret = client_secret,
                start_date = date,
                end_date = date,
                range_type = range_type,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            partitions = conn.unique(source, "DATE(payment_dt)")

            return {
                "context": {
                    "partitions": sorted(map(str, partitions)),
                },
                "params": {
                    "channel_seq": channel_seq,
                    "date": date,
                    "range_type": range_type,
                },
                "result": merge_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables["table"],
                    **merge["table"],
                    where_clause = conn.expr_datetime_range("T.payment_dt", partitions),
                    execute = bool(partitions),
                )
            }


    @task(task_id="generate_dbt_date_range", trigger_rule="all_done")
    def generate_dbt_date_range(results: list[dict]) -> dict:
        from dbt_cosmos import generate_dbt_date_range as generate
        return generate(results, "context.partitions")


    @task.short_circuit(task_id="prepare_dbt_run")
    def prepare_dbt_run(ti: TaskInstance, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_dbt_date_range")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False


    def dbt_bigquery_smartstore_invoice_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_smartstore_invoice",
            selector = "smartstore_invoice",
            ds_task_id = "generate_dbt_date_range",
        )


    @task(task_id="finalize_dag_run", trigger_rule="all_done")
    def finalize_dag_run(ti: TaskInstance):
        from dbt_cosmos import raise_on_failure
        raise_on_failure(ti)


    etl_results = (etl_smartstore_invoice
        .partial(configs=read_configs())
        .expand(credentials=read_credentials()))

    dbt_date_range = generate_dbt_date_range(etl_results)
    dbt_run = dbt_bigquery_smartstore_invoice_group()

    dbt_date_range >> prepare_dbt_run() >> dbt_run >> finalize_dag_run()
