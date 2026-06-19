"""
# 스마트스토어 상품 주문 내역 ETL 파이프라인

## 인증(Credentials)
스마트스토어 커머스 API 인증 키(애플리케이션 ID/시크릿)가 필요하다.

## 추출(Extract)
실행 시점(data_interval_end)에서 1일 전을 기준으로
- 전체 상품 주문 내역을 수집한다.
- 추가로, 변경 상품 주문 내역을 가져온다.

## 변환(Transform)
- JSON 형식의 상품 주문 내역으로부터 주문 정보, 상품 주문 정보, 주문 배송 정보, 주문 옵션 정보를 분리해
각각의 DuckDB 테이블에 적재한다.
- 변경 상품 주문 내역도 JSON 형식의 응답 본문을 파싱하여 DuckDB 테이블에 적재한다.

## 적재(Load)
- 주문 정보, 상품 주문 정보 테이블은 BigQuery/Postgres 테이블 끝에 추가한다.
- 주문 배송 정보, 주문 옵션 정보 테이블은 대응되는 BigQuery/Postgres 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
- 변경 상품 주문 내역도 결제일 파티션 필터를 더해 기존 BigQuery/Postgres 테이블과 MERGE 문으로 병합한다.
- 적재 과정에서 수집한 주문 결제일 파티션 범위를 바탕으로 후속 dbt 모델을 실행한다.
"""

from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from cosmos import DbtTaskGroup
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "smartstore_order",
    schedule = "30 8 * * *",
    start_date = pendulum.datetime(2025, 9, 1, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:smartstore",
        "objective:sales", "objective:product", "objective:delivery", "credentials:api-key",
        "schedule:daily", "time:morning", "write:append", "write:merge", "plugin:dbt"
    ],
) as dag:

    PATH = "smartstore.api.order"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    @task(task_id="etl_smartstore_order", map_index_template="{{ credentials['channel_seq'] }}")
    def etl_smartstore_order(credentials: dict, configs: dict, **kwargs) -> dict:
        from airflow_utils import format_datetime
        return main(**credentials, date=format_datetime(kwargs, subdays=1), **configs)

    def main(
            client_id: str,
            client_secret: str,
            channel_seq: str,
            date: str,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.smartstore.api import order as smartstore_order
        from linkmerce.api.smartstore.api import aggregated_order_status
        from dual_load import load_table_from_duckdb, merge_table_from_duckdb
        sources = {
            "order": "smartstore_order",
            "product_order": "smartstore_product_order",
            "delivery": "smartstore_delivery",
            "option": "smartstore_option",
            "order_status": "smartstore_order_time",
        }

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            smartstore_order(
                client_id = client_id,
                client_secret = client_secret,
                start_date = date,
                end_date = date,
                range_type = "PAYED_DATETIME",
                connection = conn,
                progress = False,
                return_type = "none",
            )

            aggregated_order_status(
                client_id = client_id,
                client_secret = client_secret,
                channel_seq = channel_seq,
                start_date = date,
                end_date = date,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            order_dates = conn.unique(sources["order"], "DATE(payment_dt)")
            last_changed_dates = conn.unique(sources["order_status"], "DATE(payment_dt)")

            return {
                "context": {
                    "partitions": sorted(map(str, set(order_dates + last_changed_dates))),
                },
                "params": {
                    "channel_seq": channel_seq,
                    "date": date,
                    "range_type": "PAYED_DATETIME",
                },
                "results": {
                    "order": load_table_from_duckdb(
                        connection = conn,
                        source_table = sources["order"],
                        target_table = tables["order"],
                    ),
                    "product_order": load_table_from_duckdb(
                        connection = conn,
                        source_table = sources["product_order"],
                        target_table = tables["product_order"],
                    ),
                    "delivery": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["delivery"],
                        target_table = tables["delivery"],
                        **merge["delivery"],
                        where_clause = conn.expr_datetime_range("T.payment_dt", [date]),
                    ),
                    "option": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["option"],
                        target_table = tables["option"],
                        **merge["option"],
                    ),
                    "order_status": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["order_status"],
                        target_table = tables["order_status"],
                        **merge["order_status"],
                        where_clause = conn.expr_datetime_range("T.payment_dt", last_changed_dates),
                        execute = bool(last_changed_dates),
                    )
                }
            }


    @task(task_id="generate_dbt_date_range")
    def generate_dbt_date_range(results: list[dict]) -> dict:
        from dbt_cosmos import generate_dbt_date_range as generate
        return generate(results, "context.partitions")


    @task.short_circuit(task_id="prepare_dbt_run")
    def prepare_dbt_run(ti: TaskInstance, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_dbt_date_range")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False


    def dbt_bigquery_smartstore_order_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_smartstore_order",
            selector = "smartstore_order",
            ds_task_id = "generate_dbt_date_range",
        )


    etl_results = (etl_smartstore_order
        .partial(configs=read_configs())
        .expand(credentials=read_credentials()))

    dbt_date_range = generate_dbt_date_range(etl_results)
    dbt_run = dbt_bigquery_smartstore_order_group()

    dbt_date_range >> prepare_dbt_run() >> dbt_run
