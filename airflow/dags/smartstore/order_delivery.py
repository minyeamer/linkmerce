"""
# 스마트스토어 주문 배송 내역 ETL 파이프라인

> 안내) 스마트스토어 상품 주문 내역을 수집하는 'smartstore_order' Dag 실행 후 트리거된다.

## 인증(Credentials)
스마트스토어 커머스 API 인증 키(애플리케이션 ID/시크릿)가 필요하다.

## 추출(Extract)
각 채널별 직전에 성공한 Task Instance 반환 값의 'params.end_datetime'부터
현재 실행 시점('data_interval_end')보다 1ms 앞선 시점까지를 조회 기간으로 하여
주문 배송 내역을 수집한다.

Airflow UI에서 Dag을 트리거하면서 Configuration JSON에 ISO 8601 형식의
'start_datetime', 'end_datetime'을 'channel_seq' 각각에 지정하면
해당 값을 기본 수집 기간 대신 적용한다. 모든 채널에 동일한 조회 기간을 적용하고 싶다면
'channel_seq' 키를 '*'로 대체할 수 있다.

채널별 설정에서 'skip'을 True로 지정하면 ETL Task가 Skipped 처리된다.

```json
{
    "channels": {
        "100000000": {
            "start_datetime": "2026-09-22T09:00:00.000+09:00",
            "end_datetime": "2026-09-22T09:59:59.999+09:00",
            "skip": false
        }
    }
}
```

## 변환(Transform)
JSON 형식의 상품 주문 내역으로부터 주문 배송 정보를 분리해 DuckDB 테이블에 적재한다.

## 적재(Load)
데이터를 BigQuery/Postgres 테이블 끝에 추가한다.
"""

from airflow.sdk import DAG, task
from airflow.models.dagrun import DagRun
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "smartstore_order_delivery",
    schedule = None,
    start_date = pendulum.datetime(2025, 9, 1, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=4),
    max_active_runs = 1,
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:smartstore", "objective:delivery", "credentials:api-key",
        "schedule:none", "time:morning", "time:afternoon", "time:night",
        "write:append", "upstream:dagrun"
    ],
) as dag:

    PATH = "smartstore.api.order_delivery"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    @task(task_id="etl_smartstore_order_delivery", map_index_template="{{ credentials['channel_seq'] }}")
    def etl_smartstore_order_delivery(credentials: dict, configs: dict, dag_run: DagRun, **kwargs) -> dict:
        from airflow_api import get_next_datetime_range
        from airflow_utils import get_datetime
        datetime_range = get_next_datetime_range(
            dag_run = dag_run,
            etl_task_id = "etl_smartstore_order_delivery",
            data_interval_end = get_datetime(kwargs),
            rendered_map_index = str(credentials["channel_seq"]),
            states = ["success"],
            format = "YYYY-MM-DDTHH:mm:ss.SSSZ",
            timedelta = { "microseconds": 1000 },
        )
        return main(**credentials, **configs, **datetime_range)

    def main(
            client_id: str,
            client_secret: str,
            channel_seq: int | str,
            start_datetime: str,
            end_datetime: str,
            tables: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.smartstore.api import order
        from dual_load import load_table_from_duckdb
        source = "smartstore_delivery"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            order(
                client_id = client_id,
                client_secret = client_secret,
                start_datetime = start_datetime,
                end_datetime = end_datetime,
                range_type = "DISPATCHED_DATETIME",
                connection = conn,
                progress = False,
                return_type = "none",
            )

            return {
                "context": {
                    "partitions": sorted(map(str, conn.unique(source, "DATE(payment_dt)"))),
                },
                "params": {
                    "channel_seq": channel_seq,
                    "start_datetime": start_datetime,
                    "end_datetime": end_datetime,
                    "range_type": "DISPATCHED_DATETIME",
                },
                "result": load_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables["table"],
                )
            }


    etl_results = (etl_smartstore_order_delivery
    .partial(configs=read_configs())
    .expand(credentials=read_credentials()))
