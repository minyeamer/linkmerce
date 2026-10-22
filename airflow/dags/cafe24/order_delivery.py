"""
# 카페24 주문 배송 내역 ETL 파이프라인

> 안내) 카페24 주문 내역을 수집하는 'cafe24_order' Dag 실행 후 매시 0분에 트리거된다.

## 인증(Credentials)
카페24 Admin API 인증 키(클라이언트 ID/시크릿)와 쇼핑몰 ID 및 OAuth 토큰 파일 경로가 필요하다.

## 추출(Extract)
각 쇼핑몰별 직전에 성공한 Task Instance 반환 값의 'params.end_datetime'부터
현재 실행 시점('data_interval_end')보다 1초 앞선 시점까지를 조회 기간으로 하여
배송시작일('shipbegin_date') 기준의 주문 배송 내역을 수집한다.

Airflow UI에서 Dag을 트리거하면서 Configuration JSON에 ISO 8601 형식의
'start_datetime', 'end_datetime'을 'mall_id' 각각에 지정하면
해당 값을 기본 수집 기간 대신 적용한다. 모든 쇼핑몰에 동일한 조회 기간을 적용하고 싶다면
'mall_id' 키를 '*'로 대체할 수 있다.

쇼핑몰별 설정에서 'skip'을 True로 지정하면 ETL Task가 Skipped 처리된다.

```json
{
    "malls": {
        "mall_id": {
            "start_datetime": "2026-10-22T09:00:00+09:00",
            "end_datetime": "2026-10-22T09:59:59+09:00",
            "skip": false
        }
    }
}
```

## 변환(Transform)
JSON 형식의 주문 내역으로부터 주문 배송 정보를 분리하고,
발송일시가 조회 기간에 해당하는 데이터만 DuckDB 테이블에 남긴다.

## 적재(Load)
데이터를 BigQuery/Postgres 테이블 끝에 추가한다.
"""

from airflow.sdk import DAG, task
from airflow.models.dagrun import DagRun
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "cafe24_order_delivery",
    schedule = None,
    start_date = pendulum.datetime(2026, 10, 22, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=4),
    max_active_runs = 1,
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:cafe24", "objective:delivery", "credentials:api-key",
        "schedule:none", "time:morning", "time:afternoon", "time:night",
        "write:append", "upstream:dagrun"
    ],
) as dag:

    PATH = "cafe24.api.order_delivery"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    @task(task_id="etl_cafe24_order_delivery", map_index_template="{{ credentials['mall_id'] }}")
    def etl_cafe24_order_delivery(credentials: dict, configs: dict, dag_run: DagRun, **kwargs) -> dict:
        from airflow_api import get_next_datetime_range
        from airflow_utils import get_datetime
        conf = (dag_run.conf or dict()).get("malls") or dict()

        datetime_range = get_next_datetime_range(
            dag_id = dag_run.dag_id,
            task_id = "etl_cafe24_order_delivery",
            data_interval_end = get_datetime(kwargs),
            rendered_map_index = credentials["mall_id"],
            states = ["success"],
            format = "YYYY-MM-DDTHH:mm:ssZ",
            timedelta = { "seconds": 1 },
            conf = (conf.get(credentials["mall_id"]) or conf.get('*') or dict()),
        )
        return main(**credentials, **configs, **datetime_range)

    def main(
            mall_id: str,
            client_id: str,
            client_secret: str,
            token_info: str,
            start_datetime: str,
            end_datetime: str,
            tables: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.cafe24.api import order
        from dual_load import load_table_from_duckdb
        source = "cafe24_order_delivery"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            order(
                mall_id = mall_id,
                client_id = client_id,
                client_secret = client_secret,
                token_info = token_info,
                start_datetime = start_datetime,
                end_datetime = end_datetime,
                date_type = "shipbegin_date",
                connection = conn,
                return_type = "none",
            )

            return {
                "context": {
                    "partitions": sorted(map(str, conn.unique(source, "DATE(order_dt)"))),
                },
                "params": {
                    "mall_id": mall_id,
                    "start_datetime": start_datetime,
                    "end_datetime": end_datetime,
                    "date_type": "shipbegin_date",
                },
                "result": load_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables["table"],
                )
            }


    etl_results = (etl_cafe24_order_delivery
        .partial(configs=read_configs())
        .expand(credentials=read_credentials()))
