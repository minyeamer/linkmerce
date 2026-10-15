"""
# 쿠팡 로켓그로스 주문 ETL 파이프라인

> 안내) 쿠팡 마켓플레이스 발주서를 수집하는 'coupang_order' Dag 실행 후 트리거된다.

## 인증(Credentials)
쿠팡 Open API 인증 키(Access Key, Secret Key, 업체 코드)가 필요하다.

## 추출(Extract)
각 업체별 직전에 성공한 Task Instance 반환 값의 'params.end_datetime'부터
현재 실행 시점('data_interval_end')보다 1분 앞선 시점까지를 필터 기간으로 설정한다.

필터 기간 내 전체 주문 목록을 수집한 후,
결제일시를 기준으로 필터 기간에 해당하는 주문만 추출한다.
(필터 기간이 2일 미만일 경우 종료일에서 1일 더한 날짜를 조회 종료일로 한다.)

Airflow UI에서 Dag을 트리거하면서 Configuration JSON에 ISO 8601 형식의
'start_datetime', 'end_datetime'을 'vendor_id' 각각에 지정하면
해당 값을 기본 필터 기간 대신 적용한다. 모든 채널에 동일한 필터 기간을 적용하고 싶다면
'vendor_id' 키를 '*'로 대체할 수 있다.

채널별 설정에서 'skip'을 True로 지정하면 ETL Task가 Skipped 처리된다.

```json
{
    "vendors": {
        "A00000001": {
            "start_datetime": "2026-09-22T09:00+09:00",
            "end_datetime": "2026-09-22T09:59+09:00",
            "skip": false
        }
    }
}
```

## 변환(Transform)
JSON 형식의 주문 내역을 각각의 DuckDB 테이블에 적재한다.

## 적재(Load)
데이터를 BigQuery/Postgres 테이블 끝에 추가한다.
"""

from airflow.sdk import DAG, task
from airflow.models.dagrun import DagRun
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "coupang_rocket_order",
    schedule = None,
    start_date = pendulum.datetime(2026, 10, 15, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=4),
    max_active_runs = 1,
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:coupang-api", "objective:sales", "credentials:api-key",
        "schedule:none", "time:morning", "time:afternoon", "time:night",
        "write:append", "upstream:dagrun"
    ],
) as dag:

    PATH = "coupang.api.rocket_order"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list[dict]:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    @task(task_id="etl_coupang_rocket_order", map_index_template="{{ credentials['vendor_id'] }}")
    def etl_coupang_rocket_order(credentials: dict, configs: dict, dag_run: DagRun, **kwargs) -> dict:
        from airflow_api import get_next_datetime_range
        from airflow_utils import get_datetime
        conf = (dag_run.conf or dict()).get("vendors") or dict()

        datetime_range = get_next_datetime_range(
            dag_id = dag_run.dag_id,
            task_id = "etl_coupang_rocket_order",
            data_interval_end = get_datetime(kwargs),
            rendered_map_index = credentials["vendor_id"],
            states = ["success"],
            format = "YYYY-MM-DDTHH:mmZ",
            timedelta = { "minutes": 1 },
            conf = (conf.get(credentials["vendor_id"]) or conf.get('*') or dict()),
        )

        return main(**credentials, **configs,
            start_datetime = pendulum.parse(datetime_range["start_datetime"]),
            end_datetime = pendulum.parse(datetime_range["end_datetime"]),
        )

    def main(
            access_key: str,
            secret_key: str,
            vendor_id: str,
            start_datetime: pendulum.DateTime,
            end_datetime: pendulum.DateTime,
            tables: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.api import rocket_order
        from dual_load import load_table_from_duckdb
        source = "coupang_rocket_order"
        add_days = 1 if (end_datetime - start_datetime).days < 2 else 0

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            rocket_order(
                access_key = access_key,
                secret_key = secret_key,
                vendor_id = vendor_id,
                start_date = start_datetime.format("YYYY-MM-DD"),
                end_date = end_datetime.add(days=add_days).format("YYYY-MM-DD"),
                connection = conn,
                return_type = "none",
            )

            where_le = "payment_dt < '{}'".format(start_datetime.format("YYYY-MM-DDTHH:mm:ss"))
            where_gte = "payment_dt >= '{}'".format(end_datetime.add(minutes=1).format("YYYY-MM-DDTHH:mm:ss"))
            conn.execute(f"DELETE FROM {source} WHERE {where_le} OR {where_gte}")

            return {
                "context": {
                    "partitions": sorted(map(str, conn.unique(source, "DATE(payment_dt)"))),
                },
                "params": {
                    "vendor_id": vendor_id,
                    "start_datetime": start_datetime.format("YYYY-MM-DDTHH:mmZ"),
                    "end_datetime": end_datetime.format("YYYY-MM-DDTHH:mmZ"),
                },
                "result": load_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables["table"],
                )
            }


    etl_results = (etl_coupang_rocket_order
        .partial(configs=read_configs())
        .expand(credentials=read_credentials()))
