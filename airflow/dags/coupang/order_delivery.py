"""
# 쿠팡 마켓플레이스 발주서 배송 및 상태 ETL 파이프라인

> 안내) 쿠팡 마켓플레이스 발주서를 수집하는 'coupang_order' Dag 실행 후 매시 0분에 트리거된다.

## 인증(Credentials)
쿠팡 Open API 인증 키(Access Key, Secret Key, 업체 코드)가 필요하다.

## 추출(Extract)
각 업체별 직전에 성공한 Task Instance 반환 값의 'params.end_datetime'부터
현재 실행 시점('data_interval_end')보다 1분 앞선 시점까지를 필터 기간으로 설정한다.

필터 기간의 종료일을 기준으로 이전 14일간 발생한 전체 발주서 목록을 수집한 후,
발송일시를 기준으로 필터 기간에 해당하는 발주서만 추출한다.
(단, 필터 기간이 1일을 초과하는 경우 조회 기간의 시작일은 필터 기간의 시작일로부터 14일 전으로 한다.)

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
    },
    "dbt_run": true
}
```

## 변환(Transform)
JSON 형식의 발주서 내역으로부터 배송 정보를 분리해 각각의 DuckDB 테이블에 적재한다.

## 적재(Load)
데이터를 BigQuery/Postgres 테이블 끝에 추가한다.
"""

from airflow.sdk import DAG, task
from airflow.models.dagrun import DagRun
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "coupang_order_delivery",
    schedule = None,
    start_date = pendulum.datetime(2026, 10, 12, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=4),
    max_active_runs = 1,
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:coupang-api", "objective:delivery", "credentials:api-key",
        "schedule:none", "time:morning", "time:afternoon", "time:night",
        "write:append", "upstream:dagrun"
    ],
) as dag:

    PATH = "coupang.api.order_delivery"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list[dict]:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    @task(task_id="etl_coupang_order_delivery", map_index_template="{{ credentials['vendor_id'] }}")
    def etl_coupang_order_delivery(credentials: dict, configs: dict, dag_run: DagRun, **kwargs) -> dict:
        from airflow_api import get_next_datetime_range
        from airflow_utils import get_datetime
        conf = (dag_run.conf or dict()).get("vendors") or dict()

        datetime_range = get_next_datetime_range(
            dag_id = dag_run.dag_id,
            task_id = "etl_coupang_order_delivery",
            data_interval_end = get_datetime(kwargs),
            rendered_map_index = credentials["vendor_id"],
            states = ["success"],
            format = "YYYY-MM-DDTHH:mmZ",
            timedelta = { "minutes": 1 },
            conf = (conf.get(credentials["vendor_id"]) or conf.get('*') or dict()),
        )
        return main(**credentials, **configs, ranges=parse_datetime_range(**datetime_range))

    def parse_datetime_range(start_datetime: str, end_datetime: str) -> dict:
        """\"yyyy-mm-ddT00:00+09:00\" 형태의 문자열 필터 기간을 파싱하고 조회 기간을 계산해 함께 반환한다."""
        filter_start_datetime = pendulum.parse(start_datetime)
        filter_end_datetime = pendulum.parse(end_datetime)

        if (filter_end_datetime - filter_start_datetime).days > 1:
            search_start_datetime = filter_start_datetime.subtract(days=14)
        else:
            search_start_datetime = filter_end_datetime.subtract(days=14, minutes=-1)

        return {
            "filter": {
                "gte": filter_start_datetime,
                "le": filter_end_datetime.add(minutes=1),
            },
            "search": {
                "start": search_start_datetime,
                "end": filter_end_datetime,
            }
        }

    def main(
            access_key: str,
            secret_key: str,
            vendor_id: str,
            ranges: dict[str, dict[str, pendulum.DateTime]],
            tables: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.api import order
        from dual_load import load_table_from_duckdb
        source = "coupang_order_delivery"
        format = "YYYY-MM-DDTHH:mmZ"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            order(
                access_key = access_key,
                secret_key = secret_key,
                vendor_id = vendor_id,
                start_datetime = ranges["search"]["start"].format(format),
                end_datetime = ranges["search"]["end"].format(format),
                status = ["DEPARTURE", "DELIVERING", "FINAL_DELIVERY"],
                connection = conn,
                progress = False,
                return_type = "none",
            )

            where_le = "send_dt < '{}'".format(ranges["filter"]["gte"].format("YYYY-MM-DDTHH:mm:ss"))
            where_gte = "send_dt >= '{}'".format(ranges["filter"]["le"].format("YYYY-MM-DDTHH:mm:ss"))
            conn.execute(f"DELETE FROM {source} WHERE send_dt IS NULL OR {where_le} OR {where_gte}")

            return {
                "context": {
                    "partitions": sorted(map(str, conn.unique(source, "DATE(order_dt)"))),
                },
                "params": {
                    "vendor_id": vendor_id,
                    "status": ["DEPARTURE", "DELIVERING", "FINAL_DELIVERY"],
                    "end_datetime": ranges["search"]["end"].format(format),
                    "ranges": {
                        "filter": {
                            "gte": ranges["filter"]["gte"].format(format),
                            "le": ranges["filter"]["le"].format(format),
                        },
                        "search": {
                            "start": ranges["search"]["start"].format(format),
                            "end": ranges["search"]["end"].format(format),
                        }
                    },
                },
                "result": load_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables["table"],
                )
            }


    etl_results = (etl_coupang_order_delivery
        .partial(configs=read_configs())
        .expand(credentials=read_credentials()))
