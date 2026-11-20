"""
# 쿠팡 매출 정산 현황 ETL 파이프라인

## 인증(Credentials)
쿠팡 Open API 인증 키(Access Key, Secret Key, 업체 코드)가 필요하다.

## 추출(Extract)
실행 시점(data_interval_end)에서 1일 전을 기준으로 쿠팡 마켓플레이스 매출내역을 수집한다.

## 변환(Transform)
JSON 형식의 응답 본문을 파싱하여 DuckDB 테이블에 적재한다.

## 적재(Load)
데이터를 BigQuery/Postgres 테이블의 끝에 추가한다.
"""

from airflow.sdk import DAG, task
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "coupang_settlement",
    schedule = "50 8 * * *",
    start_date = pendulum.datetime(2026, 11, 21, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:coupang-api", "objective:sales", "credentials:api-key",
        "schedule:daily", "time:morning", "write:append"
    ],
) as dag:

    PATH = "coupang.api.settlement"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    @task(task_id="etl_coupang_settlement", map_index_template="{{ credentials['vendor_id'] }}")
    def etl_coupang_settlement(credentials: dict, configs: dict, **kwargs) -> dict:
        from airflow_utils import format_datetime
        return main(**credentials, date=format_datetime(kwargs, subdays=1), **configs)

    def main(
            access_key: str,
            secret_key: str,
            vendor_id: str,
            date: str,
            tables: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.api import revenue_history
        from dual_load import load_table_from_duckdb
        source = "coupang_revenue_history"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            revenue_history(
                access_key = access_key,
                secret_key = secret_key,
                vendor_id = vendor_id,
                start_date = date,
                end_date = date,
                connection = conn,
                return_type = "none",
            )

            return {
                "context": {
                    "partitions": sorted(map(str, conn.unique(source, "paid_date"))),
                },
                "params": {
                    "vendor_id": vendor_id,
                    "date": date,
                },
                "result": load_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables["table"],
                ),
            }


    (etl_coupang_settlement
    .partial(configs=read_configs())
    .expand(credentials=read_credentials()))
