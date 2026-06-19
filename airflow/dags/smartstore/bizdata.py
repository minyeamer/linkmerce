"""
# 스마트스토어 상품/마케팅 채널 데이터 ETL 파이프라인

## 인증(Credentials)
스마트스토어 커머스 API 인증 키(애플리케이션 ID/시크릿)가 필요하다.

## 추출(Extract)
상품/마케팅 채널 API로 마케팅 채널별 상품 결제 데이터를 수집한다.

## 변환(Transform)
JSON 형식의 응답 본문을 파싱하여 DuckDB 테이블에 적재한다.

## 적재(Load)
데이터를 BigQuery/Postgres 테이블 끝에 추가한다.
"""

from airflow.sdk import DAG, task
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "smartstore_bizdata",
    schedule = "10 8 * * *",
    start_date = pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:low", "platform:smartstore", "objective:statistics", "credentials:api-key",
        "schedule:daily", "time:morning", "write:append"
    ],
) as dag:

    PATH = "smartstore.bizdata.marketing_channel"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    @task(task_id="etl_smartstore_bizdata", map_index_template="{{ credentials['channel_seq'] }}")
    def etl_smartstore_bizdata(credentials: dict, configs: dict, **kwargs) -> dict:
        from airflow_utils import format_datetime
        return main(**credentials, date=format_datetime(kwargs, subdays=1), **configs)

    def main(
            client_id: str,
            client_secret: str,
            channel_seq: str,
            date: str,
            tables: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.smartstore.api import marketing_channel
        from dual_load import load_table_from_duckdb
        source = "marketing_channel"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            marketing_channel(
                client_id = client_id,
                client_secret = client_secret,
                channel_seq = channel_seq,
                start_date = date,
                end_date = date,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            return {
                "params": {
                    "channel_seq": channel_seq,
                    "date": date,
                },
                "result": load_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables["table"],
                )
            }


    (etl_smartstore_bizdata
    .partial(configs=read_configs())
    .expand(credentials=read_credentials()))
