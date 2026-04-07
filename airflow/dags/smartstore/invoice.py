from airflow.sdk import DAG, task
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from datetime import timedelta
from textwrap import dedent
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
    tags = ["priority:high", "smartstore:order", "api:smartstore", "schedule:daily", "time:daytime"],
    doc_md = dedent("""
        # 스마트스토어 운송장 ETL 파이프라인

        ## 인증(Credentials)
        스마트스토어 커머스 API 인증 키(애플리케이션 ID/시크릿)가 필요하다.

        ## 추출(Extract)
        영업일 중 오전/오후 배송 접수 후, 결제일 기준 전일의 스마트스토어 주문 내역을 다운로드 받는다.
        매일 새벽에 발송일 기준으로 주문 내역을 다시 조회해 누락을 검증한다.

        ## 변환(Transform)
        JSON 형식의 응답 본문에서 주문 배송 정보를 추출해 DuckDB 테이블에 적재한다.

        ## 적재(Load)
        기존 BigQuery 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
    """).strip(),
) as dag:

    PATH = "smartstore.api.invoice"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    FIRST_SCHEDULE = "03:00"

    @task(task_id="etl_smartstore_invoice", map_index_template="{{ credentials['channel_seq'] }}")
    def etl_smartstore_invoice(credentials: dict, configs: dict, **kwargs) -> dict:
        from airflow_utils import get_execution_date
        range_type = "DISPATCHED_DATETIME" if get_execution_date(kwargs, fmt="HH:mm") == FIRST_SCHEDULE else "PAYED_DATETIME"
        return main(**credentials, date=get_execution_date(kwargs, subdays=1), range_type=range_type, **configs)

    def main(
            client_id: str,
            client_secret: str,
            channel_seq: str,
            date: str,
            range_type: str,
            service_account: dict,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.smartstore.api import order as smartstore_order
        from linkmerce.extensions.bigquery import BigQueryClient
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

            date_array = conn.unique(source, "DATE(payment_dt)")

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "channel_seq": channel_seq,
                        "date": date,
                        "range_type": range_type,
                    },
                    "counts": {
                        "table": conn.count_table(source),
                    },
                    "dates": {
                        "table": sorted(map(str, date_array)),
                    },
                    "status": {
                        "table": (client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = source,
                            staging_table = f'{tables["temp_table"]}_{channel_seq}',
                            target_table = tables["table"],
                            **merge["table"],
                            where_clause = conn.expr_date_range("DATE(T.payment_dt)", date_array),
                            progress = False,
                        ) if date_array else True),
                    },
                }


    (etl_smartstore_invoice
    .partial(configs=read_configs())
    .expand(credentials=read_credentials()))
