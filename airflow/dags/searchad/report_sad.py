from airflow.sdk import DAG, task
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "searchad_report_sad",
    schedule = "40 5 * * *",
    start_date = pendulum.datetime(2025, 8, 24, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    tags = ["priority:high", "searchad:report", "api:searchad", "schedule:daily", "time:morning"],
    doc_md = dedent("""
        # 네이버 검색광고 다차원 보고서 ETL 파이프라인

        ## 인증(Credentials)
        네이버 검색광고 API 인증 키(액세스라이선스, 비밀키)와 CUSTOMER_ID가 필요하다.

        ## 추출(Extract)
        실행 시점(data_interval_end)에서 1일 전을 기준으로,
        대용량 보고서를 생성, 조회, 삭제하는 API를 순차적으로 실행하면서
        광고성과 및 전환 보고서를 수집한다.

        ## 변환(Transform)
        TSV 형식의 응답 본문을 파싱하고, 하나의 다차원 보고서로 병합해 DuckDB 테이블에 적재한다.

        ## 적재(Load)
        보고서 데이터를 BigQuery 테이블 끝에 추가한다.
    """).strip(),
) as dag:

    PATH = "searchad.api.report"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    @task(task_id="etl_searchad_report_sad", map_index_template="{{ credentials['customer_id'] }}")
    def etl_searchad_report_sad(credentials: dict, configs: dict, **kwargs) -> dict:
        from airflow_utils import get_execution_date
        return main_searchad(**credentials, date=get_execution_date(kwargs, subdays=1), **configs)

    def main_searchad(
            api_key: str,
            secret_key: str,
            customer_id: int | str,
            date: str,
            service_account: dict,
            tables: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.searchad.api import advanced_report
        from linkmerce.extensions.bigquery import BigQueryClient
        source = "searchad_report"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            advanced_report(
                api_key = api_key,
                secret_key = secret_key,
                customer_id = customer_id,
                start_date = date,
                end_date = date,
                connection = conn,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "customer_id": customer_id,
                        "date": date,
                    },
                    "counts": {
                        "table": conn.count_table(source),
                    },
                    "status": {
                        "table": client.load_table_from_duckdb(
                            connection = conn,
                            source_table = source,
                            target_table = tables["table"],
                            progress = False,
                        ),
                    },
                }


    (etl_searchad_report_sad
    .partial(configs=read_configs())
    .expand(credentials=read_credentials()))
