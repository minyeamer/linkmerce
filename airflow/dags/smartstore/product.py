from airflow.sdk import DAG, task
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "smartstore_product",
    schedule = "30 23 * * 1-5",
    start_date = pendulum.datetime(2025, 9, 3, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    tags = ["priority:high", "smartstore:product", "smartstore:option", "api:smartstore", "schedule:weekdays", "time:night"],
    doc_md = dedent("""
        # 스마트스토어 상품 옵션 ETL 파이프라인

        ## 인증(Credentials)
        스마트스토어 커머스 API 인증 키(애플리케이션 ID/시크릿)가 필요하다.

        ## 추출(Extract)
        스마트스토어 채널별 모든 상품 목록을 수집하고,
        모든 상품코드에 대한 상품 상세 정보를 추가로 가져온다.

        ## 변환(Transform)
        JSON 형식의 응답 본문을 파싱하여 상품 목록을 DuckDB 테이블에 적재하고,
        각각의 상품 상세 정보로부터 옵션 및 추가상품 목록을 추출하여 하나의 옵션 테이블에 적재한다.

        ## 적재(Load)
        각각의 상품, 옵션 테이블을 기존 BigQuery 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
    """).strip(),
) as dag:

    PATH = "smartstore.api.product"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    @task(task_id="etl_smartstore_product", map_index_template="{{ credentials['channel_seq'] }}")
    def etl_smartstore_product(credentials: dict, configs: dict, **kwargs) -> dict:
        return main(**credentials, **configs)

    def main(
            client_id: str,
            client_secret: str,
            channel_seq: str,
            service_account: dict,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.smartstore.api import product
        from linkmerce.api.smartstore.api import option
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = {"product": "smartstore_product", "option": "smartstore_option"}

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            product(
                client_id = client_id,
                client_secret = client_secret,
                status_type = ["ALL"],
                channel_seq = channel_seq,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            option(
                client_id = client_id,
                client_secret = client_secret,
                product_id = conn.unique(sources["product"], "product_id"),
                channel_seq = channel_seq,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "channel_seq": channel_seq,
                        "status_type": ["ALL"],
                    },
                    "counts": {
                        "product": conn.count_table(sources["product"]),
                        "option": conn.count_table(sources["option"]),
                    },
                    "status": {
                        "product": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["product"],
                            staging_table = f'{tables["temp_product"]}_{channel_seq}',
                            target_table = tables["product"],
                            **merge["product"],
                            progress = False,
                        ),
                        "option": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["option"],
                            staging_table = f'{tables["temp_option"]}_{channel_seq}',
                            target_table = tables["option"],
                            **merge["option"],
                            progress = False,
                        ),
                    },
                }


    (etl_smartstore_product
    .partial(configs=read_configs())
    .expand(credentials=read_credentials()))
