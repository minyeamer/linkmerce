from airflow.sdk import DAG, task
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "searchad_contract",
    schedule = "30 5 * * *",
    start_date = pendulum.datetime(2025, 10, 30, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=5),
    catchup = False,
    tags = ["priority:medium", "searchad:contract", "api:searchad", "schedule:daily", "time:morning"],
    doc_md = dedent("""
        # 네이버 검색광고 계약 정보 ETL 파이프라인

        ## 인증(Credentials)
        네이버 검색광고 API 인증 키(액세스라이선스, 비밀키)와 CUSTOMER_ID가 필요하다.

        ## 추출(Extract)
        브랜드/신제품검색 계약 API를 통해 계정별 모든 계약을 수집한다.

        ## 변환(Transform)
        계약 유형별로 JSON 형식의 응답 본문을 파싱하여 각각의 DuckDB 테이블에 적재한 뒤,
        UNION ALL로 하나의 테이블로 병합한다.

        ## 적재(Load)
        병합된 테이블의 데이터를 기존 BigQuery 테이블을 지우고 덮어쓴다.
    """).strip(),
) as dag:

    PATH = "searchad.api.contract"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read
        return read(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read
        return read(PATH, credentials=True)["credentials"]


    @task(task_id="etl_searchad_contract", map_index_template="{{ queries['customer_id'] }}")
    def etl_searchad_contract(queries: dict, configs: dict, **kwargs) -> dict:
        return main(**queries, **configs)

    def main(
            api_key: str,
            secret_key: str,
            customer_id: int | str,
            service_account: dict,
            tables: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.searchad.api import time_contract, brand_new_contract
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = {
            "time": "searchad_contract",
            "brand_new": "searchad_contract_new",
            "contract": "searchad_contract_total",
        }

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            time_contract(
                api_key = api_key,
                secret_key = secret_key,
                customer_id = customer_id,
                connection = conn,
                return_type = "none",
            )

            brand_new_contract(
                api_key = api_key,
                secret_key = secret_key,
                customer_id = customer_id,
                connection = conn,
                return_type = "none",
            )

            conn.execute(dedent(f"""
                CREATE OR REPLACE TABLE {sources['contract']} AS
                SELECT * FROM {sources['time']}
                UNION ALL
                SELECT * FROM {sources['brand_new']}
            """).strip())

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "customer_id": customer_id,
                    },
                    "counts": {
                        "time": conn.count_table(sources["time"]),
                        "brand_new": conn.count_table(sources["brand_new"]),
                    },
                    "status": {
                        "contract": client.overwrite_table_from_duckdb(
                            connection = conn,
                            source_table = sources["contract"],
                            target_table = tables["contract"],
                            where_clause = f"(contract_end_date > '2000-01-01') AND (customer_id = {customer_id})",
                            progress = False,
                        ),
                    },
                }


    etl_searchad_contract.partial(configs=read_configs()).expand(queries=read_credentials())
