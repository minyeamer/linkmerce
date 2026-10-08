"""
# 쿠팡 상품 및 옵션 ETL 파이프라인

## 인증(Credentials)
쿠팡 Open API 인증 키(Access Key, Secret Key, 업체 코드)가 필요하다.

## 추출(Extract)
쿠팡 마켓플레이스 및 로켓그로스의 모든 상품 목록을 수집하고,
모든 상품코드에 대한 상품 상세 정보를 추가로 가져온다.

## 변환(Transform)
JSON 형식의 응답 본문을 파싱하여 상품 목록을 DuckDB 테이블에 적재하고,
상품과 옵션 정보에서 서로 간에 누락된 데이터를 병합하여 통합된 상품-옵션 테이블을 생성한다.

## 적재(Load)
각각의 상품, 옵션 테이블을 기존 BigQuery/Postgres 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
"""

from airflow.sdk import DAG, task
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "coupang_product",
    schedule = "0 23 * * 1-5",
    start_date = pendulum.datetime(2026, 10, 8, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:medium", "platform:coupang-api", "objective:product", "credentials:api-key",
        "schedule:weekdays", "time:night", "write:merge"
    ],
) as dag:

    PATH = "coupang.api.product"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    @task(task_id="etl_coupang_product", map_index_template="{{ credentials['vendor_id'] }}")
    def etl_coupang_product(credentials: dict, configs: dict, **kwargs) -> dict:
        return main(**credentials, **configs)

    def main(
            access_key: str,
            secret_key: str,
            vendor_id: str,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.api import product_option
        from dual_load import merge_table_from_duckdb
        sources = {
            "merged": "coupang_product_option",
            "product": "coupang_product",
            "item": "coupang_item",
        }

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            product_option(
                access_key = access_key,
                secret_key = secret_key,
                vendor_id = vendor_id,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            conn.sql(extract_product(sources["merged"], sources["product"]))
            conn.sql(extract_item(sources["merged"], sources["item"]))

            return {
                "params": {
                    "vendor_id": vendor_id,
                    "business_types": [None, "rocketGrowth"],
                    "statuses": [None, "DELETED"],
                },
                "results": {
                    "product": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["product"],
                        target_table = tables["product"],
                        **merge["product"],
                    ),
                    "item": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["item"],
                        target_table = tables["item"],
                        **merge["item"],
                    ),
                }
            }


    def extract_product(source: str, target: str) -> str:
        return dedent(f"""
            CREATE TABLE {target} AS
            SELECT
                vendor_inventory_id
                , product_id
                , vendor_id
                , product_name
                , display_category_id
                , category_id
                , brand_name
                , maker_name
                , product_status
                , delivery_fee
                , sales_started_at
                , sales_ended_at
                , created_at
            FROM {source}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY vendor_inventory_id)
            """).strip()

    def extract_item(source: str, target: str) -> str:
        return dedent(f"""
            CREATE TABLE {target} AS
            SELECT
                vendor_inventory_id
                , vendor_inventory_item_id
                , product_id
                , option_id
                , item_id
                , vendor_id
                , option_name
                , barcode
                , image_url
                , price
                , sales_price
            FROM {source}
            """).strip()


    etl_results = (etl_coupang_product
        .partial(configs=read_configs())
        .expand(credentials=read_credentials()))
