"""
# 카페24 상품 및 품목 ETL 파이프라인

## 인증(Credentials)
카페24 Admin API 인증 키(클라이언트 ID/시크릿)와 쇼핑몰 ID 및 OAuth 토큰 파일 경로가 필요하다.

## 추출(Extract)
카페24 쇼핑몰별 모든 상품 목록과 품목 목록을 함께 수집하고,
품목이 100개 이상 반환된 상품은 전체 품목을 별도로 조회해 보완한다.

## 변환(Transform)
JSON 형식의 응답 본문을 파싱하여 상품, 상품 품목 정보를 각각의 DuckDB 테이블에 적재한다.

## 적재(Load)
각각의 상품, 상품 품목 테이블을 기존 BigQuery/Postgres 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
"""

from airflow.sdk import DAG, task
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "cafe24_product",
    schedule = "50 22 * * 1-5",
    start_date = pendulum.datetime(2026, 10, 22, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:low", "platform:cafe24", "objective:product", "credentials:api-key",
        "schedule:weekdays", "time:night", "write:merge"
    ],
) as dag:

    PATH = "cafe24.api.product"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    @task(task_id="etl_cafe24_product", map_index_template="{{ credentials['mall_id'] }}")
    def etl_cafe24_product(credentials: dict, configs: dict, **kwargs) -> dict:
        return main(**credentials, **configs)

    def main(
            mall_id: str,
            client_id: str,
            client_secret: str,
            token_info: str,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.cafe24.api import product_option
        from dual_load import merge_table_from_duckdb
        sources = {"product": "cafe24_product", "product_variant": "cafe24_product_variant"}

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            product_option(
                mall_id = mall_id,
                client_id = client_id,
                client_secret = client_secret,
                token_info = token_info,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            return {
                "params": {
                    "mall_id": mall_id,
                },
                "results": {
                    "product": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["product"],
                        target_table = tables["product"],
                        **merge["product"],
                    ),
                    "product_variant": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["product_variant"],
                        target_table = tables["product_variant"],
                        **merge["product_variant"],
                    ),
                }
            }


    (etl_cafe24_product
        .partial(configs=read_configs())
        .expand(credentials=read_credentials()))
