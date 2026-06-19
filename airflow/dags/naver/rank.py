"""
# 네이버 쇼핑 검색 순위 ETL 파이프라인

> 안내) 실행 후 카탈로그-상품 매핑 내역을 수집하는 `naver_product_catalog` DAG을 트리거한다.

## 인증(Credentials)
네이버 오픈 API 인증 키(Client ID, Client Secret)가 필요하다.

## 추출(Extract)
네이버 쇼핑 검색 API로 키워드별 상품 노출 순위를 최대 300위까지 수집한다.

## 변환(Transform)
JSON 형식의 응답 본문을 파싱하여 DuckDB 테이블에 적재한다.
쇼핑 검색 결과로부터 상품 순위와 상품 정보를 각각의 테이블로 분리해 적재한다.

## 적재(Load)
- 상품 순위 테이블은 BigQuery/Postgres 테이블 끝에 추가한다.
- 동일한 순위 데이터를 최신 데이터만 기록하는 BigQuery/Postgres 테이블에 MERGE 문으로 덮어쓴다.
- 상품 정보 테이블은 기존 BigQuery/Postgres 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
"""

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.task.trigger_rule import TriggerRule
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "naver_shop_rank",
    schedule = "0 6-18 * * *",
    start_date = pendulum.datetime(2025, 8, 15, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=55),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:mediaum", "platform:naver-shop",
        "objective:rank", "objective:product", "credentials:api-key",
        "schedule:hourly", "time:morning", "time:afternoon", "write:append", "write:merge",
        "status:disabled"
    ],
) as dag:

    PATH = "naver.openapi.shop_rank"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_queries", retries=3, retry_delay=timedelta(minutes=1))
    def read_queries() -> list:
        from airflow_utils import read_config, split_by_credentials
        configs = read_config(PATH, credentials=True, tables=True, sheets=True)
        queries = split_by_credentials(configs["credentials"], keyword=configs["keyword"])
        return [dict(query, seq=i) for i, query in enumerate(queries)]


    CHUNK = 100

    @task(task_id="etl_naver_shop_rank", retries=3, retry_delay=timedelta(minutes=1))
    def etl_naver_shop_rank(queries: dict, configs: dict) -> dict:
        return main(**queries, **configs)

    def main(
            client_id: str,
            client_secret: str,
            keyword: list[str],
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.naver.openapi import shop_rank
        from dual_load import load_table_from_duckdb, merge_table_from_duckdb
        sources = {"rank": "naver_shop_rank", "product": "naver_shop_product"}

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            shop_rank(
                client_id = client_id,
                client_secret = client_secret,
                query = keyword,
                start = [1, 101, 201],
                sort = "sim",
                connection = conn,
                how_to_run = "async",
                progress = False,
                return_type = "none",
            )

            return {
                "params": {
                    "query": len(keyword),
                    "start": [1, 101, 201],
                    "sort": "sim",
                },
                "results": {
                    "rank": load_table_from_duckdb(
                        connection = conn,
                        source_table = sources["rank"],
                        target_table = tables["rank"],
                    ),
                    "now": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["rank"],
                        target_table = tables["now"],
                        **merge["now"],
                    ),
                    "product": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["product"],
                        target_table = tables["product"],
                        **merge["product"],
                    ),
                }
            }


    trigger_naver_product_catalog = TriggerDagRunOperator(
        task_id = "trigger_naver_product_catalog",
        trigger_dag_id = "naver_product_catalog",
        trigger_run_id = None,
        trigger_rule = TriggerRule.ONE_SUCCESS,
        logical_date = "{{ logical_date }}",
        reset_dag_run = True,
        wait_for_completion = False,
    )


    (etl_naver_shop_rank
    .partial(configs=read_configs())
    .expand(queries=read_queries())) >> trigger_naver_product_catalog
