from airflow.sdk import DAG, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.task.trigger_rule import TriggerRule
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "naver_shop_rank",
    schedule = "0 6-18 * * *",
    start_date = pendulum.datetime(2025, 8, 15, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=55),
    catchup = False,
    tags = ["priority:mediaum", "naver:rank", "naver:product", "api:naver-openapi", "schedule:hourly", "time:daytime"],
    doc_md = dedent("""
        # 네이버 쇼핑 검색 순위 ETL 파이프라인

        > 안내) 실행 후 상품-카탈로그 매핑 관계를 수집하는 `naver_product_catalog` DAG을 트리거한다.

        ## 인증(Credentials)
        네이버 오픈 API 인증 키(Client ID, Client Secret)가 필요하다.

        ## 추출(Extract)
        네이버 쇼핑 검색 API로 키워드별 상품 노출 순위를 최대 300위까지 수집한다.

        ## 변환(Transform)
        JSON 형식의 응답 본문을 파싱하여 DuckDB 테이블에 적재한다.
        정규화된 테이블 구성에 맞춰 순위 부분과 상품 정보 부분을 나눠서 적재한다.

        ## 적재(Load)
        - 순위 테이블은 BigQuery 테이블 끝에 추가한다.
        - 동일한 순위 데이터를 최신 데이터만 기록하는 BigQuery 테이블에 MERGE 문으로 덮어쓴다.
        - 상품 테이블은 기존 BigQuery 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
    """).strip(),
) as dag:

    PATH = "naver.openapi.shop_rank"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True, service_account=True)

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
            seq: int,
            service_account: dict,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.naver.openapi import shop_rank
        from linkmerce.extensions.bigquery import BigQueryClient
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

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "query": len(keyword),
                        "start": [1, 101, 201],
                        "sort": "sim",
                    },
                    "counts": {
                        "rank": conn.count_table(sources["rank"]),
                        "product": conn.count_table(sources["product"]),
                    },
                    "status": {
                        "rank": client.load_table_from_duckdb(
                            connection = conn,
                            source_table = sources["rank"],
                            target_table = tables["rank"],
                            progress = False,
                        ),
                        "now": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["rank"],
                            staging_table = f'{tables["temp_now"]}_{seq}',
                            target_table = tables["now"],
                            **merge["now"],
                            progress = False,
                        ),
                        "product": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["product"],
                            staging_table = f'{tables["temp_product"]}_{seq}',
                            target_table = tables["product"],
                            **merge["product"],
                            progress = False,
                        ),
                    },
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
