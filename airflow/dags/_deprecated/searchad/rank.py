from airflow.sdk import DAG, task
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "searchad_rank",
    schedule = "0 6-18 * * *",
    start_date = pendulum.datetime(2025, 8, 15, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=55),
    catchup = False,
    tags = ["priority:mediaum", "naver:rank", "naver:product", "login:searchad", "schedule:hourly", "time:daytime"],
    doc_md = dedent("""
        # 네이버 검색광고 노출 순위 ETL 파이프라인

        > 안내) 네이버 로그인 정책 강화로 사용 중지 (~ v0.6.8)

        ## 인증(Credentials)
        네이버 검색광고 광고 계정을 보유한 네이버 계정의 로그인 쿠키가 필요하다.
        (정책 강화로 인한 CAPTCHA 인증을 통과할 수 없어 API로 대체한다.)

        ## 추출(Extract)
        네이버 검색광고의 키워드 노출 현황 진단 도구로
        키워드별 네이버 통합검색 모바일 노출 순위를 시간대별로 수집한다.

        ## 변환(Transform)
        JSON 형식의 응답 본문을 파싱하여 DuckDB 테이블에 적재한다.
        정규화된 테이블 구성에 맞춰 순위 부분과 상품 정보 부분을 나눠서 적재한다.

        ## 적재(Load)
        - 순위 테이블은 BigQuery 테이블 끝에 추가한다.
        - 동일한 순위 데이터를 최신 데이터만 기록하는 BigQuery 테이블에 MERGE 문으로 덮어쓴다.
        - 상품 테이블은 기존 BigQuery 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
    """).strip(),
) as dag:

    PATH = "searchad.manage.rank"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read
        return read(PATH, tables=True, service_account=True)

    @task(task_id="read_queries", retries=3, retry_delay=timedelta(minutes=1))
    def read_queries() -> list:
        from airflow_utils import read, split_by_credentials
        configs = read(PATH, credentials=True, tables=True, sheets=True)
        credentials = filter_valid_accounts(configs["credentials"])
        return split_by_credentials(credentials, keyword=configs["keyword"])

    def filter_valid_accounts(credentials: list[dict]) -> list[dict]:
        """`has_permission` 함수로 네이버 검색광고 시스템에 권한이 있는 인증 정보만 필터한다."""
        from linkmerce.api.searchad.manage import has_permission # v1.0.0부터 함수 사용 불가
        accounts = [credential for credential in credentials if has_permission(**credential)]
        if credentials and (not accounts):
            from airflow.exceptions import AirflowFailException
            raise AirflowFailException("At least one account does not have permissions")
        return accounts


    CHUNK = 100

    @task(task_id="etl_searchad_rank", map_index_template="{{ queries['customer_id'] }}", retries=3, retry_delay=timedelta(minutes=1))
    def etl_searchad_rank(queries: dict, configs: dict) -> list[dict]:
        keywords = queries.pop("keyword")
        return [main(**queries, keyword=keywords[i:i+CHUNK], **configs, seq=(i//CHUNK)) for i in range(0, len(keywords), CHUNK)]

    def main(
            customer_id: int | str,
            cookies: str,
            keyword: list[str],
            service_account: dict,
            tables: dict[str, str],
            merge: dict[str, dict],
            seq: int = 0,
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.searchad.manage import exposure_rank
        from linkmerce.extensions.bigquery import BigQueryClient
        import logging
        import time
        sources = {"rank": "searchad_rank", "product": "searchad_product"}

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            start_time = time.time()
            exposure_rank(
                customer_id = customer_id,
                cookies = cookies,
                keyword = keyword,
                domain = "search",
                mobile = True,
                connection = conn,
                progress = False,
                return_type = "none",
            )
            end_time = time.time()
            minutes, seconds = map(int, divmod(end_time - start_time, 60))
            logging.info(f"[{seq}] API request completed for searching {len(keyword)} keywords in {minutes:02d}:{seconds:02d}")

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "customer_id": customer_id,
                        "keyword": len(keyword),
                        "domain": "search",
                        "mobile": True,
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
                            staging_table = f'{tables["temp_now"]}_{customer_id}',
                            target_table = tables["now"],
                            **merge["now"],
                            progress = False,
                        ),
                        "product": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["product"],
                            staging_table = f'{tables["temp_product"]}_{customer_id}',
                            target_table = tables["product"],
                            **merge["product"],
                            progress = False,
                        ),
                    },
                }


    (etl_searchad_rank
    .partial(configs=read_configs())
    .expand(queries=read_queries()))
