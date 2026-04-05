from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "naver_brand_price",
    schedule = "1 0 * * *",
    start_date = pendulum.datetime(2025, 8, 15, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(hours=1),
    catchup = False,
    tags = ["priority:medium", "naver:price", "naver:product", "login:hcenter", "schedule:daily", "time:night"],
    doc_md = dedent("""
        # 네이버 브랜드스토어 상품 가격 ETL 파이프라인

        ## 인증(Credentials)
        네이버 쇼핑파트너센터 로그인 쿠키가 필요하다.
        (브랜드스토어 권한이 필요하고, '브랜드 관리' 메뉴에 도달해야 한다.)

        ## 추출(Extract)
        실행 시점에서 조회되는 판매처별 브랜드 상품들의 목록을 수집한다.
        (매개변수로 전달되는 브랜드ID에 대한 브랜드가 등록된 상품들만 검색할 수 있다.)

        ## 변환(Transform)
        JSON 형식의 응답 본문을 파싱하여 DuckDB 테이블에 적재한다.
        정규화된 테이블 구성에 맞춰 가격 부분과 상품 정보 부분을 나눠서 적재한다.

        ## 적재(Load)
        - 가격 테이블은 BigQuery 테이블 끝에 추가한다.
        - 상품 테이블은 기존 BigQuery 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
    """).strip(),
) as dag:

    PATH = "smartstore.hcenter.price"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read
        return read(PATH, credentials="expand", tables=True, sheets=True, service_account=True)


    @task(task_id="etl_naver_brand_price")
    def etl_naver_brand_price(ti: TaskInstance, **kwargs) -> dict:
        return main(**ti.xcom_pull(task_ids="read_configs"))

    def main(
            brand_ids: list[str],
            mall_seq: list[int],
            cookies: str,
            service_account: dict,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.smartstore.hcenter import brand_price
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = {"price": "naver_price_history", "product": "naver_product"}

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            brand_price(
                cookies = cookies,
                brand_ids = brand_ids,
                mall_seq = mall_seq,
                sort_type = "recent",
                page = None,
                connection = conn,
                how_to_run = "async",
                progress = False,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "brand_ids": len(brand_ids),
                        "sort_type": "recent",
                        "page": None,
                    },
                    "counts": {
                        "price": conn.count_table(sources["price"]),
                        "product": conn.count_table(sources["product"]),
                    },
                    "status": {
                        "price": client.load_table_from_duckdb(
                            connection = conn,
                            source_table = sources["price"],
                            target_table = tables["price"],
                            progress = False,
                        ),
                        "product": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["product"],
                            staging_table = tables["temp_product"],
                            target_table = tables["product"],
                            **merge["product"],
                            progress = False,
                        ),
                    },
                }


    read_configs() >> etl_naver_brand_price()
