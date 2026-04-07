from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "naver_product_catalog",
    schedule = None, # `naver_rank_shop` Dag 실행 후 트리거 (0 6-18 * * *)
    start_date = pendulum.datetime(2025, 8, 15, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:mediaum", "naver:rank", "login:hcenter", "schedule:hourly", "time:daytime", "manual:dagrun"],
    doc_md = dedent("""
        # 네이버 상품-카탈로그 매핑 ETL 파이프라인

        > 안내) 네이버 쇼핑 검색 순위를 수집하는 `naver_shop_rank` Dag 실행 후 트리거된다.

        ## 인증(Credentials)
        네이버 쇼핑파트너센터 로그인 쿠키가 필요하다.
        (브랜드스토어 권한이 필요하고, '브랜드 관리' 메뉴에 도달해야 한다.)

        ## 추출(Extract)
        실행 시점에서 조회되는 판매처별 브랜드 상품들의 목록을 수집한다.
        (매개변수로 전달되는 브랜드ID에 대한 브랜드가 등록된 상품들만 검색할 수 있다.)

        ## 변환(Transform)
        JSON 형식의 응답 본문으로부터 카탈로그와 매칭된 상품만 필터한다.
        상품-카탈로그 매핑 관계를 정리하여 DuckDB 테이블에 적재한다.

        ## 적재(Load)
        - 현재 시간이 포함된 매핑 테이블을 일별 BigQuery 테이블에 끝에 추가해 누적한다.
        - 동일한 매핑 데이터를 최신 데이터만 기록하는 BigQuery 테이블에 MERGE 문으로 덮어쓴다.
    """).strip(),
) as dag:

    PATH = "smartstore.hcenter.product_catalog"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, credentials="expand", tables=True, sheets=True, service_account=True)


    @task(task_id="etl_product_catalog", retries=3, retry_delay=timedelta(minutes=1))
    def etl_product_catalog(ti: TaskInstance, **kwargs) -> dict:
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
        from linkmerce.api.smartstore.hcenter import product_catalog
        from linkmerce.extensions.bigquery import BigQueryClient
        source = "naver_catalog_product"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            product_catalog(
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
                        "table": conn.count_table(source),
                    },
                    "status": {
                        "table": client.load_table_from_duckdb(
                            connection = conn,
                            source_table = source,
                            target_table = tables["table"],
                            progress = False,
                        ),
                        "now": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = source,
                            staging_table = tables["temp_now"],
                            target_table = tables["now"],
                            **merge["now"],
                            progress = False,
                        ),
                    },
                }


    read_configs() >> etl_product_catalog()
