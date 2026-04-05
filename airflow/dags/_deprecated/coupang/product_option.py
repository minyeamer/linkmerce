from airflow.sdk import DAG, task
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "coupang_product_option",
    schedule = "20 9 * * *",
    start_date = pendulum.datetime(2025, 11, 4, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:medium", "coupang:option", "login:coupang", "schedule:daily", "time:morning"],
    doc_md = dedent("""
        # 쿠팡 상품 옵션 ETL 파이프라인

        > 안내) 쿠팡 윙 로그인 정책 강화로 사용 중지 (~ v0.6.8)

        ## 인증(Credentials)
        쿠팡 윙 로그인 쿠키가 필요하다.
        (정책 강화로 마지막으로 로그인된 쿠키만 사용할 수 있다.)

        ## 추출(Extract)
        쿠팡 업체별 상품 옵션 목록을 수집하고,
        쿠팡 로켓 재고 현황으로부터 로켓 옵션 목록을 수집해 병합한다.
        각각 'see_more=True' 파라미터에 의해 상품별 대표 옵션뿐 아니라,
        상품 페이지를 하나씩 접속하면서 전체 옵션 목록을 수집한다.

        ## 변환(Transform)
        JSON 형식의 응답 본문을 파싱하여 옵션 목록을 DuckDB 테이블에 적재한다.
        'see_more=True' 파라미터에 의해 수집되는 상세 옵션 목록을 동일한 테이블에 적재해
        누락없는 전체 옵션 목록을 완성한다.
        로켓 옵션 목록은 'main' 함수에서 INSERT 문을 실행해 옵션 테이블로 옮긴다.

        ## 적재(Load)
        상품 옵션 목록과 로켓 옵션 목록을 통합한 하나의 테이블을
        기존 BigQuery 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
    """).strip(),
) as dag:

    PATH = "coupang.wing.product_option"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read
        return read(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read
        return read(PATH, credentials=True)["credentials"]


    @task(task_id="etl_coupang_option", map_index_template="{{ credentials['vendor_id'] }}", pool="coupang_pool")
    def etl_coupang_option(credentials: dict, configs: dict, **kwargs) -> dict:
        return main(**credentials, **configs)

    def main(
            cookies: str,
            vendor_id: str,
            service_account: dict,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.wing import product_option, rocket_option
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = {"table": "coupang_product", "rfm": "coupang_rocket_option"}

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            for is_deleted in [False, True]:
                product_option(
                    cookies = cookies,
                    is_deleted = is_deleted,
                    see_more = True,
                    connection = conn,
                    progress = False,
                    return_type = "none",
                )

            rocket_option(
                cookies = cookies,
                hidden_status = None,
                vendor_id = vendor_id,
                see_more = True,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            if conn.table_has_rows(sources["rfm"]):
                conn.execute(insert_rocket_options())

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "vendor_id": vendor_id,
                        "is_deleted": [False, True],
                        "see_more": True,
                    },
                    "counts": {
                        "table": conn.count_table(sources["table"]),
                        "rfm": conn.count_table(sources["rfm"]),
                    },
                    "status": {
                        "table": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["table"],
                            staging_table = f'{tables["temp_option"]}_{vendor_id}',
                            target_table = tables["option"],
                            **merge["option"],
                            progress = False,
                        ),
                    },
                }

    def insert_rocket_options() -> str:
        return dedent("""
            INSERT INTO coupang_product (
                vendor_inventory_id
                , vendor_inventory_item_id
                , product_id
                , option_id
                , item_id
                , barcode
                , vendor_id
                , product_name
                , option_name
                , display_category_id
                , category_id
                , category_name
                , product_status
                , price
                , sales_price
                , order_quantity
                , stock_quantity
                , register_dt
            )
            SELECT * FROM coupang_rocket_option ON CONFLICT DO NOTHING
            """).strip()


    etl_coupang_option.partial(configs=read_configs()).expand(credentials=read_credentials())
