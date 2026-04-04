from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "naver_brand_sales",
    schedule = "10,30,50 9,10 * * *",
    start_date = pendulum.datetime(2025, 10, 24, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    tags = ["priority:high", "naver:sales", "naver:product", "login:hcenter", "schedule:daily", "time:morning"],
    doc_md = dedent("""
        # 네이버 스마트스토어 매출 ETL 파이프라인

        > 안내) 26-02-27 이후 '브랜드 애널리틱스 > 스토어 트래픽' 메뉴 삭제로 이용 불가,
        > [공지사항](https://adcenter.shopping.naver.com/board/notice_detail.nhn?noticeSeq=311782)
        > 참고 (~ v0.6.8)

        ## 인증(Credentials)
        네이버 쇼핑파트너센터 로그인 쿠키가 필요하다.
        (브랜드스토어 권한이 필요하고, '브랜드 애널리틱스' 메뉴에 도달해야 한다.)

        ## 추출(Extract)
        실행 시점(data_interval_end)에서 1일 전을 기준으로
        판매처별/상품별 매출을 수집한다.
        (조회수 등 지표가 1 이상 측정된 경우만 조회 가능하다.)

        ## 변환(Transform)
        JSON 형식의 응답 본문을 파싱하여 DuckDB 테이블에 적재한다.
        정규화된 테이블 구성에 맞춰 매출 부분과 상품 정보 부분을 나눠서 적재한다.

        ## 적재(Load)
        각각의 테이블을 대응되는 BigQuery 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
        (원본 매출 데이터가 오전 중 천천히 업데이트되기 때문에 하루에 여러 번 수집 시도한다.)
    """).strip(),
) as dag:

    PATH = "smartstore.brand.naver_brand_sales"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read
        return read(PATH, credentials="expand", tables=True, sheets=True, service_account=True)


    FIRST_SCHEDULE = "09:10"
    LAST_2_DAYS = 2
    YESTERDAY = 1

    @task(task_id="etl_naver_brand_sales")
    def etl_naver_brand_sales(ti: TaskInstance, **kwargs) -> dict:
        from airflow_utils import get_execution_date
        delta = LAST_2_DAYS if get_execution_date(kwargs, fmt="HH:mm") == FIRST_SCHEDULE else YESTERDAY
        start_date = get_execution_date(kwargs, subdays=delta)
        end_date = get_execution_date(kwargs, subdays=YESTERDAY)
        return main(start_date=start_date, end_date=end_date, **ti.xcom_pull(task_ids="read_configs"))

    def main(
            cookies: str,
            mall_seq: list[int],
            start_date: str,
            end_date: str,
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.smartstore.hcenter import aggregated_sales
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = {"sales": "naver_sales", "product": "naver_product"}

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            aggregated_sales(
                cookies = cookies,
                mall_seq = mall_seq,
                start_date = start_date,
                end_date = end_date,
                connection = conn,
                how_to_run = "async",
                progress = False,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "mall_seq": len(mall_seq),
                        "start_date": start_date,
                        "end_date": end_date,
                    },
                    "counts": {
                        "sales": conn.count_table(sources["sales"]),
                        "product": conn.count_table(sources["product"]),
                    },
                    "status": {
                        "sales": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["sales"],
                            staging_table = tables["temp_sales"],
                            target_table = tables["sales"],
                            **merge["sales"],
                            where_clause = f"T.payment_date BETWEEN '{start_date}' AND '{end_date}'",
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


    read_configs() >> etl_naver_brand_sales()
