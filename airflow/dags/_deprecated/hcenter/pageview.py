from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "naver_brand_pageview",
    schedule = "0 10 * * *",
    start_date = pendulum.datetime(2025, 12, 13, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    tags = ["priority:low", "naver:pageview", "login:hcenter", "schedule:daily", "time:morning"],
    doc_md = dedent("""
        # 네이버 브랜드스토어 상품 체류시간 ETL 파이프라인

        > 안내) 26-02-27 이후 '브랜드 애널리틱스 > 스토어 트래픽' 메뉴 삭제로 이용 불가,
        > [공지사항](https://adcenter.shopping.naver.com/board/notice_detail.nhn?noticeSeq=311782)
        > 참고 (~ v0.6.8)

        ## 인증(Credentials)
        네이버 쇼핑파트너센터 로그인 쿠키가 필요하다.
        (브랜드스토어 권한이 필요하고, '브랜드 애널리틱스' 메뉴에 도달해야 한다.)

        ## 추출(Extract)
        실행 시점(data_interval_end)에서 1일 전을 기준으로
        스토어 메인페이지와 상품 상세페이지에 대한 일별 조회수, 체류시간 등 지표를 수집한다.
        (브랜드스토어만 대상으로 조회할 수 있다.)

        ## 변환(Transform)
        JSON 형식의 응답 본문을 파싱하여 스토어/상품별 DuckDB 테이블에 적재한다.

        ## 적재(Load)
        각각의 데이터를 대응되는 BigQuery 테이블 끝에 추가한다.
    """).strip(),
) as dag:

    PATH = "smartstore.hcenter.pageview"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, credentials="expand", tables=True, sheets=True, service_account=True)


    @task(task_id="etl_naver_pageview_by_device")
    def etl_naver_pageview_by_device(ti: TaskInstance, **kwargs) -> dict:
        from airflow_utils import get_execution_date
        date = get_execution_date(kwargs, subdays=1)
        return main(aggregate_by="device", date=date, **ti.xcom_pull(task_ids="read_configs"))

    @task(task_id="etl_naver_pageview_by_product")
    def etl_naver_pageview_by_product(ti: TaskInstance, **kwargs) -> dict:
        from airflow_utils import get_execution_date
        date = get_execution_date(kwargs, subdays=1)
        return main(aggregate_by="product", date=date, **ti.xcom_pull(task_ids="read_configs"))

    def main(
            cookies: str,
            aggregate_by: str,
            mall_seq: list[int],
            date: str,
            service_account: dict,
            tables: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.smartstore.hcenter import page_view
        from linkmerce.extensions.bigquery import BigQueryClient
        source = f"naver_pv_by_{aggregate_by}"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            page_view(
                cookies = cookies,
                aggregate_by = aggregate_by,
                mall_seq = mall_seq,
                start_date = date,
                end_date = date,
                connection = conn,
                how_to_run = "async",
                progress = False,
                return_type = "none",
            )

            id_column = {"device": "device_type", "product": "product_id"}[aggregate_by]
            conn.execute(f"ALTER TABLE {source} RENAME COLUMN {id_column} TO page_id")

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "aggregate_by": aggregate_by,
                        "mall_seq": len(mall_seq),
                        "date": date,
                    },
                    "counts": {
                        aggregate_by: conn.count_table(source),
                    },
                    "status": {
                        aggregate_by: client.load_table_from_duckdb(
                            connection = conn,
                            source_table = source,
                            target_table = tables[aggregate_by],
                            progress = False,
                        ),
                    },
                }


    read_configs() >> etl_naver_pageview_by_device() >> etl_naver_pageview_by_product()
