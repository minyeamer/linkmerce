from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "cj_eflexs_stock",
    schedule = "0 9,17 * * *",
    start_date = pendulum.datetime(2026, 5, 27, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    tags = [
        "priority:high", "eflexs:stock", "login:cj-eflexs", "schedule:daily",
        "time:morning", "time:afternoon", "playwright:true"
    ],
    doc_md = dedent("""
        # CJ대한통운 eFLEXs 상세재고조회 ETL 파이프라인

        ## 인증(Credentials)
        CJ대한통운 eFLEXs 로그인을 위한 아이디, 비밀번호와
        2단계 인증을 위한 이메일 계정 로그인 정보가 필요하다.
        (2단계 인증 메일 수신에 2분 정도 소요된다.)

        ## 추출(Extract)
        매일 오전/오후 재고 업데이트 시간에 맞춰
        실행 시점(data_interval_end)을 기준으로 7일전부터 실행 시점까지의 데이터를 조회한다.

        ## 변환(Transform)
        JSON 형식의 응답 본문을 파싱하여 DuckDB 테이블에 적재한다.

        ## 적재(Load)
        데이터를 BigQuery 테이블의 끝에 추가한다.
    """).strip(),
) as dag:

    PATH = "cjlogistics.eflexs.stock"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, credentials="expand", tables=True, service_account=True)


    @task(task_id="etl_eflexs_stock")
    def etl_eflexs_stock(ti: TaskInstance, **kwargs) -> dict:
        from airflow_utils import format_datetime
        start_date, end_date = format_datetime(kwargs, subdays=7), format_datetime(kwargs)
        configs = ti.xcom_pull(task_ids="read_configs")
        return main_eflexs(start_date=start_date, end_date=end_date, **configs)

    def main_eflexs(
            userid: str,
            passwd: str,
            mail_info: dict,
            customer_id: list[int],
            start_date: str,
            end_date: str,
            service_account: dict,
            tables: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.cj.eflexs import stock
        from linkmerce.extensions.bigquery import BigQueryClient
        source = "eflexs_stock"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            stock(
                userid = userid,
                passwd = passwd,
                mail_info = mail_info,
                customer_id = customer_id,
                start_date = start_date,
                end_date = end_date,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "customer_id": customer_id,
                        "start_date": start_date,
                        "end_date": end_date,
                    },
                    "counts": {
                        "table": conn.count_table(source),
                    },
                    "status": {
                        "table": client.load_table_from_duckdb(
                            connection = conn,
                            source_table = source,
                            target_table = tables["table"],
                        ),
                    },
                }


    read_configs() >> etl_eflexs_stock()
