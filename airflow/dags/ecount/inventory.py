from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "ecount_inventory",
    schedule = "0 9,17 * * *",
    start_date = pendulum.datetime(2026, 5, 27, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    tags = ["priority:high", "ecount:inventory", "api:ecount", "schedule:daily", "time:morning", "time:afternoon"],
    doc_md = dedent("""
        # 이카운트 창고별/품목별 재고현황 ETL 파이프라인

        ## 인증(Credentials)
        이카운트 API 인증 키(회사코드, 사용자ID, API 키)가 필요하다.

        ## 추출(Extract)
        매일 오전/오후 재고 업데이트 시간에 맞춰 이카운트 API로 재고현황 데이터를 수집한다.

        ## 변환(Transform)
        JSON 형식의 응답 본문을 파싱하여 DuckDB 테이블에 적재한다.

        ## 적재(Load)
        데이터를 BigQuery 테이블의 끝에 추가한다.
    """).strip(),
) as dag:

    PATH = "ecount.api.inventory"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, credentials="expand", tables=True, service_account=True)


    @task(task_id="etl_ecount_inventory")
    def etl_ecount_inventory(ti: TaskInstance, **kwargs) -> dict:
        from airflow_utils import format_datetime
        return main(base_date=format_datetime(kwargs), **ti.xcom_pull(task_ids="read_configs"))

    def main(
            com_code: int | str,
            userid: str,
            api_key: str,
            base_date: str,
            service_account: dict,
            tables: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.ecount.api import inventory
        from linkmerce.extensions.bigquery import BigQueryClient
        source = "ecount_inventory"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            inventory(
                com_code = com_code,
                userid = userid,
                api_key = api_key,
                base_date = base_date,
                zero_yn = True,
                connection = conn,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "com_code": com_code,
                        "base_date": base_date,
                        "zero_yn": True,
                    },
                    "counts": {
                        "table": conn.count_table(source),
                    },
                    "status": {
                        "table": client.load_table_from_duckdb(
                            connection = conn,
                            source_table = source,
                            target_table = tables["table"],
                        )
                    },
                }


    read_configs() >> etl_ecount_inventory()
