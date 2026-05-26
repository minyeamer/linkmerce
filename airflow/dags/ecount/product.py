from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "ecount_product",
    schedule = "50 8,16 * * *",
    start_date = pendulum.datetime(2026, 5, 26, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    tags = ["priority:high", "ecount:product", "api:ecount", "schedule:daily", "time:morning", "time:afternoon"],
    doc_md = dedent("""
        # 이카운트 품목 리스트 ETL 파이프라인

        ## 인증(Credentials)
        이카운트 API 인증 키(회사코드, 사용자ID, API 키)가 필요하다.

        ## 추출(Extract)
        매일 오전/오후 재고 업데이트 시간에 맞춰 이카운트 API로 품목등록 리스트를 수집한다.

        ## 변환(Transform)
        JSON 형식의 응답 본문을 파싱하여 DuckDB 테이블에 적재한다.

        ## 적재(Load)
        기존 BigQuery 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
    """).strip(),
) as dag:

    PATH = "ecount.api.product"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, credentials="expand", tables=True, service_account=True)


    @task(task_id="etl_ecount_product")
    def etl_ecount_product(ti: TaskInstance, **kwargs) -> dict:
        from airflow_utils import get_execution_date
        return main(base_date=get_execution_date(kwargs), **ti.xcom_pull(task_ids="read_configs"))

    def main(
            com_code: int | str,
            userid: str,
            api_key: str,
            service_account: dict,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.ecount.api import product
        from linkmerce.extensions.bigquery import BigQueryClient
        source = "ecount_product"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            product(
                com_code = com_code,
                userid = userid,
                api_key = api_key,
                comma_yn = True,
                connection = conn,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "com_code": com_code,
                        "comma_yn": True,
                    },
                    "counts": {
                        "table": conn.count_table(source),
                    },
                    "status": {
                        "table": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = source,
                            target_table = tables["table"],
                            **merge["table"],
                        )
                    },
                }


    read_configs() >> etl_ecount_product()
