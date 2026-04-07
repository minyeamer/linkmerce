from airflow.sdk import DAG, task
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "searchad_master_gfa",
    schedule = "30 5 * * 1-5",
    start_date = pendulum.datetime(2025, 8, 30, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    tags = ["priority:medium", "searchad:master", "login:gfa", "schedule:weekdays", "time:morning"],
    doc_md = dedent("""
        # 네이버 성과형 디스플레이 광고 소재 ETL 파이프라인

        ## 인증(Credentials)
        성과형 디스플레이 광고 계정을 보유한 네이버 계정의 로그인 쿠키가 필요하다.

        ## 추출(Extract)
        계정별 캠페인, 광고그룹, 소재 목록을 수집한다.

        ## 변환(Transform)
        JSON 형식의 응답 본문을 파싱하여 DuckDB 테이블에 적재한다.

        ## 적재(Load)
        각각의 데이터를 대응되는 BigQuery 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
    """).strip(),
) as dag:

    PATH = "searchad.gfa.master"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read
        return read(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read
        return read(PATH, credentials=True)["credentials"]


    @task(task_id="etl_searchad_master_gfa", map_index_template="{{ credentials['account_no'] }}")
    def etl_searchad_master_gfa(credentials: dict, configs: dict, **kwargs) -> dict:
        types = ["campaign", "adset", "creative"]
        return {api_type: main(api_type, **credentials, **configs) for api_type in types}

    def main(
            api_type: str,
            account_no: int | str,
            cookies: str,
            service_account: dict,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.extensions.bigquery import BigQueryClient
        from importlib import import_module
        import logging
        extract = getattr(import_module("linkmerce.api.searchad.gfa"), api_type)
        status = [("RUNNABLE" if api_type == "campaign" else "ALL"), "DELETED"]
        source = f"searchad_{api_type}_gfa"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            extract(
                account_no = account_no,
                cookies = cookies,
                status = status,
                connection = conn,
                progress = False,
                return_type = "none",
            )
            logging.info(f"[{account_no}] request completed for downloading the {api_type} list")

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "account_no": account_no,
                        "status": status,
                    },
                    "counts": {
                        "table": conn.count_table(source),
                    },
                    "status": {
                        "table": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = source,
                            staging_table = f'{tables[f"temp_{api_type}"]}_{account_no}',
                            target_table = tables[api_type],
                            **merge[api_type],
                            progress = False,
                        ),
                    },
                }


    (etl_searchad_master_gfa
    .partial(configs=read_configs())
    .expand(credentials=read_credentials()))
