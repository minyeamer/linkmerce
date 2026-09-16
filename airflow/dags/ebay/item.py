"""
# ESM PLUS 상품목록 ETL 파이프라인

## 인증(Credentials)
1. ESM PLUS 로그인 정보(아이디, 비밀번호, 도메인)를 읽는다.
2. Playwright 브라우저를 통해 로그인하고 쿠키를 발급받는다.
3. 쿠키에 'ESM_REQUEST_AUTH_PC' 값이 있다면 로그인된 것으로 인식하고 지정된 파일에 덮어쓴다.

## 추출(Extract)
ESM PLUS 마스터 계정에 등록된 모든 G마켓/옥션 상품목록을 수집한다.

## 변환(Transform)
JSON 형식의 응답 본문을 파싱하여 DuckDB 테이블에 적재한다.

## 적재(Load)
기존 BigQuery/Postgres 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
"""

from airflow.sdk import DAG, task
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "ebay_item",
    schedule = "10 23 * * *",
    start_date = pendulum.datetime(2026, 9, 15, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:medium", "platform:esmplus", "objective:product", "credentials:userid",
        "schedule:daily", "time:night", "write:merge", "plugin:playwright"
    ],
) as dag:

    PATH = "ebay.esmplus.item"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True, tables=True, skip_subpath=True)


    @task(task_id="login_esmplus", retries=2, retry_delay=timedelta(minutes=1))
    def login_esmplus(configs: dict) -> str:
        from pw_actions import login_ebay
        from linkmerce.utils.regex import regexp_extract

        credentials: dict = configs["credentials"]
        cookies = login_ebay(
            userid = credentials["userid"],
            passwd = credentials["passwd"],
            site_type = credentials.get("site_type", "esmplus"),
            where = "esmplus",
        )

        if (save_to := regexp_extract(r"Path\(([^)]+)\)", credentials.get("cookies", str()))):
            from pw_actions import save_browser_cookies
            save_browser_cookies(cookies, save_to)

        return cookies


    @task(task_id="etl_ebay_item")
    def etl_ebay_item(configs: dict, cookies: str) -> dict:
        return main(**(configs | {"cookies": cookies}))

    def main(
            cookies: str,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs,
        ) -> dict:
        from dual_load import merge_table_from_duckdb
        from linkmerce.api.ebay.esmplus import item
        from linkmerce.common.load import DuckDBConnection
        source = "esmplus_item"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            item(
                cookies = cookies,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            return {
                "result": merge_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables["table"],
                    **merge["table"],
                )
            }


    configs = read_configs()
    cookies = login_esmplus(configs)
    etl_result = etl_ebay_item(configs, cookies)
