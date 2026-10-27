"""
# ESM PLUS 상품목록 ETL 파이프라인

## 인증(Credentials)
1. ESM PLUS 로그인 정보(아이디, 비밀번호, 도메인) 목록을 읽는다.
2. 로그인 정보 내 선택적으로 존재하는 쿠키 파일의 유효성을 검사하여 유효하면 로그인을 스킵한다.
3. 쿠키가 없거나 유효하지 않다면 Playwright 브라우저를 통해 계정별로 순차 로그인하고 쿠키를 발급받는다.
4. 로그인 시 쿠키에 'ESM_REQUEST_AUTH_PC' 값이 있다면 로그인된 것으로 인식하고 지정된 파일에 덮어쓴다.
5. 모든 계정의 로그인이 끝나면 계정별 쿠키를 사용해 ETL Task를 동적으로 매핑한다.

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
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        from pw_actions import login_ebay_bulk
        credentials = read_config(PATH, credentials=True, skip_subpath=True)["credentials"]
        return login_ebay_bulk(credentials, where="esmplus")

    def validate_cookies(credentials: dict):
        from linkmerce.common.exceptions import AuthenticationError
        if not credentials.get("cookies"):
            raise AuthenticationError("ESM PLUS authentication cookies are missing")


    @task(task_id="etl_ebay_item", map_index_template="{{ credentials['userid'] }}")
    def etl_ebay_item(credentials: dict, configs: dict) -> dict:
        validate_cookies(credentials)
        return main(**credentials, **configs)

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


    (etl_ebay_item
    .partial(configs=read_configs())
    .expand(credentials=read_credentials()))
