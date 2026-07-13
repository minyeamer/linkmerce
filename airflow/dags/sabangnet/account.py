"""
# 사방넷 쇼핑몰 계정 ETL 파이프라인

## 인증(Credentials)
사방넷 아이디, 비밀번호와 시스템 도메인 번호가 필요하다.
Task를 실행할 때마다 로그인하고, 쿠키와 `access_token`을 발급받아 활용한다.

## 추출(Extract)
사방넷 시스템에서 국내, 해외, 일반 쇼핑몰 계정 목록을 순차적으로 수집한다.

## 변환(Transform)
JSON 형식의 응답 본문을 파싱하여 DuckDB 테이블에 적재한다.

## 적재(Load)
기존 구글시트에 최신 데이터를 덮어쓴다.
"""

from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "sabangnet_account",
    schedule = "20 22 * * 1-5",
    start_date = pendulum.datetime(2026, 7, 13, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:low", "platform:sabangnet", "objective:sales", "credentials:userid",
        "schedule:weekdays", "time:night", "write:overwrite"
    ],
) as dag:

    PATH = "sabangnet.admin.account"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, credentials="expand", sheets=False, service_account=True)


    @task(task_id="etl_sabangnet_account", pool="sabangnet_pool")
    def etl_sabangnet_account(ti: TaskInstance, **kwargs) -> dict:
        return main(**ti.xcom_pull(task_ids="read_configs"))

    def main(
            userid: str,
            passwd: str,
            domain: int,
            service_account: dict,
            google_sheets: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.sabangnet.admin import account
        from linkmerce.extensions.gsheets import WorksheetClient
        source = "sabangnet_account"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            account(
                userid = userid,
                passwd = passwd,
                domain = domain,
                connection = conn,
                return_type = "none",
            )

            columns = ", ".join(google_sheets["columns"])
            records = conn.fetch_all_to_json(f"SELECT {columns} FROM {source}")

            client = WorksheetClient(service_account, google_sheets["key"], google_sheets["sheet"])
            return client.overwrite_worksheet(records)
            # return {
            #     "spreadsheetId": "key",
            #     "updatedRange": "sheet!A1:B1",
            #     "updatedRows": 0,
            #     "updatedColumns": 0,
            #     "updatedCells": 0,
            # }


    read_configs() >> etl_sabangnet_account()
