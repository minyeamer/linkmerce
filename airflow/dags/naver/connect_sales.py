"""
# 네이버 쇼핑 커넥트 상품별 판매 실적 ETL 파이프라인

## 인증(Credentials)
크롬 확장프로그램을 통해 Slack 채널 '_쇼핑커넥트-쿠키'에 업로드되는 로그인 쿠키를 추출한다.

## 추출(Extract)
실행 시점에서 1일 전을 기준일로 하여 스페이스 상품별 판매 실적과 상품 목록을 가져온다.

## 변환(Transform)
JSON 형식의 응답 본문을 파싱하여 판매 실적과 상품 목록을 각각의 DuckDB 테이블에 적재한다.

## 적재(Load)
- 판매 실적은 대응되는 BigQuery/Postgres 테이블 끝에 추가한다.
- 상품 목록은 기존 BigQuery/Postgres 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
- 적재 과정에서 수집한 판매 실적의 날짜 파티션 범위를 바탕으로 후속 dbt 모델을 실행한다.
"""

from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from airflow.providers.slack.hooks.slack import SlackHook
from cosmos import DbtTaskGroup
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "naver_connect_sales",
    schedule = "10 8 * * *",
    start_date = pendulum.datetime(2026, 8, 12, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:naver-connect", "objective:ads", "credentials:cookie",
        "schedule:daily", "time:morning", "write:append", "write:merge", "plugin:dbt",
        "provider:slack"
    ],
) as dag:

    PATH = "naver.brandconnect.sales_performances"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_credentials as read
        return read("naver.brandconnect", skip_subpath=True)


    @task(task_id="etl_naver_connect_sales", map_index_template="{{ credentials['userid'] }}")
    def etl_naver_connect_sales(credentials: dict, configs: dict, **kwargs) -> dict:
        from airflow_utils import format_datetime, get_datetime
        from linkmerce.utils.regex import regexp_extract

        cookies = get_naver_cookies_from_slack(
            slack_conn_id = configs["slack_conn_id"],
            channel_id = configs["channel_id"],
            userid = credentials["userid"],
            datetime = get_datetime(kwargs),
        )
        if (save_to := regexp_extract(r"Path\(([^)]+)\)", credentials["cookies"])):
            save_naver_cookies(cookies, save_to)

        return main(cookies=cookies, date=format_datetime(kwargs, subdays=1), **configs)

    def main(
            cookies: str,
            space_id: int | str | list[int | str],
            date: str,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from dual_load import load_table_from_duckdb, merge_table_from_duckdb
        from linkmerce.api.naver.brandconnect import sales_performances
        from linkmerce.common.load import DuckDBConnection
        sources = {"sales": "naver_connect_sales", "product": "naver_connect_product"}

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            sales_performances(
                cookies = cookies,
                space_id = space_id,
                start_date = date,
                end_date = date,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            return {
                "context": {
                    "partitions": sorted(map(str, conn.unique(sources["sales"], "order_date"))),
                },
                "params": {
                    "space_id": space_id,
                    "date": date,
                },
                "results": {
                    "sales": load_table_from_duckdb(
                        connection = conn,
                        source_table = sources["sales"],
                        target_table = tables["sales"],
                    ),
                    "product": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["product"],
                        target_table = tables["product"],
                        **merge["product"],
                    ),
                },
            }


    def fetch_slack_history(
            slack_hook: SlackHook,
            channel_id: str,
            datetime: pendulum.DateTime,
            limit: int = 30,
        ) -> list:
        """Slack 채널에서 전달된 날짜로부터 최근 3일간 전송된 메시지 목록을 가져온다."""
        day_start = datetime.start_of("day")
        oldest = str(day_start.subtract(days=2).timestamp())
        latest = str(day_start.add(days=1).timestamp())

        params = {"channel": channel_id, "oldest": oldest, "latest": latest, "limit": limit}
        response = slack_hook.client.conversations_history(**params)
        return response.get("messages") or list()


    def get_naver_cookies_from_slack(
            slack_conn_id: str,
            channel_id: str,
            userid: str,
            datetime: pendulum.DateTime,
            limit: int = 30,
        ) -> str:
        """Slack 채널에서 `{userid}.txt` 파일로 전송된 최신 네이버 쿠키를 가져온다."""
        from linkmerce.common.exceptions import AuthenticationError
        import requests

        slack_hook = SlackHook(slack_conn_id=slack_conn_id)
        messages: list[dict] = fetch_slack_history(slack_hook, channel_id, datetime, limit)
        token = slack_hook.get_conn().token

        for message in messages:
            for file in (message.get("files") or list()):
                if not isinstance(file, dict):
                    continue
                url = file.get("url_private_download") or file.get("url_private")
                file_name = file.get("name") or file.get("title") or str()

                if url and (file_name == f"{userid}.txt"):
                    response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
                    response.raise_for_status()
                    return response.text.strip()

        raise AuthenticationError(f"No message found containing the filename {userid}.txt.")


    def save_naver_cookies(cookies: str, save_to: str | None = None, mkdir: bool = True):
        """네이버 쿠키를 지정된 경로에 저장한다."""
        from pathlib import Path
        file_path = save_to if isinstance(save_to, Path) else Path(save_to)
        if mkdir:
            file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, 'w', encoding="utf-8") as file:
            file.write(cookies)
        return cookies


    @task(task_id="generate_dbt_date_range", trigger_rule="all_done")
    def generate_dbt_date_range(results: list[dict]) -> dict:
        from dbt_cosmos import generate_dbt_date_range as generate
        return generate(results, "context.partitions")


    @task.short_circuit(task_id="prepare_dbt_run", ignore_downstream_trigger_rules=False)
    def prepare_dbt_run(ti: TaskInstance, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_dbt_date_range")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False


    def dbt_bigquery_naver_connect_sales_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_naver_connect_sales",
            selector = "naver_connect_sales",
            ds_task_id = "generate_dbt_date_range",
        )

    def dbt_postgres_naver_connect_sales_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_postgres
        return dynamic_mapping_dbt_postgres(
            group_id = "dbt_postgres_naver_connect_sales",
            selector = "naver_connect_sales",
            ds_task_id = "generate_dbt_date_range",
        )

    @task(task_id="finalize_dag_run", trigger_rule="all_done")
    def finalize_dag_run(ti: TaskInstance):
        from dbt_cosmos import raise_on_failure
        raise_on_failure(ti)


    etl_results = (etl_naver_connect_sales
        .partial(configs=read_configs())
        .expand(credentials=read_credentials()))

    dbt_date_range = generate_dbt_date_range(etl_results)
    dbt_run = [dbt_bigquery_naver_connect_sales_group(), dbt_postgres_naver_connect_sales_group()]

    dbt_date_range >> prepare_dbt_run() >> dbt_run >> finalize_dag_run()
