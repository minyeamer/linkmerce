"""
# AUCTION 광고센터 ETL 파이프라인

## 인증(Credentials)
1. AUCTION 광고센터 로그인 정보(아이디, 비밀번호, 도메인) 목록을 읽는다.
2. 로그인 정보 내 선택적으로 존재하는 쿠키 파일의 유효성을 검사하여 유효하면 로그인을 스킵한다.
3. 쿠키가 없거나 유효하지 않다면 Playwright 브라우저를 통해 계정별로 순차 로그인하고 쿠키를 발급받는다.
4. 로그인 시 쿠키에 'AD_AUTH' 값이 있다면 로그인된 것으로 인식하고 지정된 파일에 덮어쓴다.
5. 모든 계정의 로그인이 끝나면 계정별 쿠키를 사용해 ETL Task를 동적으로 매핑한다.

## 추출(Extract)
실행 시점(data_interval_end)에서 1일 전을 기준으로 AI매출업 및 파워클릭 상품별 리포트를 가져온다.

## 변환(Transform)
JSON 형식의 응답 본문을 파싱하여 DuckDB 테이블에 적재한다.

## 적재(Load)
- 리포트 유형별로 대응되는 BigQuery/Postgres 테이블의 끝에 데이터를 추가한다.
- 적재 과정에서 수집한 날짜 파티션 범위를 바탕으로 후속 dbt 모델을 실행한다.
"""

from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from cosmos import DbtTaskGroup
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "ebay_auction_ads",
    schedule = "20 8 * * *",
    start_date = pendulum.datetime(2026, 9, 15, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:auction-ad", "objective:ads", "credentials:userid",
        "schedule:daily", "time:morning", "write:append", "plugin:playwright", "plugin:dbt"
    ],
) as dag:

    PATH = "ebay.ad.ads"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        from pw_actions import login_ebay_bulk
        credentials = read_config(PATH, credentials=True, skip_subpath=True)["credentials"]
        return login_ebay_bulk(credentials, where="ad")

    def validate_cookies(credentials: dict):
        from linkmerce.common.exceptions import AuthenticationError
        if not credentials.get("cookies"):
            raise AuthenticationError("AUCTION Adcenter authentication cookies are missing")


    @task(task_id="etl_auction_report_ai", map_index_template="{{ credentials['userid'] }}")
    def etl_auction_report_ai(credentials: dict, configs: dict, **kwargs) -> dict:
        from airflow_utils import format_datetime
        validate_cookies(credentials)
        return main(**credentials, **configs,
            report_type = "ai",
            date = format_datetime(kwargs, subdays=1),
        )

    @task(task_id="etl_auction_report_cpc", map_index_template="{{ credentials['userid'] }}")
    def etl_auction_report_cpc(credentials: dict, configs: dict, **kwargs) -> dict:
        from airflow_utils import format_datetime
        validate_cookies(credentials)
        return main(**credentials, **configs,
            report_type = "cpc",
            date = format_datetime(kwargs, subdays=1),
        )

    def main(
            cookies: str,
            report_type: str,
            master_id: int | str,
            date: str,
            tables: dict[str, str],
            **kwargs,
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from importlib import import_module
        from dual_load import load_table_from_duckdb
        extract = getattr(import_module("linkmerce.api.ebay.ad"), f"{report_type}_report")
        source = f"auction_adreport_{report_type}"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            extract(
                cookies = cookies,
                master_id = master_id,
                start_date = date,
                end_date = date,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            return {
                "context": {
                    "partitions": sorted(map(str, conn.unique(source, "ymd"))),
                },
                "params": {
                    "date": date,
                    "master_id": master_id,
                },
                "result": load_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables[f"report_{report_type}"],
                )
            }


    @task(task_id="generate_dbt_date_range")
    def generate_dbt_date_range(ai_results: list[dict], cpc_results: list[dict]) -> dict:
        from dbt_cosmos import generate_dbt_date_range as generate
        return generate(ai_results + cpc_results, "context.partitions")


    @task.short_circuit(task_id="prepare_dbt_run")
    def prepare_dbt_run(ti: TaskInstance, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_dbt_date_range")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False


    def dbt_bigquery_ebay_auction_ads_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_ebay_auction_ads",
            selector = "ebay_auction_ads",
            ds_task_id = "generate_dbt_date_range",
        )

    def dbt_postgres_ebay_auction_ads_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_postgres
        return dynamic_mapping_dbt_postgres(
            group_id = "dbt_postgres_ebay_auction_ads",
            selector = "ebay_auction_ads",
            ds_task_id = "generate_dbt_date_range",
        )


    configs = read_configs()
    credentials = read_credentials()

    etl_ai_results = (etl_auction_report_ai
        .partial(configs=configs)
        .expand(credentials=credentials))

    etl_cpc_results = (etl_auction_report_cpc
        .partial(configs=configs)
        .expand(credentials=credentials))

    dbt_date_range = generate_dbt_date_range(etl_ai_results, etl_cpc_results)
    dbt_run = [dbt_bigquery_ebay_auction_ads_group(), dbt_postgres_ebay_auction_ads_group()]

    dbt_date_range >> prepare_dbt_run() >> dbt_run
