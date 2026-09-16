"""
# Gmarket 광고센터 ETL 파이프라인

## 인증(Credentials)
1. Gmarket 광고센터 로그인 정보(아이디, 비밀번호, 도메인)를 읽는다.
2. Playwright 브라우저를 통해 로그인하고 쿠키를 발급받는다.
3. 쿠키에 'ADC_AUTH' 값이 있다면 로그인된 것으로 인식하고 지정된 파일에 덮어쓴다.

## 추출(Extract)
실행 시점(data_interval_end)에서 1일 전을 기준으로 캠페인 목록과 상품별 상세 리포트를 가져온다.

## 변환(Transform)
API 응답을 파싱하여 DuckDB 테이블에 적재한다.

## 적재(Load)
- 캠페인 그룹, 캠페인 테이블은 기존 BigQuery/Postgres 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
- 상품별 상세 리포트 테이블은 대응되는 BigQuery/Postgres 테이블 끝에 추가한다.
- 적재 과정에서 수집한 상세 리포트의 날짜 파티션 범위를 바탕으로 후속 dbt 모델을 실행한다.
"""

from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from cosmos import DbtTaskGroup
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "ebay_gmarket_ads",
    schedule = "40 8 * * *",
    start_date = pendulum.datetime(2026, 9, 15, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    max_active_runs = 1,
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:gmarket-adc", "objective:ads", "credentials:userid",
        "schedule:daily", "time:morning", "write:append", "write:merge",
        "plugin:playwright", "plugin:dbt"
    ],
) as dag:

    PATH = "ebay.adcenter.ads"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True, tables=True, skip_subpath=True)


    @task(task_id="login_gmarket_adcenter", retries=2, retry_delay=timedelta(minutes=1))
    def login_gmarket_adcenter(configs: dict) -> str:
        from pw_actions import login_ebay
        from linkmerce.utils.regex import regexp_extract

        credentials = configs["credentials"]
        cookies = login_ebay(
            userid = credentials["userid"],
            passwd = credentials["passwd"],
            site_type = credentials["site_type"],
            where = "adcenter",
        )

        if (save_to := regexp_extract(r"Path\(([^)]+)\)", credentials.get("cookies", str()))):
            from pw_actions import save_browser_cookies
            save_browser_cookies(cookies, save_to)

        return cookies


    @task(task_id="etl_gmarket_campaign")
    def etl_gmarket_campaign(configs: dict, cookies: str, **kwargs) -> dict:
        from airflow_utils import format_datetime
        return main_campaign(
            date = format_datetime(kwargs, subdays=1),
            **(configs | {"cookies": cookies})
        )

    def main_campaign(
            cookies: str,
            date: str,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs,
        ) -> dict:
        from dual_load import merge_table_from_duckdb
        from linkmerce.api.ebay.adcenter import campaign_group, campaign
        from linkmerce.common.load import DuckDBConnection
        sources = {
            "campaign_group": "gmarket_campaign_group",
            "campaign": "gmarket_campaign",
        }

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            campaign_group(
                cookies = cookies,
                start_date = date,
                end_date = date,
                connection = conn,
                return_type = "none",
            )

            group_ids = conn.unique(sources["campaign_group"], "campaign_group_id")

            campaign(
                cookies = cookies,
                campaign_group_id = group_ids,
                start_date = date,
                end_date = date,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            return {
                "params": {
                    "date": date,
                },
                "results": {
                    "campaign_group": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["campaign_group"],
                        target_table = tables["campaign_group"],
                        **merge["campaign_group"],
                    ),
                    "campaign": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["campaign"],
                        target_table = tables["campaign"],
                        **merge["campaign"],
                    )
                }
            }


    @task(task_id="etl_gmarket_adreport")
    def etl_gmarket_adreport(configs: dict, cookies: str, **kwargs) -> dict:
        from airflow_utils import format_datetime
        return main_adreport(
            date = format_datetime(kwargs, subdays=1),
            **(configs | {"cookies": cookies})
        )

    def main_adreport(
            cookies: str,
            date: str,
            tables: dict[str, str],
            **kwargs,
        ) -> dict:
        from dual_load import load_table_from_duckdb
        from linkmerce.api.ebay.adcenter import report
        from linkmerce.common.load import DuckDBConnection
        source = "gmarket_adreport"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            report(
                cookies = cookies,
                start_date = date,
                end_date = date,
                report_type = "product",
                aggregate_type = "daily",
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
                    "report_type": "product",
                    "aggregate_type": "daily",
                },
                "results": load_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables["report"],
                )
            }


    @task(task_id="generate_dbt_date_range")
    def generate_dbt_date_range(result: dict) -> dict:
        from dbt_cosmos import generate_dbt_date_range as generate
        return generate(result, "context.partitions")


    @task.short_circuit(task_id="prepare_dbt_run")
    def prepare_dbt_run(ti: TaskInstance, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_dbt_date_range")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False


    def dbt_bigquery_ebay_gmarket_ads_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_ebay_gmarket_ads",
            selector = "ebay_gmarket_ads",
            ds_task_id = "generate_dbt_date_range",
        )

    def dbt_postgres_ebay_gmarket_ads_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_postgres
        return dynamic_mapping_dbt_postgres(
            group_id = "dbt_postgres_ebay_gmarket_ads",
            selector = "ebay_gmarket_ads",
            ds_task_id = "generate_dbt_date_range",
        )


    configs = read_configs()
    cookies = login_gmarket_adcenter(configs)

    etl_campaign_result = etl_gmarket_campaign(configs, cookies)
    etl_adreport_result = etl_gmarket_adreport(configs, cookies)
    etl_campaign_result >> etl_adreport_result

    dbt_date_range = generate_dbt_date_range(etl_adreport_result)
    dbt_run = [dbt_bigquery_ebay_gmarket_ads_group(), dbt_postgres_ebay_gmarket_ads_group()]

    dbt_date_range >> prepare_dbt_run() >> dbt_run
