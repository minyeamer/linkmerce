"""
# 쿠팡 광고 ETL 파이프라인

## 인증(Credentials)
크롬 확장프로그램을 통해 Slack 채널 '_쿠팡-광고-쿠키'에 업로드되는 로그인 쿠키를 추출한다.

## 추출(Extract)
업체별 캠페인 및 광고그룹을 수집하고,
추가로 실행 시점(data_interval_end)에서 1일 전을 기준으로 매출 성장 광고 보고서를 다운로드한다.

## 변환(Transform)
- JSON 형식의 캠페인 응답 본문을 파싱하여 캠페인과 광고그룹을 각각의 DuckDB 테이블에 적재한다.
- 엑셀 바이너리 형식의 광고 보고서를 JSON 형식으로 변환하고 DuckDB 테이블에 적재한다.

## 적재(Load)
- 각각의 캠페인과 광고그룹을 기존 BigQuery/Postgres 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
- 광고 보고서 테이블은 대응되는 BigQuery/Postgres 테이블의 끝에 추가한다.
- 적재 과정에서 수집한 광고 보고서의 날짜 파티션 범위를 바탕으로 후속 dbt 모델을 실행한다.
"""

from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from cosmos import DbtTaskGroup
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "coupang_ads",
    schedule = "5 9 * * *",
    start_date = pendulum.datetime(2026, 11, 24, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:coupang-ads", "objective:ads", "credentials:cookies",
        "schedule:daily", "time:morning", "write:append", "write:merge", "plugin:dbt",
        "provider:slack", "upstream:extension"
    ],
) as dag:

    PATH = "coupang.advertising.ads"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config, get_slack_file_text
        config = read_config(PATH, credentials=True)
        today = pendulum.today("Asia/Seoul")

        credentials = list()
        for vendor in config["credentials"]:
            if not (isinstance(vendor, dict) and vendor.get("ads", True)):
                continue

            vendor_id = vendor["vendor_id"]
            cookies = get_slack_file_text(
                slack_conn_id = config["slack_conn_id"],
                channel_id = config["channel_id"],
                file_name = f"{vendor_id}.txt",
                start_datetime = today,
                end_datetime = today.add(days=1),
            )
            credentials.append({"vendor_id": vendor_id, "cookies": cookies})
        return credentials


    @task(task_id="etl_coupang_campaign", map_index_template="{{ credentials['vendor_id'] }}")
    def etl_coupang_campaign(credentials: dict, configs: dict, **kwargs) -> dict:
        return main_campaign(**credentials, **configs)

    def main_campaign(
            cookies: str,
            vendor_id: str,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.advertising import campaign
        from dual_load import merge_table_from_duckdb
        sources = {"campaign": "coupang_campaign", "adgroup": "coupang_adgroup"}

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            for is_deleted in [False, True]:
                campaign(
                    cookies = cookies,
                    vendor_id = vendor_id,
                    goal_type = "SALES",
                    is_deleted = is_deleted,
                    connection = conn,
                    progress = False,
                    return_type = "none",
                )

            return {
                "params": {
                    "vendor_id": vendor_id,
                    "goal_type": "SALES",
                    "is_deleted": [False, True],
                },
                "results": {
                    "campaign": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["campaign"],
                        target_table = tables["campaign"],
                        **merge["campaign"],
                    ),
                    "adgroup": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["adgroup"],
                        target_table = tables["adgroup"],
                        **merge["adgroup"],
                    ),
                }
            }


    @task(task_id="etl_coupang_adreport", map_index_template="{{ credentials['vendor_id'] }}")
    def etl_coupang_adreport(credentials: dict, configs: dict, **kwargs) -> dict:
        from airflow_utils import format_datetime
        date = format_datetime(kwargs, subdays=1)
        return main_adreport_pa(**credentials, date=date, **configs)

    def main_adreport_pa(
            cookies: str,
            vendor_id: str,
            date: str,
            tables: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.advertising import report_pa
        from dual_load import load_table_from_duckdb
        source = "coupang_adreport_pa"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            report_pa(
                cookies = cookies,
                vendor_id = vendor_id,
                start_date = date,
                end_date = date,
                date_type = "daily",
                report_level = "vendorItem",
                connection = conn,
                return_type = "none",
            )

            return {
                "context": {
                    "partitions": sorted(map(str, conn.unique(source, "ymd"))),
                },
                "params": {
                    "vendor_id": vendor_id,
                    "date": date,
                    "date_type": "daily",
                    "report_level": "vendorItem",
                },
                "result": load_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables["report"],
                )
            }


    @task(task_id="generate_dbt_date_range")
    def generate_dbt_date_range(results: list[dict]) -> dict:
        from dbt_cosmos import generate_dbt_date_range as generate
        return generate(results, "context.partitions")


    @task.short_circuit(task_id="prepare_dbt_run", ignore_downstream_trigger_rules=False)
    def prepare_dbt_run(ti: TaskInstance, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_dbt_date_range")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False


    def dbt_bigquery_coupang_ads_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_coupang_ads",
            selector = "coupang_ads",
            ds_task_id = "generate_dbt_date_range",
        )

    def dbt_postgres_coupang_ads_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_postgres
        return dynamic_mapping_dbt_postgres(
            group_id = "dbt_postgres_coupang_ads",
            selector = "coupang_ads",
            ds_task_id = "generate_dbt_date_range",
        )


    @task(task_id="finalize_dag_run", trigger_rule="all_done")
    def finalize_dag_run(ti: TaskInstance):
        from dbt_cosmos import raise_on_failure
        raise_on_failure(ti)


    configs = read_configs()
    credentials = read_credentials()

    etl_campaign_results = (etl_coupang_campaign
        .partial(configs=configs)
        .expand(credentials=credentials))

    etl_adreport_results = (etl_coupang_adreport
        .partial(configs=configs)
        .expand(credentials=credentials))

    etl_campaign_results >> etl_adreport_results

    dbt_date_range = generate_dbt_date_range(etl_adreport_results)
    dbt_run = [dbt_bigquery_coupang_ads_group(), dbt_postgres_coupang_ads_group()]

    dbt_date_range >> prepare_dbt_run() >> dbt_run >> finalize_dag_run()
