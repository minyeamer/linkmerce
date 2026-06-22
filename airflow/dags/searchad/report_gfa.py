"""
# 네이버 성과형 디스플레이 광고 성과 보고서 ETL 파이프라인

## 인증(Credentials)
성과형 디스플레이 광고 계정을 보유한 네이버 계정의 로그인 쿠키가 필요하다.

## 추출(Extract)
실행 시점(data_interval_end)에서 1일 전을 기준으로
계정별 캠페인/소재 성과 보고서를 다운로드한다.

## 변환(Transform)
엑셀 바이너리 형식의 보고서를 JSON 형식으로 변환하고 보고서 유형별 DuckDB 테이블에 적재한다.
소재 성과 보고서에서 누락된 캠페인 성과 보고서를 INSERT 문으로 추가한다.

## 적재(Load)
- 보고서 데이터를 BigQuery/Postgres 테이블 끝에 추가한다.
- 적재 과정에서 수집한 광고 성과일 파티션 범위를 바탕으로 후속 dbt 모델을 실행한다.
"""

from airflow.sdk import DAG, task
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.models.taskinstance import TaskInstance
from cosmos import DbtTaskGroup
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "searchad_report_gfa",
    schedule = "20 5 * * *",
    start_date = pendulum.datetime(2025, 8, 24, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:searchad", "objective:ads", "credentials:cookies",
        "schedule:daily", "time:morning", "write:append", "plugin:dbt"
    ],
) as dag:

    PATH = "searchad.gfa.report"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    @task(task_id="etl_searchad_report_gfa", map_index_template="{{ credentials['account_no'] }}")
    def etl_searchad_report_gfa(credentials: dict, configs: dict, **kwargs) -> dict:
        from airflow_utils import format_datetime
        return main(**credentials, date=format_datetime(kwargs, subdays=1), **configs)

    def main(
            account_no: int | str,
            cookies: str,
            date: str,
            tables: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.searchad.gfa import campaign_report, creative_report
        from dual_load import load_table_from_duckdb
        sources = {
            "campaign": "searchad_campaign_report",
            "creative": "searchad_creative_report",
            "merged": "searchad_report_gfa",
        }

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            campaign_report(
                account_no = account_no,
                cookies = cookies,
                start_date = date,
                end_date = date,
                date_type = "DAY",
                connection = conn,
                progress = False,
                return_type = "none",
            )

            creative_report(
                account_no = account_no,
                cookies = cookies,
                start_date = date,
                end_date = date,
                date_type = "DAY",
                connection = conn,
                progress = False,
                return_type = "none",
            )

            conn.copy_table(sources["creative"], sources["merged"])
            conn.execute(merge_into_query(sources["campaign"], sources["merged"]))

            return {
                "context": {
                    "partitions": sorted(map(str, conn.unique(sources["merged"], "ymd"))),
                },
                "params": {
                    "account_no": account_no,
                    "date": date,
                    "date_type": "DAY",
                },
                "result": load_table_from_duckdb(
                    connection = conn,
                    source_table = sources["merged"],
                    target_table = tables["table"],
                )
            }

    def merge_into_query(source_table: str, target_table: str) -> str:
        return dedent(f"""
            INSERT INTO {target_table}
            SELECT
                campaign_no
                , 0 AS adset_no
                , 0 AS creative_no
                , account_no
                , impression_count
                , click_count
                , reach_count
                , ad_cost
                , conv_count
                , conv_amount
                , ymd
            FROM {source_table} AS S
            WHERE NOT EXISTS (
                SELECT 1 FROM {target_table} AS T
                WHERE (T.account_no = S.account_no) AND (T.campaign_no = S.campaign_no)
            )""").strip()


    @task(task_id="generate_dbt_date_range", trigger_rule="all_done")
    def generate_dbt_date_range(results: list[dict]) -> dict:
        from dbt_cosmos import generate_dbt_date_range as generate
        return generate(results, "context.partitions")


    @task.short_circuit(task_id="prepare_dbt_run")
    def prepare_dbt_run(ti: TaskInstance, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_dbt_date_range")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False


    def dbt_bigquery_searchad_report_gfa_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_searchad_report_gfa",
            selector = "searchad_report_gfa",
            ds_task_id = "generate_dbt_date_range",
        )


    @task(task_id="finalize_dag_run", trigger_rule="all_done")
    def finalize_dag_run(ti: RuntimeTaskInstance):
        from dbt_cosmos import raise_on_failure
        raise_on_failure(ti)


    etl_results = (etl_searchad_report_gfa
        .partial(configs=read_configs())
        .expand(credentials=read_credentials()))

    dbt_date_range = generate_dbt_date_range(etl_results)
    dbt_run = dbt_bigquery_searchad_report_gfa_group()

    dbt_date_range >> prepare_dbt_run() >> dbt_run >> finalize_dag_run()
