"""
# 쿠팡 로켓그로스 정산 리포트 ETL 파이프라인

## 인증(Credentials)
크롬 확장프로그램을 통해 Slack 채널 '_쿠팡-윙-쿠키'에 업로드되는 로그인 쿠키를 추출한다.

## 추출(Extract)
실행 시점(data_interval_end) 전일이 포함된 1주간 매출인식일 기준으로 집계된
판매 수수료 리포트와 입출고비/배송비 리포트를 다운로드한다.
(월요일부터 일요일까지를 한 주로 본다.)

## 변환(Transform)
엑셀 바이너리 형식의 정산 리포트를 JSON 형식으로 변환하고 리포트 유형별 DuckDB 테이블에 적재한다.

## 적재(Load)
각각의 정산 리포트를 대응되는 BigQuery/Postgres 테이블과 MERGE 문으로 병합한다.
(같은 날짜에 여러 번 DAG을 재실행해도 중복된 데이터가 적재되지 않게 보장한다.
특히, 입출고비/배송비 리포트는 파티션 날짜와 조회 기준이 다르므로 반드시 중복 처리해야 한다.)
적재 과정에서 수집한 리포트의 날짜 파티션 범위를 바탕으로 후속 dbt 모델을 실행한다.
"""

from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from cosmos import DbtTaskGroup
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "coupang_rocket_sales",
    schedule = "0 9 * * *",
    start_date = pendulum.datetime(2026, 11, 24, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:coupang-wing", "objective:sales", "credentials:cookies",
        "schedule:daily", "time:morning", "write:merge", "plugin:dbt",
        "provider:slack", "upstream:extension"
    ],
) as dag:

    PATH = "coupang.wing.rocket_sales"

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
            if not (isinstance(vendor, dict) and vendor.get("wing", True)):
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


    @task(task_id="etl_coupang_rocket_sales", map_index_template="{{ credentials['vendor_id'] }}")
    def etl_coupang_rocket_sales(credentials: dict, configs: dict, **kwargs) -> dict:
        from airflow_utils import get_datetime
        dates = dict(zip(["start_date", "end_date"], generate_sales_date(get_datetime(kwargs))))
        return main(**credentials, **dates, **configs)

    def generate_sales_date(data_interval_end: pendulum.DateTime) -> tuple[str, str]:
        """실행 시점(data_interval_end)의 전일이 포함된 월요일-일요일 주간을 계산해 조회 기간을 반환한다.
        쿠팡 정산 리포트의 월초 절단을 고려해 주간 범위에 월 1일이 포함되면 전일이 속한 보고서 기간을 조회한다.
        """
        def get_last_monday(datetime: pendulum.DateTime) -> pendulum.DateTime:
            weekday = datetime.day_of_week # Monday: 0 - Sunday: 6
            return datetime if weekday == 0 else datetime.subtract(days=weekday)

        yesterday = data_interval_end.subtract(days=1)
        start_date = get_last_monday(yesterday)
        end_date = start_date.add(days=6)
        first_of_month = end_date.start_of("month")

        if start_date <= first_of_month <= end_date:
            if yesterday < first_of_month:
                start_date = start_date.subtract(days=1)
                end_date = first_of_month
            else:
                start_date = first_of_month
        return start_date.format("YYYY-MM-DD"), end_date.format("YYYY-MM-DD")

    def main(
            cookies: str,
            vendor_id: str,
            start_date: str,
            end_date: str,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.wing import rocket_settlement_download
        from dual_load import merge_table_from_duckdb
        sources = {"sales": "coupang_rocket_sales", "shipping": "coupang_rocket_shipping"}

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            rocket_settlement_download(
                cookies = cookies,
                vendor_id = vendor_id,
                start_date = start_date,
                end_date = end_date,
                date_type = "SALES",
                progress = False,
                connection = conn,
                return_type = "none",
            )

            sales_dates = conn.unique(sources["sales"], "sales_date")
            shipping_dates = conn.unique(sources["shipping"], "sales_date")

            return {
                "context": {
                    "partitions": sorted(map(str, set(sales_dates + shipping_dates))),
                },
                "params": {
                    "vendor_id": vendor_id,
                    "start_date": start_date,
                    "end_date": end_date,
                    "date_type": "SALES",
                },
                "results": {
                    "sales": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["sales"],
                        target_table = tables["sales"],
                        **merge["sales"],
                        where_clause = conn.expr_date_range("T.sales_date", sales_dates),
                        execute = bool(sales_dates),
                    ),
                    "shipping": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["shipping"],
                        target_table = tables["shipping"],
                        **merge["shipping"],
                        where_clause = conn.expr_date_range("T.sales_date", shipping_dates),
                        execute = bool(shipping_dates),
                    ),
                }
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


    def dbt_bigquery_coupang_rocket_sales_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_coupang_rocket_sales",
            selector = "coupang_rocket_sales",
            ds_task_id = "generate_dbt_date_range",
        )

    def dbt_postgres_coupang_rocket_sales_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_postgres
        return dynamic_mapping_dbt_postgres(
            group_id = "dbt_postgres_coupang_rocket_sales",
            selector = "coupang_rocket_sales",
            ds_task_id = "generate_dbt_date_range",
        )


    @task(task_id="finalize_dag_run", trigger_rule="all_done")
    def finalize_dag_run(ti: TaskInstance):
        from dbt_cosmos import raise_on_failure
        raise_on_failure(ti)


    etl_results = (etl_coupang_rocket_sales
        .partial(configs=read_configs())
        .expand(credentials=read_credentials()))

    dbt_date_range = generate_dbt_date_range(etl_results)
    dbt_run = [dbt_bigquery_coupang_rocket_sales_group(), dbt_postgres_coupang_rocket_sales_group()]

    dbt_date_range >> prepare_dbt_run() >> dbt_run >> finalize_dag_run()
