"""
# 운영비용 구글시트 업데이트 파이프라인

## 추출(Extract)
운영비용 구글시트에서 시작/종료 기간별 비용 데이터를 불러온다.

## 변환(Transform)
PK 제약 조건을 검증하여 고유한 행만 필터하고 열 순서를 맞춘다.

## 적재(Load)
- 구글시트 데이터를 기존 BigQuery/Postgres 테이블을 지우고 덮어쓴다.
- 'start_date'의 최소 날짜부터 'end_date'의 최대 날짜까지의 기간을 바탕으로 후속 dbt 모델을 실행한다.
"""

from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from cosmos import DbtTaskGroup
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "gsheets_opex",
    schedule = "55 23 * * 1-5",
    start_date = pendulum.datetime(2026, 6, 23, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=5),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:gsheets", "objective:cost",
        "schedule:weekdays", "time:night", "write:overwrite", "plugin:dbt"
    ],
) as dag:

    PATH = "core.opex"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)


    @task(task_id="dual_load_from_gsheets")
    def dual_load_from_gsheets(configs: dict) -> dict:
        new_rows = main(**configs)
        if new_rows:
            return {
                "ds_start_date": min([row["start_date"] for row in new_rows]),
                "ds_end_date": max([row["end_date"] for row in new_rows]),
            }
        return dict()

    def main(
            key: str,
            sheet: str,
            table: str,
            head: int = 1,
            numericise_ignore: list[int] | bool = list(),
            **kwargs
        ) -> list:
        from dual_load import overwrite_table_from_gsheets

        columns = ["expense_id", "expense_name", "dept_id", "brand_id", "amount", "start_date", "end_date"]
        primary_key = ["expense_id"]
        read_options = {"head": head, "numericise_ignore": numericise_ignore}

        return overwrite_table_from_gsheets(key, sheet, table, columns, primary_key, **read_options)


    @task.short_circuit(task_id="prepare_dbt_run")
    def prepare_dbt_run(ti: TaskInstance, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="dual_load_from_gsheets")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False


    def dbt_bigquery_opex_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_opex",
            selector = "gsheets_opex",
            ds_task_id = "dual_load_from_gsheets",
        )


    dbt_date_range = dual_load_from_gsheets(read_configs())
    dbt_run = dbt_bigquery_opex_group()

    dbt_date_range >> prepare_dbt_run() >> dbt_run
