"""
# 쿠팡 로켓그로스 재고 ETL 파이프라인

## 인증(Credentials)
쿠팡 Open API 인증 키(Access Key, Secret Key, 업체 코드)가 필요하다.

## 추출(Extract)
매일 오전/오후 재고 업데이트 시간에 맞춰 쿠팡 업체별 로켓그로스 재고 목록을 수집한다.

## 변환(Transform)
JSON 형식의 응답 본문을 파싱하여 DuckDB 테이블에 적재한다.

## 적재(Load)
데이터를 BigQuery/Postgres 테이블의 끝에 추가한다.
"""

from airflow.sdk import DAG, task
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.models.taskinstance import TaskInstance
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from cosmos import DbtTaskGroup
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "coupang_rocket_inventory",
    schedule = MultipleCronTriggerTimetable(
        "0 11 * * *",
        "30 17 * * *",
        timezone = "Asia/Seoul",
    ),
    start_date = pendulum.datetime(2026, 10, 8, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:coupang-api", "objective:stock", "credentials:api-key",
        "schedule:daily", "time:morning", "time:afternoon", "write:append", "plugin:dbt"
    ],
) as dag:

    PATH = "coupang.api.inventory"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    @task(task_id="etl_coupang_rocket_inventory", map_index_template="{{ credentials['vendor_id'] }}")
    def etl_coupang_rocket_inventory(credentials: dict, configs: dict, **kwargs) -> dict:
        return main(**credentials, **configs)

    def main(
            access_key: str,
            secret_key: str,
            vendor_id: str,
            tables: dict[str, str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.api import rocket_inventory
        from dual_load import load_table_from_duckdb
        source = "coupang_rocket_inventory"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            rocket_inventory(
                access_key = access_key,
                secret_key = secret_key,
                vendor_id = vendor_id,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            return {
                "context": {
                    "partitions": sorted(map(str, conn.unique(source, "DATE(updated_at)"))),
                },
                "params": {
                    "vendor_id": vendor_id,
                },
                "result": load_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables["table"],
                ),
            }


    @task(task_id="generate_dbt_date_range", trigger_rule="all_done")
    def generate_dbt_date_range(result: dict) -> dict:
        from dbt_cosmos import generate_dbt_date_range as generate
        return generate(result, "context.partitions")


    @task.short_circuit(task_id="prepare_dbt_run", ignore_downstream_trigger_rules=False)
    def prepare_dbt_run(ti: TaskInstance, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_dbt_date_range")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False


    def dbt_bigquery_coupang_rocket_inventory_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_coupang_rocket_inventory",
            selector = "coupang_rocket_inventory",
            ds_task_id = "generate_dbt_date_range",
        )

    def dbt_postgres_coupang_rocket_inventory_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_postgres
        return dynamic_mapping_dbt_postgres(
            group_id = "dbt_postgres_coupang_rocket_inventory",
            selector = "coupang_rocket_inventory",
            ds_task_id = "generate_dbt_date_range",
        )


    @task(task_id="finalize_dag_run", trigger_rule="all_done")
    def finalize_dag_run(ti: RuntimeTaskInstance):
        from dbt_cosmos import raise_on_failure
        raise_on_failure(ti)


    etl_results = (etl_coupang_rocket_inventory
        .partial(configs=read_configs())
        .expand(credentials=read_credentials()))

    dbt_date_range = generate_dbt_date_range(etl_results)
    dbt_run = [dbt_bigquery_coupang_rocket_inventory_group(), dbt_postgres_coupang_rocket_inventory_group()]

    dbt_date_range >> prepare_dbt_run() >> dbt_run >> finalize_dag_run()
