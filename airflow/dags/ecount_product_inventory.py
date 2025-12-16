from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
from typing import Literal
import pendulum


with DAG(
    dag_id = "ecount_product_inventory",
    schedule = "0 9 * * *",
    start_date = pendulum.datetime(2025, 12, 17, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    tags = ["priority:high", "ecount:product", "ecount:inventory", "api:ecount", "schedule:daily", "time:morning"],
) as dag:

    PATH = ["ecount", "api", "ecount_product_inventory"]

    @task(task_id="read_variables")
    def read_variables() -> dict:
        from variables import read
        return read(PATH, credentials="expand", tables=True, service_account=True)


    @task(task_id="etl_ecount_product")
    def etl_ecount_product(ti: TaskInstance, **kwargs) -> dict:
        from variables import get_execution_date
        return main(api_type="product", base_date=get_execution_date(kwargs), **ti.xcom_pull(task_ids="read_variables"))

    @task(task_id="etl_ecount_inventory")
    def etl_ecount_inventory(ti: TaskInstance, **kwargs) -> dict:
        from variables import get_execution_date
        return main(api_type="inventory", base_date=get_execution_date(kwargs), **ti.xcom_pull(task_ids="read_variables"))

    def main(
            com_code: int | str,
            userid: str,
            api_key: str,
            base_date: str,
            api_type: Literal["product","inventory"],
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.extensions.bigquery import BigQueryClient
        from importlib import import_module
        extract = getattr(import_module("linkmerce.api.ecount.api"), api_type)
        params = dict(base_date=base_date, zero_yn=True) if api_type == "inventory" else dict(comma_yn=True)

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            extract(
                com_code = com_code,
                userid = userid,
                api_key = api_key,
                **params,
                connection = conn,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        com_code = com_code,
                        **params,
                    ),
                    counts = {
                        api_type: conn.count_table("data"),
                    },
                    status = {
                        api_type: client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = "data",
                            staging_table = tables[f"temp_{api_type}"],
                            target_table = tables[api_type],
                            **merge[api_type],
                            progress = False,
                        )
                    },
                )


    read_variables() >> etl_ecount_product() >> etl_ecount_inventory()
