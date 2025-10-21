from airflow.sdk import DAG, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
from typing import Literal
import pendulum


with DAG(
    dag_id = "sabangnet_product",
    schedule = "20 8 * * *",
    start_date = pendulum.datetime(2025, 10, 22, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:high", "sabangnet:product", "login:sabangnet", "schedule:daily", "time:morning"],
) as dag:

    PATH = ["sabangnet", "admin", "sabangnet_product"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, credentials="expand", tables=True, service_account=True)


    @task(task_id="etl_sabangnet_product")
    def etl_sabangnet_product(ti: TaskInstance, data_interval_end: pendulum.DateTime = None, **kwargs) -> dict:
        date = str(data_interval_end.in_timezone("Asia/Seoul").date())
        return main(api_type="product", date=date, **ti.xcom_pull(task_ids="read_variables"))

    @task(task_id="etl_sabangnet_option", trigger_rule=TriggerRule.ALWAYS)
    def etl_sabangnet_option(ti: TaskInstance, data_interval_end: pendulum.DateTime = None, **kwargs) -> dict:
        date = str(data_interval_end.in_timezone("Asia/Seoul").date())
        return main(api_type="option", date=date, **ti.xcom_pull(task_ids="read_variables"))


    def main(
            userid: str,
            passwd: str,
            domain: int,
            api_type: Literal["product","option"],
            date: str,
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.extensions.bigquery import BigQueryClient
        from importlib import import_module
        module = "product" if api_type == "product" else "option_download"
        extract = getattr(import_module("linkmerce.api.sabangnet.admin"), module)

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            for is_deleted in [False, True]:
                extract(
                    userid = userid,
                    passwd = passwd,
                    domain = domain,
                    start_date = "2000-01-01",
                    end_date = date,
                    is_deleted = is_deleted,
                    connection = conn,
                    return_type = "none",
                )

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        start_date = "2000-01-01",
                        end_date = date,
                        is_deleted = [False, True],
                    ),
                    count = dict(
                        data = conn.count_table("data"),
                    ),
                    status = dict(
                        data = client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = "data",
                            staging_table = tables[f"temp_{api_type}"],
                            target_table = tables[api_type],
                            **merge[api_type],
                            progress = False,
                        ),
                    ),
                )


    read_variables() >> etl_sabangnet_product() >> etl_sabangnet_option()
