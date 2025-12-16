from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
from typing import Literal
import pendulum


with DAG(
    dag_id = "sabangnet_product",
    schedule = "20 23 * * 1-5",
    start_date = pendulum.datetime(2025, 10, 22, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:high", "sabangnet:product", "sabangnet:option", "sabangnet:mapping", "login:sabangnet", "schedule:weekdays", "time:night"],
) as dag:

    PATH = ["sabangnet", "admin", "sabangnet_product"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, credentials="expand", tables=True, service_account=True)


    @task(task_id="etl_sabangnet_product", pool="sabangnet_pool")
    def etl_sabangnet_product(ti: TaskInstance, **kwargs) -> dict:
        from variables import get_execution_date
        return main_product(product_type="product", end_date=get_execution_date(kwargs), **ti.xcom_pull(task_ids="read_variables"))

    @task(task_id="etl_sabangnet_option", pool="sabangnet_pool")
    def etl_sabangnet_option(ti: TaskInstance, **kwargs) -> dict:
        from variables import get_execution_date
        return main_product(product_type="option", end_date=get_execution_date(kwargs), **ti.xcom_pull(task_ids="read_variables"))

    @task(task_id="etl_sabangnet_add_product", pool="sabangnet_pool")
    def etl_sabangnet_add_product(ti: TaskInstance, **kwargs) -> dict:
        from variables import get_execution_date
        return main_product(product_type="add_product", end_date=get_execution_date(kwargs), **ti.xcom_pull(task_ids="read_variables"))

    def main_product(
            userid: str,
            passwd: str,
            domain: int,
            end_date: str,
            product_type: Literal["product","option","add_product"],
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.extensions.bigquery import BigQueryClient
        from importlib import import_module
        module = dict(product="product", option="option_download", add_product="add_product")[product_type]
        extract = getattr(import_module("linkmerce.api.sabangnet.admin"), module)
        include_deleted = (product_type in {"product","option"})
        disable_progress = (product_type != "option")

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            for is_deleted in ([False, True] if include_deleted else [None]):
                extract(
                    userid = userid,
                    passwd = passwd,
                    domain = domain,
                    start_date = "2000-01-01",
                    end_date = end_date,
                    **(dict(is_deleted = is_deleted) if include_deleted else dict()),
                    connection = conn,
                    **(dict(progress = False) if disable_progress else dict()),
                    return_type = "none",
                )

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        start_date = "2000-01-01",
                        end_date = end_date,
                        **(dict(is_deleted = [False, True]) if include_deleted else dict()),
                    ),
                    count = {
                        product_type: conn.count_table("data"),
                    },
                    status = {
                        product_type: client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = "data",
                            staging_table = tables[f"temp_{product_type}"],
                            target_table = tables[product_type],
                            **merge[product_type],
                            progress = False,
                        ),
                    },
                )


    @task(task_id="etl_sabangnet_mapping", pool="sabangnet_pool")
    def etl_sabangnet_mapping(ti: TaskInstance, **kwargs) -> dict:
        from variables import get_execution_date
        return main_mapping(end_date=get_execution_date(kwargs), **ti.xcom_pull(task_ids="read_variables"))

    def main_mapping(
            userid: str,
            passwd: str,
            domain: int,
            end_date: str,
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.sabangnet.admin import option_mapping
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = dict(product="product_mapping", sku="sku_mapping")

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            option_mapping(
                userid = userid,
                passwd = passwd,
                domain = domain,
                start_date = "2000-01-01",
                end_date = end_date,
                connection = conn,
                tables = sources,
                progress = False,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        start_date = "2000-01-01",
                        end_date = end_date,
                    ),
                    counts = dict(
                        product = conn.count_table(sources["product"]),
                        sku = conn.count_table(sources["sku"]),
                    ),
                    status = dict(
                        product = client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["product"],
                            staging_table = tables["temp_mapping_product"],
                            target_table = tables["mapping_product"],
                            **merge["mapping_product"],
                            progress = False,
                        ),
                        sku = client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["sku"],
                            staging_table = tables["temp_mapping_sku"],
                            target_table = tables["mapping_sku"],
                            **merge["mapping_sku"],
                            progress = False,
                        ),
                    ),
                )


    read_variables() >> etl_sabangnet_product() >> etl_sabangnet_option() >> etl_sabangnet_add_product() >> etl_sabangnet_mapping()
