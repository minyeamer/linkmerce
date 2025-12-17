from airflow.sdk import DAG, TaskGroup, task
from airflow.task.trigger_rule import TriggerRule
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
from typing import Literal
import pendulum


with DAG(
    dag_id = "cj_eflexs_stock",
    schedule = "25 9 * * *",
    start_date = pendulum.datetime(2025, 12, 18, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:high", "cj:eflexs", "coupang:inventory", "ecount:inventory", "ecount:product",
            "login:cj-eflexs", "login:coupang", "api:ecount", "schedule:daily", "time:morning"],
) as dag:

    with TaskGroup(group_id="cj_group") as cj_group:

        CJ_PATH = ["cjlogistics", "eflexs", "cj_eflexs_stock"]

        @task(task_id="read_cj_variables", retries=3, retry_delay=timedelta(minutes=1))
        def read_cj_variables() -> dict:
            from variables import read
            return read(CJ_PATH, credentials="expand", tables=True, service_account=True)


        @task(task_id="etl_eflexs_stock")
        def etl_eflexs_stock(ti: TaskInstance, **kwargs) -> dict:
            from variables import get_execution_date
            start_date, end_date = get_execution_date(kwargs, subdays=7), get_execution_date(kwargs)
            return main_eflexs(start_date=start_date, end_date=end_date, **ti.xcom_pull(task_ids="read_cj_variables"))

        def main_eflexs(
                userid: str,
                passwd: str,
                mail_info: dict,
                customer_id: list[int],
                start_date: str,
                end_date: str,
                service_account: dict,
                tables: dict[str,str],
                **kwargs
            ) -> dict:
            from linkmerce.common.load import DuckDBConnection
            from linkmerce.api.cj.eflexs import stock
            from linkmerce.extensions.bigquery import BigQueryClient

            with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
                stock(
                    userid = userid,
                    passwd = passwd,
                    mail_info = mail_info,
                    customer_id = customer_id,
                    start_date = start_date,
                    end_date = end_date,
                    connection = conn,
                    progress = False,
                    return_type = "none",
                )

                with BigQueryClient(service_account) as client:
                    return dict(
                        params = dict(
                            customer_id = customer_id,
                            start_date = start_date,
                            end_date = end_date,
                        ),
                        counts = dict(
                            stock = conn.count_table("data"),
                        ),
                        status = dict(
                            stock = client.overwrite_table_from_duckdb(
                                connection = conn,
                                source_table = "data",
                                target_table = tables["stock"],
                                progress = False,
                                truncate_target_table = True,
                            ),
                        ),
                    )


        read_cj_variables() >> etl_eflexs_stock()


    with TaskGroup(group_id="coupang_group") as coupang_group:

        COUPANG_PATH = ["coupang", "wing", "coupang_inventory"]

        @task(task_id="read_coupang_variables", retries=3, retry_delay=timedelta(minutes=1), trigger_rule=TriggerRule.ALWAYS)
        def read_coupang_variables() -> dict:
            from variables import read
            return read(COUPANG_PATH, tables=True, service_account=True)

        @task(task_id="read_coupang_credentials", retries=3, retry_delay=timedelta(minutes=1), trigger_rule=TriggerRule.ALWAYS)
        def read_coupang_credentials() -> list:
            from variables import read
            return read(COUPANG_PATH, credentials=True)["credentials"]


        @task(task_id="etl_coupang_inventory", map_index_template="{{ credentials['vendor_id'] }}", pool="coupang_pool")
        def etl_coupang_inventory(credentials: dict, variables: dict, **kwargs) -> dict:
            return main_coupang(**credentials, **variables)

        def main_coupang(
                cookies: str,
                vendor_id: str,
                service_account: dict,
                tables: dict[str,str],
                merge: dict[str,dict],
                **kwargs
            ) -> dict:
            from linkmerce.common.load import DuckDBConnection
            from linkmerce.api.coupang.wing import rocket_inventory
            from linkmerce.extensions.bigquery import BigQueryClient

            with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
                rocket_inventory(
                    cookies = cookies,
                    hidden_status = None,
                    vendor_id = vendor_id,
                    connection = conn,
                    return_type = "none",
                )

                with BigQueryClient(service_account) as client:
                    return dict(
                        params = dict(
                            vendor_id = vendor_id,
                            hidden_status = None,
                        ),
                        counts = dict(
                            inventory = conn.count_table("data"),
                        ),
                        status = dict(
                            data = client.merge_into_table_from_duckdb(
                                connection = conn,
                                source_table = "data",
                                staging_table = f'{tables["temp_inventory"]}_{vendor_id}',
                                target_table = tables["inventory"],
                                **merge["inventory"],
                                progress = False,
                            ),
                        ),
                    )


        etl_coupang_inventory.partial(variables=read_coupang_variables()).expand(credentials=read_coupang_credentials())


    with TaskGroup(group_id="ecount_group") as ecount_group:

        ECOUNT_PATH = ["ecount", "api", "ecount_inventory"]

        @task(task_id="read_ecount_variables", retries=3, retry_delay=timedelta(minutes=1), trigger_rule=TriggerRule.ALWAYS)
        def read_ecount_variables() -> dict:
            from variables import read
            return read(ECOUNT_PATH, credentials="expand", tables=True, service_account=True)


        @task(task_id="etl_ecount_inventory")
        def etl_ecount_inventory(ti: TaskInstance, **kwargs) -> dict:
            from variables import get_execution_date
            return main_ecount(api_type="inventory", base_date=get_execution_date(kwargs), **ti.xcom_pull(task_ids="read_ecount_variables"))

        @task(task_id="etl_ecount_product")
        def etl_ecount_product(ti: TaskInstance, **kwargs) -> dict:
            from variables import get_execution_date
            return main_ecount(api_type="product", base_date=get_execution_date(kwargs), **ti.xcom_pull(task_ids="read_ecount_variables"))

        def main_ecount(
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


        read_ecount_variables() >> etl_ecount_inventory() >> etl_ecount_product()


    cj_group >> coupang_group >> ecount_group
