from airflow.sdk import DAG, TaskGroup, task
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "searchad_master",
    schedule = "40 23 * * 1-5",
    start_date = pendulum.datetime(2025, 8, 30, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    tags = ["priority:medium", "searchad:report", "api:searchad", "login:gfa", "schedule:weekdays", "time:night"],
) as dag:

    with TaskGroup(group_id="api_group") as api_group:

        API_PATH = ["searchad", "api", "searchad_master"]

        @task(task_id="read_api_variables", retries=3, retry_delay=timedelta(minutes=1))
        def read_api_variables() -> dict:
            from variables import read
            return read(API_PATH, tables=True, service_account=True)

        @task(task_id="read_api_credentials", retries=3, retry_delay=timedelta(minutes=1))
        def read_api_credentials() -> list:
            from variables import read
            return read(API_PATH, credentials=True)["credentials"]


        @task(task_id="etl_searchad_master", map_index_template="{{ credentials['customer_id'] }}")
        def etl_searchad_master(credentials: dict, variables: dict, data_interval_end: pendulum.DateTime = None, **kwargs) -> dict:
            from_date = str(data_interval_end.in_timezone("Asia/Seoul").subtract(days=365*2).date())
            return {api_type: main_api(api_type, **credentials, from_date=from_date, **variables) for api_type in ["campaign", "adgroup", "ad"]}

        def main_api(
                api_type: str,
                api_key: str,
                secret_key: str,
                customer_id: int | str,
                from_date: str,
                service_account: dict,
                tables: dict[str,str],
                merge: dict[str,dict],
                **kwargs
            ) -> dict:
            from linkmerce.common.load import DuckDBConnection
            from linkmerce.extensions.bigquery import BigQueryClient
            from importlib import import_module
            import logging
            extract = getattr(import_module("linkmerce.api.searchad.api"), api_type)

            with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
                extract(
                    api_key = api_key,
                    secret_key = secret_key,
                    customer_id = customer_id,
                    from_date = from_date,
                    connection = conn,
                    return_type = "none",
                )
                logging.info(f"[{customer_id}] API request completed for downloading the {api_type} master report")

                with BigQueryClient(service_account) as client:
                    return dict(
                        params = dict(
                            customer_id = customer_id,
                            from_date = from_date,
                        ),
                        count = dict(
                            data = conn.count_table("data"),
                        ),
                        status = dict(
                            data = client.merge_into_table_from_duckdb(
                                connection = conn,
                                source_table = "data",
                                staging_table = f'{tables[f"temp_{api_type}"]}_{customer_id}',
                                target_table = tables[api_type],
                                **merge[api_type],
                                progress = False,
                            ),
                        ),
                    )


        (etl_searchad_master
            .partial(variables=read_api_variables())
            .expand(credentials=read_api_credentials()))


    with TaskGroup(group_id="gfa_group") as gfa_group:

        GFA_PATH = ["searchad", "gfa", "searchad_master"]

        @task(task_id="read_gfa_variables", retries=3, retry_delay=timedelta(minutes=1), trigger_rule=TriggerRule.ALWAYS)
        def read_gfa_variables() -> dict:
            from variables import read
            return read(GFA_PATH, tables=True, service_account=True)

        @task(task_id="read_gfa_credentials", retries=3, retry_delay=timedelta(minutes=1), trigger_rule=TriggerRule.ALWAYS)
        def read_gfa_credentials() -> list:
            from variables import read
            return read(GFA_PATH, credentials=True)["credentials"]


        @task(task_id="etl_gfa_master", map_index_template="{{ credentials['account_no'] }}")
        def etl_gfa_master(credentials: dict, variables: dict, **kwargs) -> dict:
            return {api_type: main_gfa(api_type, **credentials, **variables) for api_type in ["campaign", "adset", "creative"]}

        def main_gfa(
                api_type: str,
                account_no: int | str,
                cookies: str,
                service_account: dict,
                tables: dict[str,str],
                merge: dict[str,dict],
                **kwargs
            ) -> dict:
            from linkmerce.common.load import DuckDBConnection
            from linkmerce.extensions.bigquery import BigQueryClient
            from importlib import import_module
            import logging
            extract = getattr(import_module("linkmerce.api.searchad.gfa"), api_type)
            status = [("RUNNABLE" if api_type == "campaign" else "ALL"), "DELETED"]

            with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
                extract(
                    account_no = account_no,
                    cookies = cookies,
                    status = status,
                    connection = conn,
                    progress = False,
                    return_type = "none",
                )
                logging.info(f"[{account_no}] request completed for downloading the {api_type} list")

                with BigQueryClient(service_account) as client:
                    return dict(
                        params = dict(
                            account_no = account_no,
                            status = status,
                        ),
                        count = dict(
                            data = conn.count_table("data"),
                        ),
                        status = dict(
                            data = client.merge_into_table_from_duckdb(
                                connection = conn,
                                source_table = "data",
                                staging_table = f'{tables[f"temp_{api_type}"]}_{account_no}',
                                target_table = tables[api_type],
                                **merge[api_type],
                                progress = False,
                            ),
                        ),
                    )


        (etl_gfa_master
            .partial(variables=read_gfa_variables())
            .expand(credentials=read_gfa_credentials()))


    api_group >> gfa_group
