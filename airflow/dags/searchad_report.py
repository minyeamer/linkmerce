from airflow.sdk import DAG, TaskGroup, task
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "searchad_report",
    schedule = "40 5 * * *",
    start_date = pendulum.datetime(2025, 8, 24, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    tags = ["priority:medium", "searchad:report", "login:searchad", "login:gfa", "schedule:daily", "time:morning"],
) as dag:

    with TaskGroup(group_id="searchad_group") as searchad_group:

        SEARCHAD_PATH = ["searchad", "manage", "searchad_report"]

        @task(task_id="read_searchad_variables", retries=3, retry_delay=timedelta(minutes=1))
        def read_searchad_variables() -> dict:
            from variables import read
            return read(SEARCHAD_PATH, tables=True, service_account=True)

        @task(task_id="read_searchad_queries", retries=3, retry_delay=timedelta(minutes=1))
        def read_searchad_queries() -> list:
            from variables import read
            variables = read(SEARCHAD_PATH, credentials=True)
            params = variables["params"]
            return [dict(credential, **params[credential["customer_id"]]) for credential in variables["credentials"]]


        @task(task_id="etl_searchad_report", map_index_template="{{ queries['customer_id'] }}")
        def etl_searchad_report(queries: dict, variables: dict, data_interval_end: pendulum.DateTime = None, **kwargs) -> dict:
            date = str(data_interval_end.in_timezone("Asia/Seoul").subtract(days=1).date())
            return main(**queries, date=date, **variables)

        def main(
                customer_id: int | str,
                cookies: str,
                report_id: str,
                report_name: str,
                date: str,
                userid: str,
                service_account: dict,
                tables: dict[str,str],
                **kwargs
            ) -> dict:
            from linkmerce.common.load import DuckDBConnection
            from linkmerce.api.searchad.manage import daily_report
            from linkmerce.extensions.bigquery import BigQueryClient

            with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
                daily_report(
                    customer_id = customer_id,
                    cookies = cookies,
                    report_id = report_id,
                    report_name = report_name,
                    userid = userid,
                    start_date = date,
                    connection = conn,
                    return_type = "none",
                )

                with BigQueryClient(service_account) as client:
                    return dict(
                        params = dict(
                            customer_id = customer_id,
                            report_id = report_id,
                            report_name = report_name,
                            date = date,
                        ),
                        count = dict(
                            data = conn.count_table("data"),
                        ),
                        status = dict(
                            data = client.load_table_from_duckdb(
                                connection = conn,
                                source_table = "data",
                                target_table = tables["data"],
                                progress = False,
                            ),
                        ),
                    )


        (etl_searchad_report
            .partial(variables=read_searchad_variables())
            .expand(queries=read_searchad_queries()))


    with TaskGroup(group_id="gfa_group") as gfa_group:

        GFA_PATH = ["searchad", "gfa", "searchad_report"]

        @task(task_id="read_gfa_variables", retries=3, retry_delay=timedelta(minutes=1), trigger_rule=TriggerRule.ALWAYS)
        def read_gfa_variables() -> dict:
            from variables import read
            return read(GFA_PATH, tables=True, service_account=True)

        @task(task_id="read_gfa_credentials", retries=3, retry_delay=timedelta(minutes=1), trigger_rule=TriggerRule.ALWAYS)
        def read_gfa_credentials() -> list:
            from variables import read
            return read(GFA_PATH, credentials=True)["credentials"]


        @task(task_id="etl_gfa_report", map_index_template="{{ queries['account_no'] }}")
        def etl_gfa_report(credentials: dict, variables: dict, data_interval_end: pendulum.DateTime = None, **kwargs) -> dict:
            date = str(data_interval_end.in_timezone("Asia/Seoul").subtract(days=1).date())
            return main(**credentials, date=date, **variables)

        def main(
                account_no: int | str,
                cookies: str,
                date: str,
                service_account: dict,
                tables: dict[str,str],
                **kwargs
            ) -> dict:
            from linkmerce.common.load import DuckDBConnection
            from linkmerce.api.searchad.gfa import performance_report
            from linkmerce.extensions.bigquery import BigQueryClient

            with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
                performance_report(
                    account_no = account_no,
                    cookies = cookies,
                    start_date = date,
                    connection = conn,
                    progress = False,
                    return_type = "none",
                )

                with BigQueryClient(service_account) as client:
                    return dict(
                        params = dict(
                            account_no = account_no,
                            date = date,
                        ),
                        count = dict(
                            data = conn.count_table("data"),
                        ),
                        status = dict(
                            data = client.load_table_from_duckdb(
                                connection = conn,
                                source_table = "data",
                                target_table = tables["data"],
                                progress = False,
                            ),
                        ),
                    )


        (etl_gfa_report
            .partial(variables=read_gfa_variables())
            .expand(credentials=read_gfa_credentials()))


    searchad_group >> gfa_group
