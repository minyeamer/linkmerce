from airflow.sdk import DAG, task
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "searchad_report",
    schedule = "50 5 * * *",
    start_date = pendulum.datetime(2025, 8, 24, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    tags = ["priority:medium", "searchad:report", "login:searchad", "schedule:daily", "time:morning"],
) as dag:

    PATH = ["searchad", "manage", "searchad_report"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, tables=True, service_account=True)

    @task(task_id="read_queries", retries=3, retry_delay=timedelta(minutes=1))
    def read_queries() -> list:
        from variables import read
        variables = read(PATH, credentials=True)
        params = variables["params"]
        credentials = filter_valid_accounts(variables["credentials"])
        return [dict(credential, **params[credential["customer_id"]]) for credential in credentials]

    def filter_valid_accounts(credentials: list[dict]) -> list[dict]:
        from linkmerce.api.searchad.manage import has_permission
        accounts = [credential for credential in credentials if has_permission(**credential)]
        if credentials and (not accounts):
            from airflow.exceptions import AirflowFailException
            raise AirflowFailException("At least one account does not have permissions")
        return accounts


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


    etl_searchad_report.partial(variables=read_variables()).expand(queries=read_queries())
