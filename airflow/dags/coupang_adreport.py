from airflow.sdk import DAG, task
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "coupang_adreport",
    schedule = "50 5 * * *",
    start_date = pendulum.datetime(2025, 11, 1, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:medium", "coupang:adreport", "login:coupang", "schedule:daily", "time:morning"],
) as dag:

    PATH = ["coupang", "advertising", "coupang_adreport"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from variables import read
        return read(PATH, credentials=True)["credentials"]


    @task(task_id="etl_coupang_adreport", map_index_template="{{ credentials['vendor_id'] }}")
    def etl_coupang_adreport(credentials: dict, variables: dict, data_interval_end: pendulum.DateTime = None, **kwargs) -> dict:
        date = str(data_interval_end.in_timezone("Asia/Seoul").subtract(days=1).date())
        return main(**credentials, date=date, **variables)

    def main(
            cookies: str,
            vendor_id: str,
            date: str,
            service_account: dict,
            tables: dict[str,str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.advertising import marketing_report
        from linkmerce.extensions.bigquery import BigQueryClient

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            marketing_report(
                cookies = cookies,
                vendor_id = vendor_id,
                start_date = date,
                date_type = "daily",
                report_type = "vendorItem",
                progress = False,
                connection = conn,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        vendor_id = vendor_id,
                        date = date,
                        date_type = "daily",
                        report_type = "vendorItem",
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


    etl_coupang_adreport.partial(variables=read_variables()).expand(credentials=read_credentials())
