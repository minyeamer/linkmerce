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


    @task(task_id="etl_coupang_adreport", map_index_template="{{ credentials['vendor_id'] }}", pool="coupang_pool")
    def etl_coupang_adreport(credentials: dict, variables: dict, data_interval_end: pendulum.DateTime = None, **kwargs) -> dict:
        date = str(data_interval_end.in_timezone("Asia/Seoul").subtract(days=1).date())
        if credentials.get("nca"):
            return dict(
                pa = main(**credentials, report_type="pa", date=date, **variables),
                nca = main(**credentials, report_type="nca", date=date, **variables),
            )
        else:
            return main(**credentials, report_type="pa", date=date, **variables)

    def main(
            cookies: str,
            vendor_id: str,
            report_type: str,
            date: str,
            service_account: dict,
            tables: dict[str,str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.advertising import adreport
        from linkmerce.extensions.bigquery import BigQueryClient
        report_level = "creative" if report_type == "nca" else "vendorItem"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            adreport(
                cookies = cookies,
                vendor_id = vendor_id,
                start_date = date,
                report_type = report_type,
                date_type = "daily",
                report_level = report_level,
                connection = conn,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        vendor_id = vendor_id,
                        date = date,
                        report_type = report_type,
                        date_type = "daily",
                        report_level = report_level,
                    ),
                    count = dict(
                        data = conn.count_table("data"),
                    ),
                    status = dict(
                        data = client.load_table_from_duckdb(
                            connection = conn,
                            source_table = "data",
                            target_table = tables[report_type],
                            progress = False,
                        ),
                    ),
                )


    etl_coupang_adreport.partial(variables=read_variables()).expand(credentials=read_credentials())
