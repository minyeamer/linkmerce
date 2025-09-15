from airflow.sdk import DAG, task
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "coupang_rocket_sales",
    schedule = "30 9 * * *",
    start_date = pendulum.datetime(2025, 9, 15, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:high", "coupang:rocket", "login:coupang", "schedule:daily", "time:morning"],
) as dag:

    PATH = ["coupang", "wing", "coupang_rocket_sales"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from variables import read
        return read(PATH, credentials=True)["credentials"]


    @task(task_id="etl_coupang_rocket_sales", map_index_template="{{ credentials['vendor_id'] }}")
    def etl_coupang_rocket_sales(credentials: dict, variables: dict, **kwargs) -> dict:
        dates = dict(zip(["start_date", "end_date"], generate_sales_date(**kwargs)))
        return main(**credentials, **dates, **variables)

    def generate_sales_date(data_interval_end: pendulum.DateTime = None, **kwargs) -> tuple[str,str]:
        def get_last_monday(datetime: pendulum.DateTime):
            weekday = datetime.day_of_week # Monday: 0 - Sunday: 6
            return datetime if weekday == 0 else datetime.subtract(days=weekday)
        start_date = get_last_monday(data_interval_end.in_timezone("Asia/Seoul").subtract(days=1))
        end_date = start_date.add(days=7)
        return str(start_date.date()), str(end_date.date())

    def main(
            userid: str,
            passwd: str,
            vendor_id: str,
            start_date: str,
            end_date: str,
            service_account: dict,
            tables: dict[str,str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.wing import rocket_settlement_download
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = dict(sales="coupang_rocket_sales", shipping="coupang_rocket_shipping")

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            rocket_settlement_download(
                userid = userid,
                passwd = passwd,
                vendor_id = vendor_id,
                start_date = start_date,
                end_date = end_date,
                date_type = "SALES",
                progress = False,
                connection = conn,
                tables = sources,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        vendor_id = vendor_id,
                        start_date = start_date,
                        end_date = end_date,
                        date_type = "SALES",
                    ),
                    count = dict(
                        sales = conn.count_table(sources["sales"]),
                        shipping = conn.count_table(sources["shipping"]),
                    ),
                    status = dict(
                        sales = client.overwrite_table_from_duckdb(
                            connection = conn,
                            source_table = sources["sales"],
                            target_table = tables["sales"],
                            where_clause = f"(sales_date BETWEEN '{start_date}' AND '{end_date}') AND (vendor_id = '{vendor_id}')",
                            progress = False,
                        ),
                        shipping = client.overwrite_table_from_duckdb(
                            connection = conn,
                            source_table = sources["shipping"],
                            target_table = tables["shipping"],
                            where_clause = f"(sales_date BETWEEN '{start_date}' AND '{end_date}') AND (vendor_id = '{vendor_id}')",
                            progress = False,
                        ),
                    ),
                )


    etl_coupang_rocket_sales.partial(variables=read_variables()).expand(credentials=read_credentials())
