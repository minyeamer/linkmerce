from airflow.sdk import DAG, task
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "coupang_rocket_sales",
    schedule = "10 9 * * *",
    start_date = pendulum.datetime(2025, 10, 22, tz="Asia/Seoul"),
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


    @task(task_id="etl_coupang_rocket_sales", map_index_template="{{ credentials['vendor_id'] }}", pool="coupang_pool")
    def etl_coupang_rocket_sales(credentials: dict, variables: dict, **kwargs) -> dict:
        dates = dict(zip(["start_date", "end_date"], generate_sales_date(**kwargs)))
        return main(**credentials, **dates, **variables)

    def generate_sales_date(data_interval_end: pendulum.DateTime = None, **kwargs) -> tuple[str,str]:
        from variables import in_timezone
        def get_last_monday(datetime: pendulum.DateTime) -> pendulum.DateTime:
            weekday = datetime.day_of_week # Monday: 0 - Sunday: 6
            return datetime if weekday == 0 else datetime.subtract(days=weekday)
        start_date = get_last_monday(in_timezone(data_interval_end, subdays=1))
        end_date = start_date.add(days=6)
        return start_date.format("YYYY-MM-DD"), end_date.format("YYYY-MM-DD")

    def main(
            cookies: str,
            vendor_id: str,
            start_date: str,
            end_date: str,
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.wing import rocket_settlement_download
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = dict(sales="coupang_rocket_sales", shipping="coupang_rocket_shipping")
        date_array = dict(sales=list(), shipping=list())

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            rocket_settlement_download(
                cookies = cookies,
                vendor_id = vendor_id,
                start_date = start_date,
                end_date = end_date,
                date_type = "SALES",
                progress = False,
                connection = conn,
                tables = sources,
                return_type = "none",
            )

            date_array = {table: conn.unique(sources[table], "sales_date") for table in ["sales", "shipping"]}

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        vendor_id = vendor_id,
                        start_date = start_date,
                        end_date = end_date,
                        date_type = "SALES",
                    ),
                    counts = dict(
                        sales = conn.count_table(sources["sales"]),
                        shipping = conn.count_table(sources["shipping"]),
                    ),
                    dates = date_array,
                    status = dict(
                        sales = client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["sales"],
                            staging_table = tables["temp_sales"],
                            target_table = tables["sales"],
                            **merge["sales"],
                            where_clause = "({sales_date}) AND (T.vendor_id = '{vendor_id}')".format(
                                sales_date = conn.expr_date_range("T.sales_date", date_array["sales"]),
                                vendor_id = vendor_id,
                            ),
                            progress = False,
                        ),
                        shipping = client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["shipping"],
                            staging_table = tables["temp_shipping"],
                            target_table = tables["shipping"],
                            **merge["shipping"],
                            where_clause = "({sales_date}) AND (T.vendor_id = '{vendor_id}')".format(
                                sales_date = conn.expr_date_range("T.sales_date", date_array["shipping"]),
                                vendor_id = vendor_id,
                            ),
                            progress = False,
                        ),
                    ),
                )


    etl_coupang_rocket_sales.partial(variables=read_variables()).expand(credentials=read_credentials())
