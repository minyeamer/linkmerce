from airflow.sdk import DAG, task
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "smartstore_order",
    schedule = "30 8 * * *",
    start_date = pendulum.datetime(2025, 9, 1, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:high", "smartstore:order", "api:smartstore", "schedule:daily", "time:morning"],
) as dag:

    PATH = ["smartstore", "api", "smartstore_order"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from variables import read
        return read(PATH, credentials=True)["credentials"]


    @task(task_id="etl_smartstore_order", map_index_template="{{ credentials['channel_seq'] }}")
    def etl_smartstore_order(credentials: dict, variables: dict, data_interval_end: pendulum.DateTime = None, **kwargs) -> dict:
        date = str(data_interval_end.in_timezone("Asia/Seoul").subtract(days=1).date())
        return main(**credentials, date=date, **variables)

    def main(
            client_id: str,
            client_secret: str,
            channel_seq: str,
            date: str,
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.smartstore.api import product_order, aggregated_order_status
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = dict(order="smartstore_order", option="smartstore_option")

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            options = dict(transform_options = dict(tables = sources))
            product_order(client_id, client_secret, date, range_type="PAYED_DATETIME", connection=conn, progress=False, return_type="none", **options)
            aggregated_order_status(client_id, client_secret, date, connection=conn, progress=False, return_type="none")

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        channel_seq = channel_seq,
                        date = date,
                    ),
                    count = dict(
                        order = conn.count_table(sources["order"]),
                        option = conn.count_table(sources["option"]),
                        status = conn.count_table("data"),
                    ),
                    status = dict(
                        order = client.load_table_from_duckdb(conn, sources["order"], tables["order"]),
                        option = client.merge_into_table_from_duckdb(conn, sources["option"], f'{tables["temp_option"]}_{channel_seq}', tables["option"], **merge["option"]),
                        status = client.load_table_from_duckdb(conn, "data", tables["order_status"]),
                    )
                )


    etl_smartstore_order.partial(variables=read_variables()).expand(credentials=read_credentials())
