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
    def etl_smartstore_order(credentials: dict, variables: dict, **kwargs) -> dict:
        from variables import get_execution_date
        return main(**credentials, date=get_execution_date(kwargs, subdays=1), **variables)

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
        from linkmerce.api.smartstore.api import order as smartstore_order
        from linkmerce.api.smartstore.api import aggregated_order_status
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = dict(
            order = "smartstore_order",
            product_order = "smartstore_product_order",
            delivery = "smartstore_delivery",
            option = "smartstore_option",
        )

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            smartstore_order(
                client_id = client_id,
                client_secret = client_secret,
                start_date = date,
                range_type = "PAYED_DATETIME",
                connection = conn,
                tables = sources,
                progress = False,
                return_type = "none",
            )

            aggregated_order_status(
                client_id = client_id,
                client_secret = client_secret,
                channel_seq = channel_seq,
                start_date = date,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            query = f"SELECT MIN(DATE(payment_dt)), MAX(DATE(payment_dt)) FROM data"
            payment_date_from, payment_date_to = conn.execute(query).fetchall()[0]

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        channel_seq = channel_seq,
                        date = date,
                        range_type = "PAYED_DATETIME",
                        payment_date_from = payment_date_from,
                        payment_date_to = payment_date_to,
                    ),
                    count = dict(
                        order = conn.count_table(sources["order"]),
                        product_order = conn.count_table(sources["product_order"]),
                        delivery = conn.count_table(sources["delivery"]),
                        option = conn.count_table(sources["option"]),
                        status = conn.count_table("data"),
                    ),
                    status = dict(
                        order = client.load_table_from_duckdb(
                            connection = conn,
                            source_table = sources["order"],
                            target_table = tables["order"],
                            progress = False,
                        ),
                        product_order = client.load_table_from_duckdb(
                            connection = conn,
                            source_table = sources["product_order"],
                            target_table = tables["product_order"],
                            progress = False,
                        ),
                        delivery = client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["delivery"],
                            staging_table = f'{tables["temp_delivery"]}_{channel_seq}',
                            target_table = tables["delivery"],
                            **merge["delivery"],
                            where_clause = f"DATE(T.payment_dt) = '{date}'",
                            progress = False,
                        ),
                        option = client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["option"],
                            staging_table = f'{tables["temp_option"]}_{channel_seq}',
                            target_table = tables["option"],
                            **merge["option"],
                            progress = False,
                        ),
                        status = (client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = "data",
                            staging_table = f'{tables["temp_order_status"]}_{channel_seq}',
                            target_table = tables["order_status"],
                            **merge["status"],
                            where_clause = f"DATE(T.payment_dt) BETWEEN '{payment_date_from}' AND '{payment_date_to}'",
                            progress = False,
                        ) if payment_date_from is not None else True),
                    ),
                )


    etl_smartstore_order.partial(variables=read_variables()).expand(credentials=read_credentials())
