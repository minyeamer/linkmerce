from airflow.sdk import DAG, task
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "smartstore_product",
    schedule = "25 8 * * *",
    start_date = pendulum.datetime(2025, 9, 3, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=5),
    catchup = False,
    tags = ["priority:high", "smartstore:product", "api:smartstore", "schedule:daily", "time:morning"],
) as dag:

    PATH = ["smartstore", "api", "smartstore_product"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from variables import read
        return read(PATH, credentials=True)["credentials"]


    @task(task_id="etl_smartstore_product", map_index_template="{{ credentials['channel_seq'] }}")
    def etl_smartstore_product(credentials: dict, variables: dict, **kwargs) -> dict:
        return main(**credentials, **variables)

    def main(
            client_id: str,
            client_secret: str,
            channel_seq: str,
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.smartstore.api import product
        from linkmerce.extensions.bigquery import BigQueryClient

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            product(client_id, client_secret, status_type=["ALL"], channel_seq=channel_seq, connection=conn, return_type="none")

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        channel_seq = channel_seq,
                        status_type = ["ALL"],
                    ),
                    count = dict(
                        product = conn.count_table("data"),
                    ),
                    status = dict(
                        option = client.merge_into_table_from_duckdb(conn, "data", f'{tables["temp_product"]}_{channel_seq}', tables["product"], **merge["product"]),
                    )
                )


    etl_smartstore_product.partial(variables=read_variables()).expand(credentials=read_credentials())
