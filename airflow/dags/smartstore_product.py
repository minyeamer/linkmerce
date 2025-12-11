from airflow.sdk import DAG, task
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "smartstore_product",
    schedule = "30 23 * * 1-5",
    start_date = pendulum.datetime(2025, 9, 3, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    tags = ["priority:high", "smartstore:product", "smartstore:option", "api:smartstore", "schedule:weekdays", "time:night"],
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
        from linkmerce.api.smartstore.api import product as product_api
        from linkmerce.api.smartstore.api import option as option_api
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = dict(product="smartstore_product", option="smartstore_option")

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            product_api(
                client_id = client_id,
                client_secret = client_secret,
                status_type = ["ALL"],
                channel_seq = channel_seq,
                connection = conn,
                tables = dict(default=sources["product"]),
                progress = False,
                return_type = "none",
            )

            query = "SELECT product_id FROM {} WHERE channel_seq = {}".format(sources["product"], channel_seq)
            product_id = [row[0] for row in conn.execute(query).fetchall()]

            option_api(
                client_id = client_id,
                client_secret = client_secret,
                product_id = product_id,
                channel_seq = channel_seq,
                connection = conn,
                tables = dict(default=sources["option"]),
                progress = False,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        channel_seq = channel_seq,
                        status_type = ["ALL"],
                    ),
                    counts = dict(
                        product = conn.count_table(sources["product"]),
                        option = conn.count_table(sources["option"]),
                    ),
                    status = dict(
                        product = client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["product"],
                            staging_table = f'{tables["temp_product"]}_{channel_seq}',
                            target_table = tables["product"],
                            **merge["product"],
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
                    ),
                )


    etl_smartstore_product.partial(variables=read_variables()).expand(credentials=read_credentials())
