from airflow.sdk import DAG, task
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "smartstore_bizdata",
    schedule = "10 8 * * *",
    start_date = pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    tags = ["priority:low", "smartstore:bizdata", "api:smartstore", "schedule:weekdays", "time:morning"],
) as dag:

    PATH = ["smartstore", "api", "smartstore_bizdata"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from variables import read
        return read(PATH, credentials=True)["credentials"]


    @task(task_id="etl_smartstore_bizdata", map_index_template="{{ credentials['channel_seq'] }}")
    def etl_smartstore_bizdata(credentials: dict, variables: dict, **kwargs) -> dict:
        from variables import get_execution_date
        return main(**credentials, date=get_execution_date(kwargs, subdays=1), **variables)

    def main(
            client_id: str,
            client_secret: str,
            channel_seq: str,
            date: str,
            service_account: dict,
            tables: dict[str,str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.smartstore.api import marketing_channel
        from linkmerce.extensions.bigquery import BigQueryClient

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            marketing_channel(
                client_id = client_id,
                client_secret = client_secret,
                channel_seq = channel_seq,
                start_date = date,
                end_date = date,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        channel_seq = channel_seq,
                        date = date,
                    ),
                    counts = dict(
                        data = conn.count_table("data"),
                    ),
                    status = dict(
                        data = client.load_table_from_duckdb(
                            connection = conn,
                            source_table = "data",
                            target_table = tables["marketing_channel"],
                            progress = False,
                        ),
                    ),
                )


    etl_smartstore_bizdata.partial(variables=read_variables()).expand(credentials=read_credentials())
