from airflow.sdk import DAG, task
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "smartstore_invoice",
    schedule = MultipleCronTriggerTimetable(
        "0 3 * * *",
        "30 10 * * 1-5",
        "0 15 * * 1-5",
        timezone = "Asia/Seoul",
    ),
    start_date = pendulum.datetime(2025, 10, 25, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:high", "smartstore:order", "api:smartstore", "schedule:daily", "time:daytime"],
) as dag:

    PATH = ["smartstore", "api", "smartstore_invoice"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from variables import read
        return read(PATH, credentials=True)["credentials"]


    FIRST_SCHEDULE = "03:00"

    @task(task_id="etl_smartstore_invoice", map_index_template="{{ credentials['channel_seq'] }}")
    def etl_smartstore_invoice(credentials: dict, variables: dict, **kwargs) -> dict:
        from variables import get_execution_date
        range_type = "DISPATCHED_DATETIME" if get_execution_date(kwargs, format="%H:%M") == FIRST_SCHEDULE else "PAYED_DATETIME"
        return main(**credentials, date=get_execution_date(kwargs, subdays=1), range_type=range_type, **variables)

    def main(
            client_id: str,
            client_secret: str,
            channel_seq: str,
            date: str,
            range_type: str,
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.smartstore.api import order as smartstore_order
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = dict(delivery = "smartstore_delivery")

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            smartstore_order(
                client_id = client_id,
                client_secret = client_secret,
                start_date = date,
                range_type = range_type,
                connection = conn,
                tables = sources,
                progress = False,
                return_type = "none",
            )

            query = "SELECT MIN(DATE(payment_dt)), MAX(DATE(payment_dt)) FROM {}".format(sources["delivery"])
            payment_date_from, payment_date_to = conn.execute(query).fetchall()[0]

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        channel_seq = channel_seq,
                        date = date,
                        range_type = range_type,
                        payment_date_from = payment_date_from,
                        payment_date_to = payment_date_to,
                    ),
                    count = dict(
                        data = conn.count_table(sources["delivery"]),
                    ),
                    status = dict(
                        data = (client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["delivery"],
                            staging_table = f'{tables["temp_delivery"]}_{channel_seq}',
                            target_table = tables["delivery"],
                            **merge["delivery"],
                            where_clause = f"DATE(T.payment_dt) BETWEEN '{payment_date_from}' AND '{payment_date_to}'",
                            progress = False,
                        ) if payment_date_from is not None else True),
                    ),
                )


    etl_smartstore_invoice.partial(variables=read_variables()).expand(credentials=read_credentials())
