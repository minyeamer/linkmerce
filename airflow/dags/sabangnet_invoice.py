from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "sabangnet_invoice",
    schedule = MultipleCronTriggerTimetable(
        "30 10 * * 1-5",
        "30 14 * * 1-5",
        "50 23 * * 1-5",
        timezone = "Asia/Seoul",
    ),
    start_date = pendulum.datetime(2025, 11, 27, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    tags = ["priority:low", "sabangnet:order", "login:sabangnet", "schedule:weekdays", "time:allday"],
) as dag:

    PATH = ["sabangnet", "admin", "sabangnet_invoice"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, credentials="expand", tables=True, service_account=True)


    LAST_SCHEDULE = "23:50"
    LAST_7_DAYS = 7
    TODAY = None

    @task(task_id="etl_sabangnet_invoice", pool="sabangnet_pool")
    def etl_sabangnet_invoice(ti: TaskInstance, **kwargs) -> dict:
        from variables import get_execution_date
        date_type = "dlvery_rcv_dt" if get_execution_date(kwargs, format="%H:%M") == LAST_SCHEDULE else "reg_dm"
        start_date = get_execution_date(kwargs, subdays=(TODAY if date_type == "dlvery_rcv_dt" else LAST_7_DAYS))
        end_date = get_execution_date(kwargs)
        return main(start_date=start_date, end_date=end_date, date_type=date_type, **ti.xcom_pull(task_ids="read_variables"))

    def main(
            userid: str,
            passwd: str,
            domain: int,
            download_no: int,
            start_date: str,
            end_date: str,
            date_type: str,
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.sabangnet.admin import order_download
        from linkmerce.extensions.bigquery import BigQueryClient

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            order_download(
                userid = userid,
                passwd = passwd,
                domain = domain,
                download_no = download_no,
                download_type = "invoice",
                start_date = start_date,
                end_date = end_date,
                date_type = date_type,
                connection = conn,
                return_type = "none",
            )

            query = "SELECT DISTINCT DATE(order_dt) FROM data"
            order_dates = sorted([f"'{date[0]}'" for date in conn.execute(query).fetchall()])

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        start_date = start_date,
                        end_date = end_date,
                        date_type = date_type,
                        download_no = download_no,
                        download_type = "invoice",
                    ),
                    counts = dict(
                        invoice = conn.count_table("data"),
                    ),
                    status = dict(
                        invoice = (client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = "data",
                            staging_table = tables["temp_invoice"],
                            target_table = tables["invoice"],
                            **merge["invoice"],
                            where_clause = f"DATE(T.order_dt) IN ({', '.join(order_dates)})",
                            progress = False,
                        ) if order_dates else True),
                        order_dates = [date[1:-1] for date in order_dates],
                    ),
                )


    read_variables() >> etl_sabangnet_invoice()
