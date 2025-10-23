from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "sabangnet_order",
    schedule = MultipleCronTriggerTimetable(
        "15 9 * * 1~5",
        "30 10 * * 1~5",
        "0 15 * * 1~5",
        timezone = "Asia/Seoul",
    ),
    start_date = pendulum.datetime(2025, 9, 11, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:high", "sabangnet:order", "sabangnet:invoice", "login:sabangnet", "schedule:weekdays", "time:daytime"],
) as dag:

    PATH = ["sabangnet", "admin", "sabangnet_order"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, credentials="expand", tables=True, service_account=True)


    FIRST_SCHEDULE = "09:15"
    LAST_7_DAYS = 7
    YESTERDAY = 1

    @task(task_id="etl_sabangnet_order", pool="sabangnet_pool")
    def etl_sabangnet_order(ti: TaskInstance, data_interval_end: pendulum.DateTime = None, **kwargs) -> dict:
        delta = LAST_7_DAYS if data_interval_end.strftime("%H:%M") == FIRST_SCHEDULE else YESTERDAY
        start_date = data_interval_end.in_timezone("Asia/Seoul").subtract(days=delta)
        end_date = data_interval_end.in_timezone("Asia/Seoul").subtract(days=YESTERDAY)
        return main(start_date=start_date, end_date=end_date, **ti.xcom_pull(task_ids="read_variables"))

    def main(
            userid: str,
            passwd: str,
            domain: int,
            excel_form: int,
            start_date: str,
            end_date: str,
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
                excel_form = excel_form,
                start_date = start_date,
                end_date = end_date,
                date_type = "ord_dt",
                connection = conn,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        start_date = start_date,
                        end_date = end_date,
                        date_type = "ord_dt",
                        excel_form = excel_form,
                    ),
                    count = dict(
                        data = conn.count_table("data"),
                    ),
                    status = dict(
                        data = client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = "data",
                            staging_table = tables["temp_order"],
                            target_table = tables["order"],
                            **merge["order"],
                            where_clause = f"DATE(T.order_dt) BETWEEN '{start_date}' AND '{end_date}'",
                            progress = False,
                        ),
                    ),
                )


    read_variables() >> etl_sabangnet_order()
