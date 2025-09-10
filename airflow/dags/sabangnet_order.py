from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "sabangnet_order",
    schedule = "50 8 * * *",
    start_date = pendulum.datetime(2025, 9, 11, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:high", "sabangnet:order", "login:sabangnet", "schedule:daily", "time:morning"],
) as dag:

    PATH = ["sabangnet", "admin", "sabangnet_order"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, credentials="expand", tables=True, service_account=True)


    @task(task_id="etl_sabangnet_order")
    def etl_sabangnet_order(ti: TaskInstance, data_interval_end: pendulum.DateTime = None, **kwargs) -> dict:
        date = str(data_interval_end.in_timezone("Asia/Seoul").subtract(days=1).date())
        return main(date=date, **ti.xcom_pull(task_ids="read_variables"))

    def main(
            userid: str,
            passwd: str,
            domain: int,
            excel_form: int,
            date: str,
            service_account: dict,
            tables: dict[str,str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.sabangnet.admin import order_download
        from linkmerce.extensions.bigquery import BigQueryClient

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            order_download(userid, passwd, domain, excel_form, date, date_type="ord_dt", connection=conn, return_type="none")

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        date = date,
                        date_type = "ord_dt",
                        excel_form = excel_form,
                    ),
                    count = dict(
                        order = conn.count_table("data"),
                    ),
                    status = dict(
                        order = client.load_table_from_duckdb(conn, "data", tables["order"]),
                    )
                )


    read_variables() >> etl_sabangnet_order()
