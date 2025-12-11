from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "sabangnet_order",
    schedule = None, # triggered by API request (managed by human)
    start_date = None,
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:high", "sabangnet:order", "login:sabangnet", "schedule:weekdays", "time:daytime", "manual:api"],
) as dag:

    PATH = ["sabangnet", "admin", "sabangnet_order"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, credentials="expand", tables=True, service_account=True)

    def get_order_date_pair(
            data_interval_start: pendulum.DateTime,
            data_interval_end: pendulum.DateTime,
            **kwargs
        ) -> dict[str,str]:
        from variables import strftime
        return dict(
            start_date = strftime(data_interval_start, format="%Y%m%d%H%M%S"),
            end_date = strftime(data_interval_end, format="%Y%m%d%H%M%S"),
        )


    @task(task_id="etl_sabangnet_order", pool="sabangnet_pool")
    def etl_sabangnet_order(ti: TaskInstance, **kwargs) -> dict:
        return main(download_type="order", **get_order_date_pair(**kwargs), **ti.xcom_pull(task_ids="read_variables"))

    @task(task_id="etl_sabangnet_dispatch", pool="sabangnet_pool")
    def etl_sabangnet_dispatch(ti: TaskInstance, **kwargs) -> dict:
        return main(download_type="dispatch", **get_order_date_pair(**kwargs), **ti.xcom_pull(task_ids="read_variables"))

    @task(task_id="etl_sabangnet_option", pool="sabangnet_pool")
    def etl_sabangnet_option(ti: TaskInstance, **kwargs) -> dict:
        return main(download_type="option", **get_order_date_pair(**kwargs), **ti.xcom_pull(task_ids="read_variables"))

    def main(
            userid: str,
            passwd: str,
            domain: int,
            download_no: dict[str,int],
            download_type: str,
            start_date: str,
            end_date: str,
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            date_type: str = "reg_dm",
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
                download_no = download_no[download_type],
                download_type = download_type,
                start_date = start_date,
                end_date = end_date,
                date_type = date_type,
                connection = conn,
                return_type = "none",
            )

            if download_type == "order":
                query = "SELECT DISTINCT DATE(order_dt) FROM data"
                order_dates = sorted([f"'{date[0]}'" for date in conn.execute(query).fetchall()])
                where_clause = f"DATE(T.order_dt) IN ({', '.join(order_dates)})"
            elif download_type == "dispatch":
                query = "SELECT MIN(DATE(register_dt)), MAX(DATE(register_dt)) FROM data"
                start_date, end_date = map(str, conn.execute(query).fetchall()[0])
                if start_date == end_date:
                    where_clause = f"DATE(T.register_dt) = '{start_date}'"
                else:
                    where_clause = f"DATE(T.register_dt) BETWEEN '{start_date}' AND '{end_date}'"
            else:
                where_clause = None

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        start_date = start_date,
                        end_date = end_date,
                        date_type = date_type,
                        download_no = download_no[download_type],
                        download_type = download_type,
                    ),
                    counts = {
                        download_type: conn.count_table("data"),
                    },
                    status = {
                        download_type: client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = "data",
                            staging_table = tables[f"temp_{download_type}"],
                            target_table = tables[download_type],
                            **merge[download_type],
                            where_clause = where_clause,
                            progress = False,
                        ),
                    },
                )


    read_variables() >> [etl_sabangnet_order(), etl_sabangnet_dispatch(), etl_sabangnet_option()]
