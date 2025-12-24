from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "sabangnet_order",
    schedule = "30 23 * * *", # triggered by API request (managed by human)
    start_date = pendulum.datetime(2025, 9, 11, tz="Asia/Seoul"),
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
            fmt: str = "YYYYMMDDHHmmss",
            **kwargs
        ) -> dict[str,str]:
        from variables import format_date
        return dict(
            start_date = format_date(data_interval_start, fmt),
            end_date = format_date(data_interval_end, fmt),
        )


    @task(task_id="etl_sabangnet_order", pool="sabangnet_pool")
    def etl_sabangnet_order(ti: TaskInstance, **kwargs) -> dict:
        kwargs["fmt"] = "YYYYMMDDHHmmss" if ti.run_id.startswith("api__") else "YYYYMMDD"
        return main(download_type="order", **get_order_date_pair(**kwargs), **ti.xcom_pull(task_ids="read_variables"))

    @task(task_id="etl_sabangnet_dispatch", pool="sabangnet_pool")
    def etl_sabangnet_dispatch(ti: TaskInstance, **kwargs) -> dict:
        kwargs["fmt"] = "YYYYMMDDHHmmss" if ti.run_id.startswith("api__") else "YYYYMMDD"
        return main(download_type="dispatch", **get_order_date_pair(**kwargs), **ti.xcom_pull(task_ids="read_variables"))

    @task(task_id="etl_sabangnet_option", pool="sabangnet_pool")
    def etl_sabangnet_option(ti: TaskInstance, **kwargs) -> dict:
        kwargs["fmt"] = "YYYYMMDDHHmmss" if ti.run_id.startswith("api__") else "YYYYMMDD"
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
                date_column, date_array = "DATE(T.order_dt)", conn.unique("data", "DATE(order_dt)")
            elif download_type == "dispatch":
                date_column, date_array = "DATE(T.register_dt)", conn.unique("data", "DATE(register_dt)")
            else:
                date_column, date_array = None, None

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
                    **(dict(dates = {
                        download_type: sorted(map(str, date_array))
                    }) if date_column else dict()),
                    status = {
                        download_type: (client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = "data",
                            staging_table = tables[f"temp_{download_type}"],
                            target_table = tables[download_type],
                            **merge[download_type],
                            where_clause = (conn.expr_date_range(date_column, date_array) if date_column else None),
                            progress = False,
                        ) if (not date_column) or date_array else True),
                    },
                )


    def branch_condition(ti: TaskInstance, **kwargs) -> str | None:
        if ti.run_id.startswith("api__1st__"):
            return "ecount_stock_report"
        else:
            return None

    branch_dagrun_trigger = BranchPythonOperator(
        task_id = "branch_dagrun_trigger",
        python_callable = branch_condition,
    )


    ecount_stock_report = TriggerDagRunOperator(
        task_id = "ecount_stock_report",
        trigger_dag_id = "ecount_stock_report",
        trigger_run_id = "{{ run_id }}",
        logical_date = "{{ logical_date }}",
        reset_dag_run = True,
        wait_for_completion = False,
    )


    (read_variables()
    >> etl_sabangnet_order() >> etl_sabangnet_dispatch() >> etl_sabangnet_option()
    >> branch_dagrun_trigger >> [ecount_stock_report])
