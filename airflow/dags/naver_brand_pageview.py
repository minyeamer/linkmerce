from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "naver_brand_pageview",
    schedule = "0 10 * * *",
    start_date = pendulum.datetime(2025, 12, 13, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    tags = ["priority:low", "naver:pageview", "login:partnercenter", "schedule:daily", "time:morning"],
) as dag:

    PATH = ["smartstore", "brand", "naver_brand_pageview"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, credentials="expand", tables=True, sheets=True, service_account=True)


    @task(task_id="etl_naver_device_pageview")
    def etl_naver_device_pageview(ti: TaskInstance, **kwargs) -> dict:
        from variables import get_execution_date
        date = get_execution_date(kwargs, subdays=1)
        return main(aggregate_by="device", date=date, **ti.xcom_pull(task_ids="read_variables"))

    @task(task_id="etl_naver_product_pageview")
    def etl_naver_product_pageview(ti: TaskInstance, **kwargs) -> dict:
        from variables import get_execution_date
        date = get_execution_date(kwargs, subdays=1)
        return main(aggregate_by="product", date=date, **ti.xcom_pull(task_ids="read_variables"))

    def main(
            cookies: str,
            aggregate_by: str,
            mall_seq: list[int],
            date: str,
            service_account: dict,
            tables: dict[str,str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.smartstore.brand import page_view
        from linkmerce.extensions.bigquery import BigQueryClient

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            page_view(
                cookies = cookies,
                aggregate_by = aggregate_by,
                mall_seq = mall_seq,
                start_date = date,
                end_date = date,
                connection = conn,
                how = "async",
                progress = False,
                return_type = "none",
            )

            id_column = dict(device="device_type", product="product_id")[aggregate_by]
            conn.execute(f"ALTER TABLE data RENAME COLUMN {id_column} TO page_id;")

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        aggregate_by = aggregate_by,
                        mall_seq = len(mall_seq),
                        date = date,
                    ),
                    counts = {
                        aggregate_by: conn.count_table("data"),
                    },
                    status = {
                        aggregate_by: client.load_table_from_duckdb(
                            connection = conn,
                            source_table = "data",
                            target_table = tables[aggregate_by],
                            progress = False,
                        ),
                    },
                )


    read_variables() >> etl_naver_device_pageview() >> etl_naver_product_pageview()
