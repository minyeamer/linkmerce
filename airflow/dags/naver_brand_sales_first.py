from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "naver_brand_sales_first",
    schedule = "50 8 * * *",
    start_date = pendulum.datetime(2025, 8, 15, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    tags = ["priority:high", "naver:sales", "naver:product", "login:partnercenter", "schedule:daily", "time:morning"],
) as dag:

    PATH = ["smartstore", "brand", "naver_brand_sales"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, credentials="expand", tables=True, sheets=True, service_account=True)


    @task(task_id="etl_brand_sales")
    def etl_brand_sales(ti: TaskInstance, data_interval_end: pendulum.DateTime = None, **kwargs) -> dict:
        start_date = str(data_interval_end.in_timezone("Asia/Seoul").subtract(days=2).date())
        end_date = str(data_interval_end.in_timezone("Asia/Seoul").subtract(days=1).date())
        return main(start_date=start_date, end_date=end_date, **ti.xcom_pull(task_ids="read_variables"))

    def main(
            mall_seq: list[int],
            start_date: str,
            end_date: str,
            cookies: str,
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.smartstore.brand import aggregated_sales
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = dict(sales="naver_brand_sales", product="naver_brand_product")

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            options = dict(transform_options = dict(tables = sources))
            aggregated_sales(cookies, mall_seq, start_date, end_date, connection=conn, how="async", return_type="none", **options)

            with BigQueryClient(service_account) as client:
                on = f"payment_date BETWEEN '{start_date}' AND '{end_date}'"

                return dict(
                    params = dict(
                        mall_seq = len(mall_seq),
                        start_date = start_date,
                        end_date = end_date,
                    ),
                    count = dict(
                        sales = conn.count_table(sources["sales"]),
                        product = conn.count_table(sources["product"]),
                    ),
                    status = dict(
                        sales = client.merge_into_table_from_duckdb(conn, sources["sales"], tables["temp_sales"], tables["sales"], where_clause=on, **merge["sales"]),
                        product = client.merge_into_table_from_duckdb(conn, sources["product"], tables["temp_product"], tables["product"], **merge["product"]),
                    )
                )


    read_variables() >> etl_brand_sales()
