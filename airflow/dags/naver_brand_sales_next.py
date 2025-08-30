from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "naver_brand_sales_next",
    schedule = "10,30,50 9,10 * * *",
    start_date = pendulum.datetime(2025, 8, 15, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    tags = ["priority:high", "naver:sales", "naver:product", "login:partnercenter", "schedule:daily-incremental", "time:morning"],
) as dag:

    PATH = ["smartstore", "brand", "naver_brand_sales"]

    @task(task_id="read_variables")
    def read_variables() -> dict:
        from variables import read
        return read(PATH, credentials="expand", tables=True, sheets=True, service_account=True)


    @task(task_id="etl_naver_brand_sales")
    def etl_naver_brand_sales(ti: TaskInstance, data_interval_end: pendulum.DateTime = None, **kwargs) -> dict:
        date = str(data_interval_end.in_timezone("Asia/Seoul").subtract(days=1).date())
        return main(date=date, **ti.xcom_pull(task_ids="read_variables"))

    def main(
            mall_seq: list[int],
            date: str,
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
            aggregated_sales(cookies, mall_seq, date, connection=conn, how="async", return_type="none", **options)

            with BigQueryClient(service_account) as client:
                on = f"payment_date = '{date}'"

                return dict(
                    params = dict(
                        mall_seq = len(mall_seq),
                        date = date,
                    ),
                    count = dict(
                        sales = conn.count_table(sources["sales"]),
                        product = conn.count_table(sources["product"]),
                    ),
                    status = dict(
                        sales = client.merge_into_table_from_duckdb(conn, sources["sales"], tables["temp_data"], tables["data"], where_clause=on, **merge["data"]),
                        product = client.merge_into_table_from_duckdb(conn, sources["product"], tables["temp_product"], tables["product"], **merge["product"]),
                    )
                )


    read_variables() >> etl_naver_brand_sales()
