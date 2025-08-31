from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "naver_brand_price",
    schedule = "1 0 * * *",
    start_date = pendulum.datetime(2025, 8, 15, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(hours=1),
    catchup = False,
    tags = ["priority:medium", "naver:price", "naver:product", "login:partnercenter", "schedule:daily", "time:night"],
) as dag:

    PATH = ["smartstore", "brand", "naver_brand_price"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, credentials="expand", tables=True, sheets=True, service_account=True)


    @task(task_id="etl_naver_brand_price")
    def etl_naver_brand_price(ti: TaskInstance, **kwargs) -> dict:
        return main(**ti.xcom_pull(task_ids="read_variables"))

    def main(
            brand_ids: list[str],
            mall_seq: list[int],
            cookies: str,
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.smartstore.brand import brand_price
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = dict(price="naver_brand_price", product="naver_brand_product")

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            options = dict(transform_options = dict(tables = sources))
            brand_price(cookies, brand_ids, mall_seq, sort_type="recent", page=None, connection=conn, how="async", return_type="none", **options)

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        brand_ids = len(brand_ids),
                        sort_type = "recent",
                        page = None,
                    ),
                    count = dict(
                        price = conn.count_table("naver_brand_price"),
                        product = conn.count_table("naver_brand_product"),
                    ),
                    status = dict(
                        price = client.load_table_from_duckdb(conn, sources["price"], tables["price"]),
                        product = client.merge_into_table_from_duckdb(conn, sources["product"], tables["temp_product"], tables["product"], **merge["product"]),
                    )
                )


    read_variables() >> etl_naver_brand_price()
