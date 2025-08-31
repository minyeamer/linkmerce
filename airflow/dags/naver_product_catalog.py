from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "naver_product_catalog",
    schedule = None, # triggered by rank_shop (0 6-18 * * *)
    start_date = pendulum.datetime(2025, 8, 15, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:high", "naver:rank", "login:partnercenter", "schedule:hourly", "time:daytime", "triggered:true"],
) as dag:

    PATH = ["smartstore", "brand", "naver_product_catalog"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, credentials="expand", tables=True, sheets=True, service_account=True)


    @task(task_id="etl_product_catalog")
    def etl_product_catalog(ti: TaskInstance, **kwargs) -> dict:
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
        from linkmerce.api.smartstore.brand import product_catalog
        from linkmerce.extensions.bigquery import BigQueryClient

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            product_catalog(cookies, brand_ids, mall_seq, sort_type="recent", page=None, connection=conn, how="async", return_type="none")

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        brand_ids = len(brand_ids),
                        sort_type = "recent",
                        page = None,
                    ),
                    count = dict(
                        data = conn.count_table("data")
                    ),
                    status = dict(
                        data = client.load_table_from_duckdb(conn, "data", tables["data"]),
                        now = client.merge_into_table_from_duckdb(conn, "data", tables["temp_now"], tables["now"], **merge["now"]),
                    )
                )


    read_variables() >> etl_product_catalog()
