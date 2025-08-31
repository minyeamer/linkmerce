from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "naver_shopping_page",
    schedule = "0 5 * * *",
    start_date = pendulum.datetime(2025, 8, 18, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(hours=1),
    catchup = False,
    tags = ["priority:high", "naver:rank", "schedule:daily", "time:morning"],
) as dag:

    PATH = ["naver", "main", "naver_shopping_page"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, tables=True, sheets=True, service_account=True)


    @task(task_id="etl_naver_shopping_page")
    def etl_naver_shopping_page(ti: TaskInstance, **kwargs) -> dict:
        return main(**ti.xcom_pull(task_ids="read_variables"))

    def main(
            keyword: list[str],
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.naver.main import shopping_page
        from linkmerce.extensions.bigquery import BigQueryClient

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            shopping_page(keyword[:10], connection=conn, how="async", return_type="none")

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(keyword = len(keyword)),
                    count = dict(data = conn.count_table("data")),
                    status = dict(
                        data = client.merge_into_table_from_duckdb(conn, "data", tables["temp_data"], tables["data"], **merge["data"]),
                    )
                )


    read_variables() >> etl_naver_shopping_page()
