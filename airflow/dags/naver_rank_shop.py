from airflow.sdk import DAG, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "naver_rank_shop",
    schedule = "0 6-18 * * *",
    start_date = pendulum.datetime(2025, 8, 15, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=55),
    catchup = False,
    tags = ["priority:high", "naver:rank", "naver:product", "api:naver-openapi", "schedule:hourly", "time:daytime"],
) as dag:

    PATH = ["naver", "openapi", "naver_rank_shop"]

    @task(task_id="read_variables")
    def read_variables() -> dict:
        from variables import read
        return read(PATH, tables=True, service_account=True)

    @task(task_id="read_queries")
    def read_queries() -> list:
        from variables import read, split_by_credentials
        variables = read(PATH, credentials=True, tables=True, sheets=True)
        return split_by_credentials(variables["credentials"], keyword=variables["keyword"])


    @task(task_id="init_staging_table")
    def init_staging_table(variables: dict):
        from linkmerce.extensions.bigquery import BigQueryClient
        tables = variables["tables"]

        with BigQueryClient(variables["service_account"]) as client:
            for staging_table in [tables["temp_now"], tables["temp_product"]]:
                if client.table_exists(staging_table):
                    client.execute_job(f"DROP TABLE `{client.project_id}.{staging_table}`;")


    CHUNK = 100

    @task(task_id="etl_naver_rank_shop", pool="naver_rank_shop")
    def etl_naver_rank_shop(queries: dict, variables: dict) -> dict:
        keyword = queries.pop("keyword")
        return main(keyword, queries, **variables)

    def main(
            keyword: list[str],
            credentials: dict,
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.naver.openapi import rank_shop
        from linkmerce.extensions.bigquery import BigQueryClient

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            rank_shop(**credentials, query=keyword, start=[1,101,201], sort="sim", connection=conn, how="async", return_type="none")

            with BigQueryClient(service_account) as client:
                timeout = dict(table_lock_wait_interval=3, table_lock_wait_timeout=30, if_staging_table_exists="errors")

                return dict(
                    params = dict(
                        query = len(keyword),
                        start = [1,101,201],
                        sort = "sim",
                    ),
                    count = dict(
                        rank = conn.count_table("data"),
                        product = conn.count_table("product"),
                    ),
                    status = dict(
                        rank = client.load_table_from_duckdb(conn, "data", tables["data"]),
                        now = client.merge_into_table_from_duckdb(conn, "data", tables["temp_now"], tables["now"], **merge["now"], **timeout),
                        product = client.merge_into_table_from_duckdb(conn, "product", tables["temp_product"], tables["product"], **merge["product"], **timeout),
                    )
                )


    naver_product_catalog = TriggerDagRunOperator(
        task_id = "naver_product_catalog",
        trigger_dag_id = "naver_product_catalog",
        trigger_run_id = None,
        trigger_rule = TriggerRule.ONE_SUCCESS,
        logical_date = "{{data_interval_start}}",
        reset_dag_run = True,
        wait_for_completion = False,
        poke_interval = 60,
        allowed_states = ["success"],
        failed_states = None,
    )


    variables = read_variables()
    init_staging_table(variables) >> etl_naver_rank_shop.partial(variables=variables).expand(queries=read_queries()) >> naver_product_catalog
