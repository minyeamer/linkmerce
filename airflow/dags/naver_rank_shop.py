from airflow.sdk import DAG, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.task.trigger_rule import TriggerRule
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

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, tables=True, service_account=True)

    @task(task_id="read_queries", retries=3, retry_delay=timedelta(minutes=1))
    def read_queries() -> list:
        from variables import read, split_by_credentials
        variables = read(PATH, credentials=True, tables=True, sheets=True)
        queries = split_by_credentials(variables["credentials"], keyword=variables["keyword"])
        return [dict(query, seq=i) for i, query in enumerate(queries)]


    CHUNK = 100

    @task(task_id="etl_naver_rank_shop")
    def etl_naver_rank_shop(queries: dict, variables: dict) -> dict:
        return main(**queries, **variables)

    def main(
            client_id: str,
            client_secret: str,
            keyword: list[str],
            seq: int,
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.naver.openapi import rank_shop
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = dict(rank="naver_rank_shop", product="naver_product")

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            rank_shop(
                client_id = client_id,
                client_secret = client_secret,
                query = keyword,
                start = [1, 101, 201],
                sort = "sim",
                connection = conn,
                tables = sources,
                how = "async",
                progress = False,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        query = len(keyword),
                        start = [1, 101, 201],
                        sort = "sim",
                    ),
                    count = dict(
                        rank = conn.count_table(sources["rank"]),
                        product = conn.count_table(sources["product"]),
                    ),
                    status = dict(
                        rank = client.load_table_from_duckdb(
                            connection = conn,
                            source_table = sources["rank"],
                            target_table = tables["rank"],
                            progress = False,
                        ),
                        now = client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["rank"],
                            staging_table = f'{tables["temp_now"]}_{seq}',
                            target_table = tables["now"],
                            **merge["now"],
                            progress = False,
                        ),
                        product = client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["product"],
                            staging_table = f'{tables["temp_product"]}_{seq}',
                            target_table = tables["product"],
                            **merge["product"],
                            progress = False,
                        ),
                    ),
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


    etl_naver_rank_shop.partial(variables=read_variables()).expand(queries=read_queries()) >> naver_product_catalog
