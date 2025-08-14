from airflow.sdk import DAG, task
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "naver_rank_ad",
    schedule = "0 6-18 * * *",
    start_date = pendulum.datetime(2025, 8, 15, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=55),
    catchup = False,
    tags = ["priority:high", "naver:rank", "naver:product", "login:searchad", "schedule:hourly", "time:daytime"],
) as dag:

    PATH = ["searchad", "manage", "naver_rank_ad"]

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

    @task(task_id="etl_naver_rank_ad", pool="naver_rank_ad")
    def etl_naver_rank_ad(queries: dict, variables: dict) -> list:
        keywords = queries.pop("keyword")
        return [main(keywords[i:i+CHUNK], queries, **variables) for i in range(0, len(keywords), CHUNK)]

    def main(
            keyword: list[str],
            credentials: dict,
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.searchad.manage import rank_exposure
        from linkmerce.extensions.bigquery import BigQueryClient

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            rank_exposure(**credentials, keyword=keyword, domain="search", mobile=True, connection=conn, return_type="none")

            with BigQueryClient(service_account) as client:
                timeout = dict(table_lock_wait_interval=3, table_lock_wait_timeout=30, if_staging_table_exists="errors")

                return dict(
                    params = dict(
                        keyword = len(keyword),
                        domain = "search",
                        mobile = True,
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

    variables = read_variables()
    init_staging_table(variables) >> etl_naver_rank_ad.partial(variables=variables).expand(queries=read_queries())
