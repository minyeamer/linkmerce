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

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, tables=True, service_account=True)

    @task(task_id="read_queries", retries=3, retry_delay=timedelta(minutes=1))
    def read_queries() -> list:
        from variables import read, split_by_credentials
        variables = read(PATH, credentials=True, tables=True, sheets=True)
        credentials = filter_valid_accounts(variables["credentials"])
        return split_by_credentials(credentials, keyword=variables["keyword"])

    def filter_valid_accounts(credentials: list[dict]) -> list[dict]:
        from linkmerce.api.searchad.manage import has_permission
        accounts = [credential for credential in credentials if has_permission(**credential)]
        if credentials and (not accounts):
            from airflow.exceptions import AirflowFailException
            raise AirflowFailException("At least one account does not have permissions")
        return accounts


    CHUNK = 100

    @task(task_id="etl_naver_rank_ad", map_index_template="{{ queries['customer_id'] }}")
    def etl_naver_rank_ad(queries: dict, variables: dict) -> list[dict]:
        keywords = queries.pop("keyword")
        return [main(**queries, keyword=keywords[i:i+CHUNK], **variables, seq=(i//CHUNK)) for i in range(0, len(keywords), CHUNK)]

    def main(
            customer_id: int | str,
            cookies: str,
            keyword: list[str],
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            seq: int = 0,
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.searchad.manage import rank_exposure
        from linkmerce.extensions.bigquery import BigQueryClient
        import logging
        import time
        sources = dict(rank="naver_rank_ad", product="naver_product")

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            start_time = time.time()
            rank_exposure(
                customer_id = customer_id,
                cookies = cookies,
                keyword = keyword,
                domain = "search",
                mobile = True,
                connection = conn,
                tables = sources,
                progress = False,
                return_type = "none",
            )
            end_time = time.time()
            minutes, seconds = map(int, divmod(end_time - start_time, 60))
            logging.info(f"[{seq}] API request completed for searching {len(keyword)} keywords in {minutes:02d}:{seconds:02d}")

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        customer_id = customer_id,
                        keyword = len(keyword),
                        domain = "search",
                        mobile = True,
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
                            staging_table = f'{tables["temp_now"]}_{customer_id}',
                            target_table = tables["now"],
                            **merge["now"],
                            progress = False,
                        ),
                        product = client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["product"],
                            staging_table = f'{tables["temp_product"]}_{customer_id}',
                            target_table = tables["product"],
                            **merge["product"],
                            progress = False,
                        ),
                    ),
                )


    etl_naver_rank_ad.partial(variables=read_variables()).expand(queries=read_queries())
