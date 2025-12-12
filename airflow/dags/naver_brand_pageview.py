from airflow.sdk import DAG, task
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "naver_brand_pageview",
    schedule = "50 7 * * *",
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


    @task(task_id="etl_naver_brand_pageview", map_index_template="{{ aggregate_by }}")
    def etl_naver_brand_pageview(aggregate_by: str, variables: dict, **kwargs) -> dict:
        from variables import get_execution_date
        return main(aggregate_by=aggregate_by, date=get_execution_date(kwargs, subdays=1), **variables)

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
        source_table = f"pageview_by_{aggregate_by}"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            page_view(
                cookies = cookies,
                aggregate_by = aggregate_by,
                mall_seq = mall_seq,
                start_date = date,
                end_date = date,
                connection = conn,
                tables = {"default": source_table},
                how = "async",
                progress = False,
                return_type = "none",
            )

            id_column = dict(device="device_type", product="product_id")[aggregate_by]
            conn.execute(f"ALTER TABLE {source_table} RENAME COLUMN {id_column} TO page_id;")

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        aggregate_by = aggregate_by,
                        mall_seq = len(mall_seq),
                        date = date,
                    ),
                    counts = {
                        aggregate_by: conn.count_table(source_table),
                    },
                    status = {
                        aggregate_by: client.load_table_from_duckdb(
                            connection = conn,
                            source_table = source_table,
                            target_table = tables[aggregate_by],
                            progress = False,
                        ),
                    },
                )


    etl_naver_brand_pageview.partial(variables=read_variables()).expand(aggregate_by=["device","product"])
