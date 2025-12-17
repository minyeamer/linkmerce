from airflow.sdk import DAG, task
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "coupang_option",
    schedule = "40 9 * * *",
    start_date = pendulum.datetime(2025, 11, 4, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:medium", "coupang:option", "login:coupang", "schedule:daily", "time:morning"],
) as dag:

    PATH = ["coupang", "wing", "coupang_option"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from variables import read
        return read(PATH, credentials=True)["credentials"]


    @task(task_id="etl_coupang_option", map_index_template="{{ credentials['vendor_id'] }}", pool="coupang_pool")
    def etl_coupang_option(credentials: dict, variables: dict, **kwargs) -> dict:
        return main(**credentials, **variables)

    def main(
            cookies: str,
            vendor_id: str,
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.wing import product_option, rocket_option
        from linkmerce.extensions.bigquery import BigQueryClient

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            for is_deleted in [False, True]:
                product_option(
                    cookies = cookies,
                    is_deleted = is_deleted,
                    see_more = True,
                    connection = conn,
                    tables = {"default": "vendor"},
                    progress = False,
                    return_type = "none",
                )

            rocket_option(
                cookies = cookies,
                hidden_status = None,
                vendor_id = vendor_id,
                see_more = True,
                connection = conn,
                tables = {"default": "rfm"},
                progress = False,
                return_type = "none",
            )

            if conn.table_has_rows("rfm"):
                conn.execute(RFM_INSERT_QUERY)

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        vendor_id = vendor_id,
                        is_deleted = [False, True],
                        see_more = True,
                    ),
                    counts = dict(
                        vendor = conn.count_table("vendor"),
                        rfm = conn.count_table("rfm"),
                    ),
                    status = dict(
                        data = client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = "vendor",
                            staging_table = f'{tables["temp_option"]}_{vendor_id}',
                            target_table = tables["option"],
                            **merge["option"],
                            progress = False,
                        ),
                    ),
                )

    RFM_INSERT_QUERY = """
    INSERT INTO vendor (
        vendor_inventory_id
        , vendor_inventory_item_id
        , product_id
        , option_id
        , item_id
        , barcode
        , vendor_id
        , product_name
        , option_name
        , display_category_id
        , category_id
        , category_name
        , product_status
        , price
        , sales_price
        , order_quantity
        , stock_quantity
        , register_dt
    )
    SELECT * FROM rfm ON CONFLICT DO NOTHING;
    """


    etl_coupang_option.partial(variables=read_variables()).expand(credentials=read_credentials())
