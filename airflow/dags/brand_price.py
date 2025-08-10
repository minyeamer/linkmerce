from airflow.sdk import DAG, task
import pendulum


with DAG(
    dag_id = "brand_price",
    schedule = "1 0 * * *",
    start_date = pendulum.datetime(2023, 7, 19, tz="Asia/Seoul"),
    catchup = False,
    tags = ["smartstore", "partnercenter", "brand_price"],
) as dag:

    @task(task_id="brand_ids")
    def brand_ids() -> list[list[str],list[int]]:
        from variables import read_google_sheets
        columns = ["brandIds", "mallSeq"]
        return read_google_sheets(["smartstore","brand","brand_price"], columns, numericise_ignore=[1])

    @task(task_id="cookies")
    def cookies() -> str:
        from variables import read_cookies
        return read_cookies(["smartstore","brand"])

    @task(task_id="tables")
    def tables() -> dict[str,str]:
        from variables import map_tables
        return map_tables(["smartstore","brand","brand_price"])

    @task(task_id="schemas")
    def schemas() -> dict[str,list[dict]]:
        from variables import map_schemas
        return map_schemas(["smartstore","brand","brand_price"])

    @task(task_id="params")
    def params() -> tuple[list,dict]:
        from variables import read_params
        params_ = read_params(["smartstore","brand","product"])
        return params_["by"], params_["agg"]

    @task(task_id="service_account")
    def service_account() -> dict:
        from variables import read_service_account
        return read_service_account()


    @task(task_id="etl_brand_price")
    def etl_brand_price(
            brand_ids: list[list[str],list[int]],
            cookies: str,
            tables: dict[str,str],
            schemas: dict[str,list[dict]],
            groupby: tuple[list,dict],
            service_account: dict,
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.smartstore.brand import brand_price
        from extensions.bigquery import connect as connect_bigquery
        from extensions.bigquery import load_table_from_duckdb
        from extensions.bigquery import upsert_table_from_duckdb

        project_id = service_account["project_id"]

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            brand_price(cookies, *brand_ids, sort_type="recent", page=None, connection=conn, how="async", return_type="none")

            with connect_bigquery(project_id, service_account) as client:
                price_args = (client, conn, "data", tables["data"], project_id, schemas["data"])
                product_args = (client, conn, "product", tables["product"], project_id, *groupby, "WHERE TRUE", schemas["product"])

                return dict(
                    brand_ids = len(brand_ids[0]),
                    count = dict(price=conn.count_table("data"), product=conn.count_table("product")),
                    status = dict(price=load_table_from_duckdb(*price_args), product=upsert_table_from_duckdb(*product_args)),
                )


    etl_brand_price(brand_ids(), cookies(), tables(), schemas(), params(), service_account())
