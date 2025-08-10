from airflow.sdk import DAG, task
import pendulum


with DAG(
    dag_id = "brand_sales_next",
    schedule = "10,30,50 9,10 * * *",
    start_date = pendulum.datetime(2023, 7, 20, tz="Asia/Seoul"),
    catchup = False,
    tags = ["smartstore", "partnercenter", "brand_sales"],
) as dag:

    @task(task_id="mall_seq")
    def mall_seq() -> list[int]:
        from variables import read_google_sheets
        return read_google_sheets(["smartstore","brand","brand_sales"], "mallSeq")

    @task(task_id="cookies")
    def cookies() -> str:
        from variables import read_cookies
        return read_cookies(["smartstore","brand"])

    @task(task_id="tables")
    def tables() -> dict[str,str]:
        from variables import map_tables
        return map_tables(["smartstore","brand","brand_sales"])

    @task(task_id="schemas")
    def schemas() -> dict[str,list[dict]]:
        from variables import map_schemas
        return map_schemas(["smartstore","brand","brand_sales"])

    @task(task_id="params")
    def params() -> tuple[list,dict]:
        from variables import read_params
        params_ = read_params(["smartstore","brand","product"])
        return params_["by"], params_["agg"]

    @task(task_id="service_account")
    def service_account() -> dict:
        from variables import read_service_account
        return read_service_account()


    @task(task_id="etl_brand_sales")
    def etl_brand_sales(
            mall_seq: list[str],
            cookies: str,
            tables: dict[str,str],
            schemas: dict[str,list[dict]],
            groupby: tuple[list,dict],
            service_account: dict,
            data_interval_end: pendulum.DateTime = None,
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.smartstore.brand import aggregated_sales
        from extensions.bigquery import connect as connect_bigquery
        from extensions.bigquery import overwrite_table_from_duckdb
        from extensions.bigquery import upsert_table_from_duckdb

        date = str(data_interval_end.subtract(days=1).date())
        project_id = service_account["project_id"]

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            aggregated_sales(cookies, mall_seq, date, date, connection=conn, how="async", return_type="none")

            with connect_bigquery(project_id, service_account) as client:
                condition = f"WHERE paymentDate = '{date}'"
                partition_by = dict(by=["paymentDate"])
                sales_args = (client, conn, "data", tables["data"], project_id, condition, schemas["data"], partition_by)
                product_args = (client, conn, "product", tables["product"], project_id, *groupby, "WHERE TRUE", schemas["product"])

                return dict(
                    mall_seq = len(mall_seq),
                    date = date,
                    count = dict(sales=conn.count_table("data"), product=conn.count_table("product")),
                    status = dict(sales=overwrite_table_from_duckdb(*sales_args), product=upsert_table_from_duckdb(*product_args)),
                )


    etl_brand_sales(mall_seq(), cookies(), tables(), schemas(), params(), service_account())
