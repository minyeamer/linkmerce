from airflow.sdk import DAG, task
import pendulum


with DAG(
    dag_id = "match_catalog",
    schedule = None,
    start_date = pendulum.datetime(2025, 8, 11, tz="Asia/Seoul"),
    catchup = False,
    tags = ["smartstore", "partnercenter", "match_catalog"],
) as dag:

    @task(task_id="brand_ids")
    def brand_ids() -> list[list[str],list[int]]:
        from variables import read_google_sheets
        columns = ["brandIds", "mallSeq"]
        return read_google_sheets(["smartstore","brand","match_catalog"], columns, numericise_ignore=[1])

    @task(task_id="cookies")
    def cookies() -> str:
        from variables import read_cookies
        return read_cookies(["smartstore","brand"])

    @task(task_id="tables")
    def tables() -> dict[str,str]:
        from variables import map_tables
        return map_tables(["smartstore","brand","match_catalog"])

    @task(task_id="schema")
    def schema() -> list[dict]:
        from variables import read_schema
        return read_schema(["smartstore","brand","match_catalog"])

    @task(task_id="service_account")
    def service_account() -> dict:
        from variables import read_service_account
        return read_service_account()


    @task(task_id="etl_match_catalog")
    def etl_match_catalog(
            brand_ids: list[list[str],list[int]],
            cookies: str,
            tables: dict[str,str],
            schema: list[dict],
            service_account: dict,
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.smartstore.brand import match_catalog
        from extensions.bigquery import connect as connect_bigquery
        from extensions.bigquery import load_table_from_duckdb
        from extensions.bigquery import overwrite_table_from_duckdb, TEMP_TABLE

        project_id = service_account["project_id"]

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            match_catalog(cookies, *brand_ids, sort_type="recent", page=None, connection=conn, how="async", return_type="none")

            with connect_bigquery(project_id, service_account) as client:
                return dict(
                    brand_ids = len(brand_ids[0]),
                    data = conn.count_table("data"),
                    status = dict(
                        data = load_table_from_duckdb(client, conn, "data", tables["data"], project_id, schema),
                        cur = overwrite_table_from_duckdb(client, conn, "data", tables["cur"], project_id, "WHERE TRUE", schema),
                        prev = overwrite_table_from_duckdb(client, conn, TEMP_TABLE, tables["prev"], project_id, "WHERE TRUE", schema),
                    )
                )


    etl_match_catalog(brand_ids(), cookies(), tables(), schema(), service_account())
