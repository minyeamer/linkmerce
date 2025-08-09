from airflow.sdk import DAG, Variable, task
from airflow.models.taskinstance import TaskInstance
import pendulum


with DAG(
    dag_id = "rank_shop",
    schedule = "0 6-18 * * *",
    start_date = pendulum.datetime(2025, 8, 10, tz="Asia/Seoul"),
    catchup = False,
    tags = ["naver", "openapi", "rank"],
) as dag:

    @task(task_id="read_tables")
    def read_tables() -> dict[str,str]:
        import json
        return json.loads(Variable.get("table_info"))["rank_shop"]

    def _pull_tables(ti: TaskInstance, **kwargs) -> dict[str,str]:
        return ti.xcom_pull(task_ids="read_tables")


    @task(task_id="get_query")
    def get_query_with_client() -> list[dict]:
        from extensions.gsheets import get_all_records
        import json
        sheet_name = json.loads(Variable.get("sheet_info"))["rank_shop"]
        rows = get_all_records(Variable.get("gspread_key"), sheet_name, account=Variable.get("service_account"), numericise_ignore=[1])

        client_info = [dict(zip(["client_id","client_secret"],info)) for info in json.loads(Variable.get("naver_openapi"))]
        n_split = list(range(0, len(rows), len(rows)//len(client_info)))[:len(client_info)]
        return [dict(client_info[i], query=[row["query"] for row in rows[start:end]])
                for i, (start, end) in enumerate(zip(n_split, n_split[1:]+[None]))]

    @task(task_id="etl_rank", pool="nshopping_rank_pool")
    def etl_rank(client_and_query: dict, **kwargs):
        table_info = _pull_tables(**kwargs)

        from linkmerce.api.naver.openapi import rank_shop
        from linkmerce.common.load import DuckDBConnection
        from extensions.bigquery import connect as connect_bigquery
        from extensions.bigquery import load_table_from_duckdb
        from extensions.bigquery import upsert_table_from_duckdb
        import time

        with DuckDBConnection() as conn:
            conn.execute("SET TimeZone = 'Asia/Seoul';")
            rank_shop(**client_and_query, start=[1,101,201], connection=conn, how="async", return_type="none")

            project_id = Variable.get("project_id")
            with connect_bigquery(project_id, Variable.get("service_account")) as client:
                load_table_from_duckdb(client, conn, "data", table_info["data"], project_id, _rank_schema())

                condition = "WHERE TRUE"
                upsert_table_from_duckdb(client, conn, "product", table_info["product"], project_id, *_product_groupby(), condition, _product_schema())

        time.sleep(1)

    def _rank_schema() -> list:
        from extensions.bigquery import to_bigquery_schema
        return to_bigquery_schema([
            {"name":"keyword", "type":"STRING", "description":"키워드"},
            {"name":"nvMid", "type":"INTEGER", "description":"쇼핑상품ID", "mode":"REQUIRED"},
            {"name":"mallPid", "type":"INTEGER", "description":"상품코드"},
            {"name":"productType", "type":"INTEGER", "description":"상품종류"},
            {"name":"displayRank", "type":"INTEGER", "description":"노출순위"},
            {"name":"createdAt", "type":"DATETIME", "description":"수집일시", "mode":"REQUIRED"}
        ])

    def _product_schema() -> list:
        from extensions.bigquery import to_bigquery_schema
        return to_bigquery_schema([
            {"name":"nvMid", "type":"INTEGER", "description":"쇼핑상품ID", "mode":"REQUIRED"},
            {"name":"mallPid", "type":"INTEGER", "description":"판매처코드"},
            {"name":"productName", "type":"STRING", "description":"상품명"},
            {"name":"mallName", "type":"STRING", "description":"판매처명"},
            {"name":"brandName", "type":"STRING", "description":"브랜드명"},
            {"name":"updatedAt", "type":"DATETIME", "description":"갱신일시"}
        ])

    def _product_groupby() -> tuple[list[str], dict[str,str]]:
        by = ["nvMid"]
        agg = {
            "mallPid": "first",
            "productName": "first",
            "mallName": "first",
            "brandName": "first",
            "updatedAt": "max"
        }
        return by, agg


    @task(task_id="get_brand_ids")
    def get_brand_ids() -> tuple[list[int],list[int]]:
        from extensions.gsheets import get_all_records
        import json
        sheet_name = json.loads(Variable.get("sheet_info"))["match_catalog"]
        rows = get_all_records(Variable.get("gspread_key"), sheet_name, account=Variable.get("service_account"), numericise_ignore=[1])
        return [row["brandIds"] for row in rows], [row["mallSeq"] for row in rows]

    @task(task_id="read_cookies")
    def read_cookies() -> str:
        with open(Variable.get("cookies_partnercenter"), 'r', encoding="utf-8") as file:
            return file.read().strip()

    @task(task_id="etl_match")
    def etl_match(brand_ids: tuple[list[str],list[int]], cookies: str, **kwargs):
        table_info = _pull_tables(**kwargs)

        from linkmerce.api.smartstore.brand import match_catalog
        from linkmerce.common.load import DuckDBConnection
        from extensions.bigquery import connect as connect_bigquery
        from extensions.bigquery import load_table_from_duckdb

        with DuckDBConnection() as conn:
            conn.execute("SET TimeZone = 'Asia/Seoul';")
            match_catalog(cookies, *brand_ids, sort_type="recent", page=None, connection=conn, how="async", return_type="none")

            project_id = Variable.get("project_id")
            with connect_bigquery(project_id, Variable.get("service_account")) as client:
                load_table_from_duckdb(client, conn, "data", table_info["match"], project_id, _match_schema())

    def _match_schema() -> list:
        from extensions.bigquery import to_bigquery_schema
        return to_bigquery_schema([
            {"name":"mallPid", "type":"INTEGER", "description":"판매처코드", "mode":"REQUIRED"},
            {"name":"catalogId", "type":"INTEGER", "description":"카탈로그코드"},
            {"name":"createdAt", "type":"DATETIME", "description":"수집일시"}
        ])


    tables = read_tables()
    rank = etl_rank.partial().expand(client_and_query=get_query_with_client())
    match = etl_match(get_brand_ids(), read_cookies())
    tables >> rank >> match
