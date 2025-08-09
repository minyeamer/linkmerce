from airflow.sdk import DAG, Variable, task
import pendulum


with DAG(
    dag_id = "brand_price",
    schedule = "1 0 * * *",
    start_date = pendulum.datetime(2023, 7, 19, tz="Asia/Seoul"),
    catchup = False,
    tags = ["smartstore", "brand", "price"],
) as dag:

    @task(task_id="get_brand_ids")
    def get_brand_ids() -> tuple[list[int],list[int]]:
        from extensions.gsheets import get_all_records
        import json
        sheet_name = json.loads(Variable.get("sheet_info"))["brand_price"]
        rows = get_all_records(Variable.get("gspread_key"), sheet_name, account=Variable.get("service_account"), numericise_ignore=[1])
        return [row["brandIds"] for row in rows], [row["mallSeq"] for row in rows]

    @task(task_id="read_cookies")
    def read_cookies() -> str:
        with open(Variable.get("cookies_partnercenter"), 'r', encoding="utf-8") as file:
            return file.read().strip()

    @task(task_id="read_tables")
    def read_tables() -> dict[str,str]:
        import json
        return json.loads(Variable.get("table_info"))["brand_price"]

    @task(task_id="etl")
    def etl(brand_ids: tuple[list[str],list[int]], cookies: str, table_info: dict[str,str]):
        from linkmerce.api.smartstore.brand import brand_price
        from linkmerce.common.load import DuckDBConnection
        from extensions.bigquery import connect as connect_bigquery
        from extensions.bigquery import load_table_from_duckdb
        from extensions.bigquery import upsert_table_from_duckdb

        with DuckDBConnection() as conn:
            conn.execute("SET TimeZone = 'Asia/Seoul';")
            brand_price(cookies, *brand_ids, sort_type="recent", page=None, connection=conn, how="async", return_type="none")

            project_id = Variable.get("project_id")
            with connect_bigquery(project_id, Variable.get("service_account")) as client:
                load_table_from_duckdb(client, conn, "data", table_info["data"], project_id, _price_schema())

                condition = "WHERE TRUE"
                upsert_table_from_duckdb(client, conn, "product", table_info["product"], project_id, *_product_groupby(), condition, _product_schema())

    def _price_schema() -> list:
        from extensions.bigquery import to_bigquery_schema
        return to_bigquery_schema([
            {"name":"mallPid", "type":"INTEGER", "description":"상품코드", "mode":"REQUIRED"},
            {"name":"mallSeq", "type":"INTEGER", "description":"판매처번호"},
            {"name":"categoryId", "type":"INTEGER", "description":"카테고리코드"},
            {"name":"salesPrice", "type":"INTEGER", "description":"판매가"},
            {"name":"updateDate", "type":"DATE", "description":"수집일자", "mode":"REQUIRED"}
        ])

    def _product_schema() -> list:
        from extensions.bigquery import to_bigquery_schema
        return to_bigquery_schema([
            {"name":"mallPid", "type":"INTEGER", "description":"상품코드", "mode":"REQUIRED"},
            {"name":"mallSeq", "type":"INTEGER", "description":"판매처번호"},
            {"name":"categoryId", "type":"INTEGER", "description":"카테고리코드"},
            {"name":"categoryId3", "type":"INTEGER", "description":"소분류코드"},
            {"name":"productName", "type":"STRING", "description":"상품명"},
            {"name":"salesPrice", "type":"INTEGER", "description":"판매가"},
            {"name":"registerDate", "type":"DATE", "description":"등록일자"},
            {"name":"updateDate", "type":"DATE", "description":"갱신일자"}
        ])

    def _product_groupby() -> tuple[list[str], dict[str,str]]:
        by = ["mallPid"]
        agg = {
            "mallSeq": "first",
            "categoryId": "first",
            "categoryId3": "first",
            "productName": "first",
            "salesPrice": "first",
            "registerDate": "min",
            "updateDate": "max"
        }
        return by, agg

    etl(get_brand_ids(), read_cookies(), read_tables())
