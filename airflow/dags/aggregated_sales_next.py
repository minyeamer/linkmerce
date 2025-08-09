from airflow.sdk import DAG, Variable, task
import pendulum


with DAG(
    dag_id = "aggregated_sales_next",
    schedule = "10,30,50 9,10 * * *",
    start_date = pendulum.datetime(2023, 7, 20, tz="Asia/Seoul"),
    catchup = False,
    tags = ["smartstore", "brand", "sales"],
) as dag:

    @task(task_id="get_mall_seq")
    def get_mall_seq() -> list[int]:
        from extensions.gsheets import get_all_records
        import json
        sheet_name = json.loads(Variable.get("sheet_info"))["aggregated_sales"]
        rows = get_all_records(Variable.get("gspread_key"), sheet_name, account=Variable.get("service_account"))
        return [row["mallSeq"] for row in rows]

    @task(task_id="read_cookies")
    def read_cookies() -> str:
        with open(Variable.get("cookies_partnercenter"), 'r', encoding="utf-8") as file:
            return file.read().strip()

    @task(task_id="read_tables")
    def read_tables() -> dict[str,str]:
        import json
        return json.loads(Variable.get("table_info"))["aggregated_sales"]

    @task(task_id="etl")
    def etl(
            mall_seq: list[str],
            cookies: str,
            table_info: dict[str,str],
            data_interval_end: pendulum.DateTime = None,
            **kwargs
        ):
        from linkmerce.api.smartstore.brand import aggregated_sales
        from linkmerce.common.load import DuckDBConnection
        from extensions.bigquery import connect as connect_bigquery
        from extensions.bigquery import overwrite_table_from_duckdb
        from extensions.bigquery import upsert_table_from_duckdb

        with DuckDBConnection() as conn:
            conn.execute("SET TimeZone = 'Asia/Seoul';")
            date = data_interval_end.subtract(days=1).date()
            aggregated_sales(cookies, mall_seq, str(date), str(date), connection=conn, how="async", return_type="none")

            project_id = Variable.get("project_id")
            with connect_bigquery(project_id, Variable.get("service_account")) as client:
                condition = f"WHERE paymentDate = '{date}'"
                overwrite_table_from_duckdb(client, conn, "data", table_info["data"], project_id, condition, _sales_schema())

                condition = "WHERE TRUE"
                upsert_table_from_duckdb(client, conn, "product", table_info["product"], project_id, *_product_groupby(), condition, _product_schema())

    def _sales_schema() -> list:
        from extensions.bigquery import to_bigquery_schema
        return to_bigquery_schema([
            {"name":"mallPid", "type":"INTEGER", "description":"상품코드", "mode":"REQUIRED"},
            {"name":"mallSeq", "type":"INTEGER", "description":"판매처번호"},
            {"name":"categoryId3", "type":"INTEGER", "description":"소분류코드"},
            {"name":"clickCount", "type":"INTEGER", "description":"조회수"},
            {"name":"paymentCount", "type":"INTEGER", "description":"결제수"},
            {"name":"paymentAmount", "type":"INTEGER", "description":"결제금액"},
            {"name":"paymentDate", "type":"DATE", "description":"결제일자", "mode":"REQUIRED"}
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

    etl(get_mall_seq(), read_cookies(), read_tables())
