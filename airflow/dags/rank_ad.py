from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
import pendulum


with DAG(
    dag_id = "rank_ad",
    schedule = "0 6-18 * * *",
    start_date = pendulum.datetime(2025, 8, 11, tz="Asia/Seoul"),
    catchup = False,
    tags = ["searchad", "searchad_manage", "naver_rank"],
) as dag:

    @task(task_id="customer_info")
    def customer_info() -> list[dict]:
        from variables import list_accounts
        return list_accounts(["searchad","manage"])

    @task(task_id="cookies")
    def cookies() -> list[str]:
        from variables import list_cookies
        return list_cookies(["searchad","manage"])

    @task(task_id="tables")
    def tables() -> dict[str,str]:
        from variables import map_tables
        return map_tables(["searchad","manage","rank_ad"])

    @task(task_id="schemas")
    def schemas() -> dict[str,list[dict]]:
        from variables import map_schemas
        return map_schemas(["searchad","manage","rank_ad"])

    @task(task_id="params")
    def params() -> tuple[list,dict]:
        from variables import read_params
        params_ = read_params(["searchad","manage","product"])
        return params_["by"], params_["agg"]

    @task(task_id="service_account")
    def service_account() -> dict:
        from variables import read_service_account
        return read_service_account()

    def _read_xcom(ti: TaskInstance, **kwargs) -> tuple[str, dict[str,str], dict[str,list[dict]], tuple[list,dict], dict]:
        tasks = ["mkdir", "tables", "schemas", "params", "service_account"]
        return [ti.xcom_pull(task_ids=task_id) for task_id in tasks]


    @task(task_id="mkdir")
    def mkdir() -> str:
        from variables import read_file_path
        import os
        import shutil

        file_path = read_file_path(["searchad","manage","rank_ad"])
        if os.path.exists(file_path):
            shutil.rmtree(file_path)
        os.makedirs(file_path, exist_ok=True)
        os.makedirs(os.path.join(file_path, "data"), exist_ok=True)
        os.makedirs(os.path.join(file_path, "product"), exist_ok=True)
        return file_path


    @task(task_id="queries")
    def queries(customer_info: list[dict], cookies: list[str]) -> list[dict]:
        from variables import read_google_sheets
        keywords = read_google_sheets(["searchad","manage","rank_ad"], "keyword", numericise_ignore=[1])
        n_split = list(range(0, len(keywords), len(keywords)//len(customer_info)))[:len(customer_info)]
        return [dict(customer_info[i], cookies=cookies[i], keyword=keywords[start:end], seq=i)
                for i, (start, end) in enumerate(zip(n_split, (n_split[1:]+[None])))]


    @task(task_id="etl_rank_ad", pool="searchad_rank_pool")
    def etl_rank_ad(queries: dict, **kwargs) -> list[dict]:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.searchad.manage import exposure_rank
        from extensions.bigquery import connect as connect_bigquery
        from extensions.bigquery import load_table_from_duckdb
        import os
        import time

        temp_path, tables, schemas, _, service_account = _read_xcom(**kwargs)
        project_id = service_account["project_id"]
        returns = list()

        keywords = queries.pop("keyword", list())
        seq = queries.pop("seq", 0)
        domain = "search"
        mobile = True

        for i in range(0, len(keywords), 100):
            keyword = keywords[i:i+100]

            with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
                results = exposure_rank(**queries, keyword=keyword, domain=domain, mobile=mobile, connection=conn, return_type="parquet")
                temp_file = {name: _write_parquet(results[name], os.path.join(temp_path, f"{name}/{seq}_{i}.parquet")) for name in ["data", "product"]}

                with connect_bigquery(project_id, service_account) as client:
                    returns.append(dict(
                        query = len(keyword),
                        domain = domain,
                        mobile = mobile,
                        count = dict(rank=conn.count_table("data"), product=conn.count_table("product")),
                        status = dict(rank=load_table_from_duckdb(client, conn, "data", tables["data"], project_id, schemas["data"]), product=None),
                        temp_file = temp_file,
                    ))
            time.sleep(1)

        return returns

    def _write_parquet(obj: bytes, temp_file: str) -> str:
        with open(temp_file, "wb") as file:
            file.write(obj)
        return temp_file


    @task(task_id="refresh_rank", trigger_rule="one_success")
    def refresh_rank(**kwargs) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from extensions.bigquery import connect as connect_bigquery
        from extensions.bigquery import overwrite_table_from_duckdb, TEMP_TABLE

        temp_path, tables, schemas, _, service_account = _read_xcom(**kwargs)
        project_id = service_account["project_id"]

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            conn.execute("CREATE OR REPLACE TEMP TABLE data AS SELECT * FROM read_parquet('{}/data/*.parquet')".format(temp_path))
            with connect_bigquery(project_id, service_account) as client:
                cur_args = (client, conn, "data", tables["cur"], project_id, "WHERE TRUE", schemas["data"])
                prev_args = (client, conn, TEMP_TABLE, tables["prev"], project_id, "WHERE TRUE", schemas["data"])
                return dict(status=dict(cur=overwrite_table_from_duckdb(*cur_args), prev=overwrite_table_from_duckdb(*prev_args)))


    @task(task_id="upsert_product", trigger_rule="one_success")
    def upsert_product(**kwargs) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from extensions.bigquery import connect as connect_bigquery
        from extensions.bigquery import upsert_table_from_duckdb

        temp_path, tables, schemas, groupby, service_account = _read_xcom(**kwargs)
        project_id = service_account["project_id"]

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            conn.execute("CREATE OR REPLACE TEMP TABLE product AS SELECT * FROM read_parquet('{}/product/*.parquet')".format(temp_path))
            with connect_bigquery(project_id, service_account) as client:
                product_args = (client, conn, "product", tables["product"], project_id, *groupby, "WHERE TRUE", schemas["product"])
                return dict(status = upsert_table_from_duckdb(*product_args))


    read = [tables(), schemas(), params(), service_account()]
    main = etl_rank_ad.expand(queries=queries(customer_info(), cookies()))

    read >> mkdir() >> main >> refresh_rank() >> upsert_product()
