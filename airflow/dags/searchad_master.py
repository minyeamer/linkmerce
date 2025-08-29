from airflow.sdk import DAG, task
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "searchad_master",
    schedule = "40 5 * * *",
    start_date = pendulum.datetime(2025, 8, 30, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    tags = ["priority:medium", "searchad:report", "api:searchad", "schedule:daily", "time:morning"],
) as dag:

    PATH = ["searchad", "api", "searchad_master"]

    @task(task_id="read_variables")
    def read_variables() -> dict:
        from variables import read
        return read(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials")
    def read_credentials() -> list:
        from variables import read
        return read(PATH, credentials=True)["credentials"]


    @task(task_id="etl_searchad_master", map_index_template="{{ credentials['customer_id'] }}")
    def etl_searchad_master(credentials: dict, variables: dict, data_interval_end: pendulum.DateTime = None, **kwargs) -> dict:
        from_date = str(data_interval_end.in_timezone("Asia/Seoul").subtract(days=365*2).date())
        return {api_type: main(api_type, **credentials, from_date=from_date, **variables) for api_type in ["campaign", "adgroup", "ad"]}

    def main(
            api_type: str,
            api_key: str,
            secret_key: str,
            customer_id: int | str,
            from_date: str,
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.extensions.bigquery import BigQueryClient
        from importlib import import_module
        import logging
        extract = getattr(import_module("linkmerce.api.searchad.api"), api_type)

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            extract(api_key, secret_key, customer_id, from_date, connection=conn, return_type="none")
            logging.info(f"[{customer_id}] API request completed for downloading the {api_type} master report")

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        customer_id = customer_id,
                        from_date = from_date,
                    ),
                    count = dict(
                        data = conn.count_table("data"),
                    ),
                    status = dict(
                        data = client.merge_into_table_from_duckdb(conn, "data", f'{tables[f"temp_{api_type}"]}_{customer_id}', tables[api_type], **merge[api_type]),
                    )
                )


    etl_searchad_master.partial(variables=read_variables()).expand(credentials=read_credentials())
