from airflow.sdk import DAG, task
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "searchad_contract",
    schedule = "30 5 * * *",
    start_date = pendulum.datetime(2025, 10, 30, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=5),
    catchup = False,
    tags = ["priority:medium", "searchad:contract", "api:searchad", "schedule:daily", "time:morning"],
) as dag:

    PATH = ["searchad", "api", "searchad_contract"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from variables import read
        return read(PATH, credentials=True)["credentials"]


    @task(task_id="etl_searchad_contract", map_index_template="{{ queries['customer_id'] }}")
    def etl_searchad_contract(queries: dict, variables: dict, **kwargs) -> dict:
        return main(**queries, **variables)

    def main(
            api_key: str,
            secret_key: str,
            customer_id: int | str,
            service_account: dict,
            tables: dict[str,str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.searchad.api import contract
        from linkmerce.extensions.bigquery import BigQueryClient

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            contract(
                api_key = api_key,
                secret_key = secret_key,
                customer_id = customer_id,
                connection = conn,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        customer_id = customer_id,
                    ),
                    count = dict(
                        time = conn.execute("SELECT COUNT(*) FROM data WHERE contract_type = 0;").fetchall()[0][0],
                        brand_new = conn.execute("SELECT COUNT(*) FROM data WHERE contract_type = 1;").fetchall()[0][0],
                    ),
                    status = dict(
                        data = client.overwrite_table_from_duckdb(
                            connection = conn,
                            source_table = "data",
                            target_table = tables["data"],
                            where_clause = f"(contract_end_date > '2000-01-01') AND (customer_id = {customer_id})",
                            progress = False,
                        ),
                    ),
                )


    etl_searchad_contract.partial(variables=read_variables()).expand(queries=read_credentials())
