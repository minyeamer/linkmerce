from airflow.sdk import DAG, task
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "coupang_campaign",
    schedule = "55 5 * * *",
    start_date = pendulum.datetime(2025, 11, 6, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:low", "coupang:campaign", "login:coupang", "schedule:daily", "time:morning"],
) as dag:

    PATH = ["coupang", "advertising", "coupang_campaign"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from variables import read
        return read(PATH, credentials=True)["credentials"]


    @task(task_id="etl_coupang_campaign", map_index_template="{{ credentials['vendor_id'] }}", pool="coupang_pool")
    def etl_coupang_campaign(credentials: dict, variables: dict, **kwargs) -> dict:
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
        from linkmerce.api.coupang.advertising import campaign
        from linkmerce.extensions.bigquery import BigQueryClient

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            for is_deleted in [False, True]:
                campaign(
                    cookies = cookies,
                    vendor_id = vendor_id,
                    is_deleted = is_deleted,
                    connection = conn,
                    progress = False,
                    return_type = "none",
                )

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        vendor_id = vendor_id,
                        is_deleted = [False, True],
                    ),
                    count = dict(
                        data = conn.count_table("data"),
                    ),
                    status = dict(
                        data = client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = "data",
                            staging_table = f'{tables["temp_campaign"]}_{vendor_id}',
                            target_table = tables["campaign"],
                            **merge["campaign"],
                            progress = False,
                        ),
                    ),
                )


    etl_coupang_campaign.partial(variables=read_variables()).expand(credentials=read_credentials())
