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
        goal_types = ["SALES", "NCA"] if credentials.get("nca") else ["SALES"]
        return main(**credentials, goal_types=goal_types, **variables)

    def main(
            cookies: str,
            vendor_id: str,
            goal_types: list[str],
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.advertising import campaign as list_campaign
        from linkmerce.api.coupang.advertising import creative as list_creative
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = dict(campaign="coupang_campaign", adgroup="coupang_adgroup")

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            for goal_type in goal_types:
                for is_deleted in [False, True]:
                    list_campaign(
                        cookies = cookies,
                        vendor_id = vendor_id,
                        goal_type = goal_type,
                        is_deleted = is_deleted,
                        connection = conn,
                        tables = sources,
                        progress = False,
                        return_type = "none",
                    )

            query = "SELECT campaign_id FROM {} WHERE goal_type = 1".format(sources["campaign"])
            nca_campaign_ids = [row[0] for row in conn.sql(query).fetchall()]

            if nca_campaign_ids:
                list_creative(
                    cookies = cookies,
                    vendor_id = vendor_id,
                    campaign_ids = nca_campaign_ids,
                    connection = conn,
                    tables = {"default": "coupang_creative"},
                    progress = False,
                    return_type = "none",
                )

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        vendor_id = vendor_id,
                        goal_type = goal_type,
                        is_deleted = [False, True],
                    ),
                    count = dict(
                        campaign = conn.count_table(sources["campaign"]),
                        adgroup = conn.count_table(sources["adgroup"]),
                        creative = (conn.count_table("coupang_creative") if nca_campaign_ids else None),
                    ),
                    status = dict(
                        campaign = client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["campaign"],
                            staging_table = f'{tables["temp_campaign"]}_{vendor_id}',
                            target_table = tables["campaign"],
                            **merge["campaign"],
                            progress = False,
                        ),
                        adgroup = client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["adgroup"],
                            staging_table = f'{tables["temp_adgroup"]}_{vendor_id}',
                            target_table = tables["adgroup"],
                            **merge["adgroup"],
                            progress = False,
                        ),
                        creative = (client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = "coupang_creative",
                            staging_table = f'{tables["temp_creative"]}_{vendor_id}',
                            target_table = tables["creative"],
                            **merge["creative"],
                            progress = False,
                        ) if nca_campaign_ids else None),
                    ),
                )


    etl_coupang_campaign.partial(variables=read_variables()).expand(credentials=read_credentials())
