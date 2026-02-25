from airflow.sdk import DAG, task
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "google_ads",
    schedule = "50 7 * * *",
    start_date = pendulum.datetime(2026, 2, 26, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    tags = ["priority:medium", "google:ads", "api:google", "schedule:daily", "time:morning"],
) as dag:

    GOOGLE_PATH = ["google", "api", "google_ads"]

    @task(task_id="read_objects_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_objects_variables() -> dict:
        from variables import read
        return read(GOOGLE_PATH, tables=True, service_account=True)

    @task(task_id="read_objects_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_objects_credentials() -> list:
        from variables import read
        return read(GOOGLE_PATH, credentials=True)["credentials"]


    AD_OBJECTS = ["campaign", "adgroup", "ad", "asset"]

    @task(task_id="etl_google_objects", map_index_template="{{ credentials['customer_id'] }}")
    def etl_google_objects(credentials: dict, variables: dict, **kwargs) -> dict:
        return {ad_level: main_object(ad_level, **credentials, **variables) for ad_level in AD_OBJECTS}

    def main_object(
            ad_level: str,
            customer_id: int | str,
            manager_id: int | str,
            developer_token: str,
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.extensions.bigquery import BigQueryClient
        from importlib import import_module
        extract = getattr(import_module("linkmerce.api.google.api"), ad_level)
        date_range = dict() if ad_level == "asset" else dict(date_range = "LAST_30_DAYS")

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            extract(
                customer_id = customer_id,
                manager_id = manager_id,
                developer_token = developer_token,
                service_account = service_account,
                **date_range,
                connection = conn,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        ad_level = ad_level,
                        **date_range,
                    ),
                    counts = dict(
                        data = conn.count_table("data"),
                    ),
                    status = dict(
                        data = client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = "data",
                            staging_table = "{}_{}".format(tables[f"temp_{ad_level}"], customer_id),
                            target_table = tables[ad_level],
                            **merge[ad_level],
                            progress = False,
                        ),
                    ),
                )


    @task(task_id="read_insight_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_insight_variables() -> dict:
        from variables import read
        return read(GOOGLE_PATH, tables=True, service_account=True)

    @task(task_id="read_insight_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_insight_credentials() -> list:
        from variables import read
        return read(GOOGLE_PATH, credentials=True)["credentials"]


    @task(task_id="etl_google_insight", map_index_template="{{ credentials['customer_id'] }}")
    def etl_google_insight(credentials: dict, variables: dict, **kwargs) -> dict:
        from variables import get_execution_date
        return main_insight(**credentials, date=get_execution_date(kwargs, subdays=1), **variables)

    def main_insight(
            customer_id: int | str,
            manager_id: int | str,
            developer_token: str,
            date: str,
            service_account: dict,
            tables: dict[str,str],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.google.api import asset_view
        from linkmerce.extensions.bigquery import BigQueryClient

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            asset_view(
                customer_id = customer_id,
                manager_id = manager_id,
                developer_token = developer_token,
                service_account = service_account,
                start_date = date,
                end_date = date,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        date = date,
                    ),
                    counts = dict(
                        data = conn.count_table("data"),
                    ),
                    status = dict(
                        data = client.load_table_from_duckdb(
                            connection = conn,
                            source_table = "data",
                            target_table = tables["insight"],
                            progress = False,
                        ),
                    ),
                )


    google_objects = (etl_google_objects
        .partial(variables=read_objects_variables())
        .expand(credentials=read_objects_credentials()))


    google_insight = (etl_google_insight
        .partial(variables=read_insight_variables())
        .expand(credentials=read_insight_credentials()))


    google_objects >> google_insight
