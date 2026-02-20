from airflow.sdk import DAG, task
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "meta_ads",
    schedule = "40 7 * * *",
    start_date = pendulum.datetime(2026, 2, 20, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    tags = ["priority:medium", "meta:ads", "api:meta", "schedule:daily", "time:morning"],
) as dag:

    META_PATH = ["meta", "api", "meta_ads"]

    @task(task_id="read_objects_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_objects_variables() -> dict:
        from variables import read
        return read(META_PATH, tables=True, service_account=True)

    @task(task_id="read_objects_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_objects_credentials() -> list:
        from variables import read
        return read(META_PATH, credentials=True)["credentials"]


    @task(task_id="etl_meta_objects", map_index_template="{{ credentials['app_id'] }}")
    def etl_meta_objects(credentials: dict, variables: dict, **kwargs) -> dict:
        variables = dict(variables, merge=variables["merge"]["objects"])
        return {api_type: main_objects(api_type, **credentials, **variables) for api_type in ["campaigns","adsets","ads"]}

    def main_objects(
            api_type: str,
            access_token: str,
            app_id: str,
            app_secret: str,
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            account_ids: list[str] = list(),
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.extensions.bigquery import BigQueryClient
        from importlib import import_module
        extract = getattr(import_module("linkmerce.api.meta.api"), api_type)

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            extract(
                access_token = access_token,
                app_id = app_id,
                app_secret = app_secret,
                account_ids = account_ids,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        account_ids = account_ids,
                    ),
                    counts = dict(
                        data = conn.count_table("data"),
                    ),
                    status = dict(
                        data = client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = "data",
                            staging_table = "{}_{}".format(tables[f"temp_{api_type}"], app_id),
                            target_table = tables[api_type],
                            **merge[api_type],
                            progress = False,
                        ),
                    ),
                )


    @task(task_id="read_insights_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_insights_variables() -> dict:
        from variables import read
        return read(META_PATH, tables=True, service_account=True)

    @task(task_id="read_insights_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_insights_credentials() -> list:
        from variables import read
        return read(META_PATH, credentials=True)["credentials"]


    @task(task_id="etl_meta_insights", map_index_template="{{ credentials['app_id'] }}")
    def etl_meta_insights(credentials: dict, variables: dict, **kwargs) -> dict:
        from variables import get_execution_date
        variables = dict(variables, merge=variables["merge"]["insights"])
        return main_insights(**credentials, date=get_execution_date(kwargs, subdays=1), **variables)

    def main_insights(
            access_token: str,
            app_id: str,
            app_secret: str,
            date: str,
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            account_ids: list[str] = list(),
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.meta.api import insights
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = dict(
            campaigns = "meta_campaigns",
            adsets = "meta_adsets",
            ads = "meta_ads",
            metrics = "meta_insight",
        )

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            insights(
                access_token = access_token,
                app_id = app_id,
                app_secret = app_secret,
                ad_level = "ad",
                start_date = date,
                end_date = date,
                date_type = "daily",
                account_ids = account_ids,
                connection = conn,
                tables = sources,
                progress = False,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        ad_level = "ad",
                        date = date,
                        date_type = "daily",
                        account_ids = account_ids,
                    ),
                    counts = dict(
                        campaigns = conn.count_table(sources["campaigns"]),
                        adsets = conn.count_table(sources["adsets"]),
                        ads = conn.count_table(sources["ads"]),
                        metrics = conn.count_table(sources["metrics"]),
                    ),
                    status = dict(
                        campaigns = client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["campaigns"],
                            staging_table = f'{tables["temp_campaigns"]}_{app_id}',
                            target_table = tables["campaigns"],
                            **merge["campaigns"],
                            progress = False,
                        ),
                        adsets = client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["adsets"],
                            staging_table = f'{tables["temp_adsets"]}_{app_id}',
                            target_table = tables["adsets"],
                            **merge["adsets"],
                            progress = False,
                        ),
                        ads = client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["ads"],
                            staging_table = f'{tables["temp_ads"]}_{app_id}',
                            target_table = tables["ads"],
                            **merge["ads"],
                            progress = False,
                        ),
                        metrics = client.load_table_from_duckdb(
                            connection = conn,
                            source_table = sources["metrics"],
                            target_table = tables["insights"],
                            progress = False,
                        ),
                    ),
                )


    meta_objects = (etl_meta_objects
        .partial(variables=read_objects_variables())
        .expand(credentials=read_objects_credentials()))


    meta_insights = (etl_meta_insights
        .partial(variables=read_insights_variables())
        .expand(credentials=read_insights_credentials()))


    meta_objects >> meta_insights
