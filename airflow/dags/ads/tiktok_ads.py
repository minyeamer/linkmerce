"""
# 틱톡 광고 ETL 파이프라인

> 전용 Slack 채널에 틱톡 광고 일별 보고서 파일을 업로드한 후 수동으로 실행한다.

## 인증(Credentials)
전용 Slack 채널의 파일을 다운로드할 수 있는 Slack API 토큰이 필요하다.

## 추출(Extract)
Slack 채널에 업로드된 최신 틱톡 광고 일별 보고서 엑셀 파일을 다운로드한다.

## 변환(Transform)
엑셀 행에서 캠페인, 광고그룹, 광고, 일별 광고 보고서를 DuckDB 테이블로 분리한다.

## 적재(Load)
- 각각의 캠페인, 광고그룹, 광고 테이블을 기존 BigQuery/Postgres 테이블과
  MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
- 보고서 테이블은 대응되는 BigQuery/Postgres 테이블을 지우고 덮어쓴다.
"""

from airflow.sdk import DAG, task
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.providers.slack.hooks.slack import SlackHook
from cosmos import DbtTaskGroup
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "tiktok_ads",
    schedule = None,
    start_date = pendulum.datetime(2026, 9, 2, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:tiktok-ads", "objective:ads",
        "schedule:manual", "write:overwrite", "write:merge", "plugin:dbt",
        "provider:slack", "upstream:manual"
    ],
) as dag:

    PATH = "tiktok.ads.report"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)


    @task(task_id="etl_tiktok_ads", retries=3, retry_delay=timedelta(minutes=1))
    def etl_tiktok_ads(ti: TaskInstance, **kwargs) -> dict:
        return main(**ti.xcom_pull(task_ids="read_configs"))

    def main(
            slack_conn_id: str,
            channel_id: str,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs,
        ) -> dict:
        """Slack에 업로드된 틱톡 광고 보고서를 DuckDB를 거쳐 BigQuery/Postgres에 적재한다."""
        from dual_load import merge_table_from_duckdb, overwrite_table_from_duckdb
        from linkmerce.common.load import DuckDBConnection
        import logging

        logger = logging.getLogger(__name__)
        file_name, rows = download_tiktok_ads_report(slack_conn_id, channel_id)
        logger.info("TikTok Ads report file: '%s', row count: %s", file_name, len(rows))

        sources = {
            "campaign": "tiktok_ads_campaign",
            "adgroup": "tiktok_ads_adgroup",
            "ad": "tiktok_ads_ad",
            "report": "tiktok_ads_report",
            "rows": "tiktok_ads_rows",
        }

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            conn.execute(create_source_rows(sources["rows"]))
            conn.insert_into_table_from_json(sources["rows"], rows)

            conn.execute(create_campaign(sources["campaign"]))
            conn.execute(create_adgroup(sources["adgroup"]))
            conn.execute(create_ad(sources["ad"]))
            conn.execute(create_report(sources["report"]))

            conn.execute(bulk_insert_campaign(sources["campaign"], sources["rows"]))
            conn.execute(bulk_insert_adgroup(sources["adgroup"], sources["rows"]))
            conn.execute(bulk_insert_ad(sources["ad"], sources["rows"]))
            conn.execute(bulk_insert_report(sources["report"], sources["rows"]))

            partitions = conn.unique(sources["report"], "ymd")

            return {
                "context": {
                    "partitions": sorted(map(str, partitions)),
                },
                "params": {
                    "file_name": file_name,
                    "row_count": len(rows),
                },
                "results": {
                    "campaign": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["campaign"],
                        target_table = tables["campaign"],
                        **merge["campaign"],
                    ),
                    "adgroup": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["adgroup"],
                        target_table = tables["adgroup"],
                        **merge["adgroup"],
                    ),
                    "ad": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["ad"],
                        target_table = tables["ad"],
                        **merge["ad"],
                    ),
                    "report": overwrite_table_from_duckdb(
                        connection = conn,
                        source_table = sources["report"],
                        target_table = tables["report"],
                        where_clause = conn.expr_date_range("ymd", partitions),
                        execute = bool(partitions),
                    ),
                },
            }


    def download_tiktok_ads_report(slack_conn_id: str, channel_id: str) -> tuple[str, list[dict]]:
        """Slack 채널에서 틱톡 광고 일별 보고서 파일을 다운로드하여 행 목록으로 변환한다."""
        from linkmerce.utils.excel import excel2json
        import requests

        if not channel_id:
            raise AirflowException("TikTok Ads Slack channel ID is not configured.")

        slack_hook = SlackHook(slack_conn_id=slack_conn_id)
        response = slack_hook.client.conversations_history(channel=channel_id, limit=5)
        messages: list[dict] = response.get("messages") or list()

        file_name = url = None
        for message in messages:
            files = message.get("files") or list()
            file: dict = files[0] if files and isinstance(files[0], dict) else None
            if not file:
                continue

            name = file.get("name") or file.get("title") or str()
            if name.startswith("Tiktok Ads") and name.lower().endswith(".xlsx"):
                file_name = name
                url = file.get("url_private_download") or file.get("url_private")
                break

        if not (file_name and url):
            raise AirflowException("No TikTok Ads report file was found in Slack.")

        token = slack_hook.get_conn().token
        response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
        response.raise_for_status()

        rows = excel2json(response.content, warnings=False)
        return file_name, [row for row in rows if row["By Day"] != '-']


    def create_source_rows(table: str) -> str:
        return dedent(f"""
            CREATE TABLE {table} (
                "Account name" VARCHAR
                , "By Day" VARCHAR
                , "Campaign name" VARCHAR
                , "Campaign ID" VARCHAR
                , "Ad group name" VARCHAR
                , "Ad group ID" VARCHAR
                , "Advertising objective" VARCHAR
                , "Campaign type" VARCHAR
                , "Optimization goal" VARCHAR
                , "Ad name" VARCHAR
                , "Ad ID" VARCHAR
                , "Website URL (Ad level）" VARCHAR
                , "Ad Type" VARCHAR
                , "Spend" INTEGER
                , "Impressions" INTEGER
                , "Clicks (destination)" INTEGER
                , "Reach" INTEGER
                , "Conversions" INTEGER
                , "Currency" VARCHAR
            )
            """).strip()


    def create_campaign(table: str) -> str:
        return dedent(f"""
            CREATE TABLE {table} (
                campaign_id VARCHAR NOT NULL
                , campaign_name VARCHAR
                , PRIMARY KEY (campaign_id)
            )
            """).strip()

    def bulk_insert_campaign(table: str, rows: str) -> str:
        return dedent(f"""
            INSERT INTO {table}
            SELECT
                "Campaign ID" AS campaign_id
                , "Campaign name" AS campaign_name
            FROM {rows}
            WHERE NULLIF("Campaign ID", '-') IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY "Campaign ID" ORDER BY "By Day" DESC) = 1
            ON CONFLICT DO NOTHING
            """).strip()


    def create_adgroup(table: str) -> str:
        return dedent(f"""
            CREATE TABLE {table} (
                adgroup_id VARCHAR NOT NULL
                , adgroup_name VARCHAR
                , campaign_id VARCHAR NOT NULL
                , PRIMARY KEY (adgroup_id)
            )
            """).strip()

    def bulk_insert_adgroup(table: str, rows: str) -> str:
        return dedent(f"""
            INSERT INTO {table}
            SELECT
                "Ad group ID" AS adgroup_id
                , "Ad group name" AS adgroup_name
                , "Campaign ID" AS campaign_id
            FROM {rows}
            WHERE NULLIF("Ad group ID", '-') IS NOT NULL
                AND NULLIF("Campaign ID", '-') IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY "Ad group ID" ORDER BY TRY_CAST("By Day" AS DATE) DESC) = 1
            ON CONFLICT DO NOTHING
            """).strip()


    def create_ad(table: str) -> str:
        return dedent(f"""
            CREATE TABLE {table} (
                ad_id VARCHAR NOT NULL
                , ad_name VARCHAR
                , campaign_id VARCHAR NOT NULL
                , adgroup_id VARCHAR NOT NULL
                , landing_url VARCHAR
                , PRIMARY KEY (ad_id)
            )
            """).strip()

    def bulk_insert_ad(table: str, rows: str) -> str:
        return dedent(f"""
            INSERT INTO {table}
            SELECT
                "Ad ID" AS ad_id
                , "Ad name" AS ad_name
                , "Campaign ID" AS campaign_id
                , "Ad group ID" AS adgroup_id
                , "Website URL (Ad level）" AS landing_url
            FROM {rows}
            WHERE NULLIF("Ad ID", '-') IS NOT NULL
                AND NULLIF("Campaign ID", '-') IS NOT NULL
                AND NULLIF("Ad group ID", '-') IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY "Ad ID" ORDER BY TRY_CAST("By Day" AS DATE) DESC) = 1
            ON CONFLICT DO NOTHING
            """).strip()


    def create_report(table: str) -> str:
        return dedent(f"""
            CREATE TABLE {table} (
                campaign_id VARCHAR NOT NULL
                , adgroup_id VARCHAR NOT NULL
                , ad_id VARCHAR NOT NULL
                , ad_type VARCHAR NOT NULL
                , impression_count INTEGER
                , click_count INTEGER
                , reach_count INTEGER
                , ad_cost INTEGER
                , conv_count INTEGER
                , ymd DATE NOT NULL
                , PRIMARY KEY (ymd, ad_id, ad_type)
            )
            """).strip()

    def bulk_insert_report(table: str, rows: str) -> str:
        return dedent(f"""
            INSERT INTO {table}
            SELECT
                "Campaign ID" AS campaign_id
                , "Ad group ID" AS adgroup_id
                , "Ad ID" AS ad_id
                , COALESCE("Ad Type", '-') AS ad_type
                , "Impressions" AS impression_count
                , "Clicks (destination)" AS click_count
                , "Reach" AS reach_count
                , "Spend" AS ad_cost
                , "Conversions" AS conv_count
                , TRY_CAST("By Day" AS DATE) AS ymd
            FROM {rows}
            WHERE NULLIF("Campaign ID", '-') IS NOT NULL
                AND NULLIF("Ad group ID", '-') IS NOT NULL
                AND NULLIF("Ad ID", '-') IS NOT NULL
                AND TRY_CAST("By Day" AS DATE) IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY "By Day", "Ad ID", "Ad Type"
                ORDER BY "Campaign ID" DESC, "Ad group ID" DESC
            ) = 1
            ON CONFLICT DO NOTHING
            """).strip()


    @task(task_id="generate_dbt_date_range")
    def generate_dbt_date_range(result: dict) -> dict:
        from dbt_cosmos import generate_dbt_date_range as generate
        return generate(result, "context.partitions")


    @task.short_circuit(task_id="prepare_dbt_run")
    def prepare_dbt_run(ti: TaskInstance, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_dbt_date_range")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False


    def dbt_bigquery_tiktok_ads_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_tiktok_ads",
            selector = "tiktok_ads",
            ds_task_id = "generate_dbt_date_range",
        )

    def dbt_postgres_tiktok_ads_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_postgres
        return dynamic_mapping_dbt_postgres(
            group_id = "dbt_postgres_tiktok_ads",
            selector = "tiktok_ads",
            ds_task_id = "generate_dbt_date_range",
        )


    etl_result = etl_tiktok_ads()

    dbt_date_range = generate_dbt_date_range(etl_result)
    dbt_run = [dbt_bigquery_tiktok_ads_group(), dbt_postgres_tiktok_ads_group()]

    read_configs() >> etl_result
    dbt_date_range >> prepare_dbt_run() >> dbt_run
