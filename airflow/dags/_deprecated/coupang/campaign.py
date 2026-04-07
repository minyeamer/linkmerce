from airflow.sdk import DAG, task
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "coupang_campaign",
    schedule = "55 5 * * 1-5",
    start_date = pendulum.datetime(2025, 11, 6, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:low", "coupang:campaign", "login:coupang", "schedule:weekdays", "time:morning"],
    doc_md = dedent("""
        # 쿠팡 광고 캠페인/소재 ETL 파이프라인

        > 안내) 쿠팡 광고 로그인 정책 강화로 사용 중지 (~ v0.6.8)

        ## 인증(Credentials)
        쿠팡 광고 로그인 쿠키가 필요하다.
        (정책 강화로 마지막으로 로그인된 쿠키만 사용할 수 있다.)

        ## 추출(Extract)
        쿠팡 업체별 전체 캠페인 목록을 수집하고 (광고그룹은 캠페인 내에 포함),
        신규 구매 고객 확보(NCA) 목표의 캠페인이 있다면 소재 목록도 추가로 가져온다.
        (삭제 여부를 구분해 각각 2번에 나눠서 요청한다.)

        ## 변환(Transform)
        JSON 형식의 응답 본문을 파싱하여 캠페인, 광고그룹, 소재에 대한
        각각의 DuckDB 테이블에 적재한다.

        ## 적재(Load)
        각각의 캠페인, 광고그룹, 소재 테이블을 기존 BigQuery 테이블과
        MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
    """).strip(),
) as dag:

    PATH = "coupang.advertising.campaign"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read
        return read(PATH, tables=True, service_account=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read
        return read(PATH, credentials=True)["credentials"]


    @task(task_id="etl_coupang_campaign", map_index_template="{{ credentials['vendor_id'] }}", pool="coupang_pool")
    def etl_coupang_campaign(credentials: dict, configs: dict, **kwargs) -> dict:
        """기본으로 매출 성장(PA) 목표의 캠페인을 가져온다.   
        인증 정보에 `nca=True` 설정된 업체만 신규 구매 고객 확보(NCA) 목표의 캠페인을 추가로 수집한다."""
        goal_types = ["SALES", "NCA"] if credentials.get("nca") else ["SALES"]
        return main(**credentials, goal_types=goal_types, **configs)

    def main(
            cookies: str,
            vendor_id: str,
            goal_types: list[str],
            service_account: dict,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.advertising import campaign
        from linkmerce.api.coupang.advertising import creative
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = {
            "campaign": "coupang_campaign",
            "adgroup": "coupang_adgroup",
            "creative": "coupang_creative",
        }

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            for goal_type in goal_types:
                for is_deleted in [False, True]:
                    campaign(
                        cookies = cookies,
                        vendor_id = vendor_id,
                        goal_type = goal_type,
                        is_deleted = is_deleted,
                        connection = conn,
                        progress = False,
                        return_type = "none",
                    )

            query = "SELECT campaign_id FROM coupang_campaign WHERE goal_type = 1"
            nca_campaign_ids = [row[0] for row in conn.sql(query)[0].fetchall()]

            if nca_campaign_ids:
                creative(
                    cookies = cookies,
                    vendor_id = vendor_id,
                    campaign_ids = nca_campaign_ids,
                    connection = conn,
                    progress = False,
                    return_type = "none",
                )

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "vendor_id": vendor_id,
                        "goal_type": goal_type,
                        "is_deleted": [False, True],
                    },
                    "counts": {
                        "campaign": conn.count_table(sources["campaign"]),
                        "adgroup": conn.count_table(sources["adgroup"]),
                        "creative": (conn.count_table(sources["creative"]) if nca_campaign_ids else None),
                    },
                    "status": {
                        "campaign": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["campaign"],
                            staging_table = f'{tables["temp_campaign"]}_{vendor_id}',
                            target_table = tables["campaign"],
                            **merge["campaign"],
                            progress = False,
                        ),
                        "adgroup": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["adgroup"],
                            staging_table = f'{tables["temp_adgroup"]}_{vendor_id}',
                            target_table = tables["adgroup"],
                            **merge["adgroup"],
                            progress = False,
                        ),
                        "creative": (client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["creative"],
                            staging_table = f'{tables["temp_creative"]}_{vendor_id}',
                            target_table = tables["creative"],
                            **merge["creative"],
                            progress = False,
                        ) if nca_campaign_ids else None),
                    },
                }


    (etl_coupang_campaign
    .partial(configs=read_configs())
    .expand(credentials=read_credentials()))
