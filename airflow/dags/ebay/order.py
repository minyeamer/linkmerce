"""
# ESM PLUS 주문 내역 ETL 파이프라인

## 인증(Credentials)
1. ESM PLUS 로그인 정보(아이디, 비밀번호, 도메인) 목록을 읽는다.
2. 로그인 정보 내 선택적으로 존재하는 쿠키 파일의 유효성을 검사하여 유효하면 로그인을 스킵한다.
3. 쿠키가 없거나 유효하지 않다면 Playwright 브라우저를 통해 계정별로 순차 로그인하고 쿠키를 발급받는다.
4. 로그인 시 쿠키에 'ESM_REQUEST_AUTH_PC' 값이 있다면 로그인된 것으로 인식하고 지정된 파일에 덮어쓴다.
5. 모든 계정의 로그인이 끝나면 계정별 쿠키를 사용해 ETL Task를 동적으로 매핑한다.

## 추출(Extract)
실행 시점(data_interval_end)에서 30일 전부터 현재까지의 검색 기간에 대해
ESM PLUS 마스터 계정에 등록된 모든 G마켓/옥션 주문 내역을 수집한다.

## 변환(Transform)
JSON 형식의 주문 내역으로부터 주문 정보, 주문 상품 정보를 분리해 각각의 DuckDB 테이블에 적재한다.

## 적재(Load)
- 각각의 테이블을 대응되는 BigQuery/Postgres 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
- 적재 과정에서 수집한 주문 일자 파티션 범위를 바탕으로 후속 dbt 모델을 실행한다.
"""

from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from cosmos import DbtTaskGroup
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "ebay_order",
    schedule = "0 */1 * * *",
    start_date = pendulum.datetime(2026, 10, 26, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:esmplus", "objective:sales", "objective:product",
        "credentials:userid", "schedule:hourly", "time:morning", "time:afternoon", "time:night",
        "write:merge", "plugin:playwright", "plugin:dbt"
    ],
) as dag:

    PATH = "ebay.esmplus.order"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_config
        from pw_actions import login_ebay_bulk
        credentials = read_config(PATH, credentials=True, skip_subpath=True)["credentials"]
        return login_ebay_bulk(credentials, where="esmplus")

    def validate_cookies(credentials: dict):
        from linkmerce.common.exceptions import AuthenticationError
        if not credentials.get("cookies"):
            raise AuthenticationError("ESM PLUS authentication cookies are missing")


    @task(task_id="etl_ebay_order", map_index_template="{{ credentials['userid'] }}")
    def etl_ebay_order(credentials: dict, configs: dict, **kwargs) -> dict:
        from airflow_utils import format_datetime
        validate_cookies(credentials)
        return main(**credentials, **configs,
            start_date = format_datetime(kwargs, subdays=30),
            end_date = format_datetime(kwargs, subdays=0),
        )

    def main(
            cookies: str,
            start_date: str,
            end_date: str,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs,
        ) -> dict:
        from dual_load import merge_table_from_duckdb
        from linkmerce.api.ebay.esmplus import order
        from linkmerce.common.load import DuckDBConnection
        sources = {"order": "esmplus_order", "item": "esmplus_order_item"}

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            order(
                cookies = cookies,
                start_date = start_date,
                end_date = end_date,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            partitions = conn.unique(sources["order"], "DATE(payment_dt)")

            return {
                "context": {
                    "partitions": sorted(map(str, partitions)),
                },
                "params": {
                    "start_date": start_date,
                    "end_date": end_date,
                },
                "results": {
                    "order": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["order"],
                        target_table = tables["order"],
                        **merge["order"],
                        where_clause = conn.expr_datetime_range("T.payment_dt", partitions),
                        execute = bool(partitions),
                    ),
                    "item": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["item"],
                        target_table = tables["item"],
                        **merge["item"],
                    ),
                }
            }


    @task(task_id="generate_dbt_date_range", trigger_rule="all_done")
    def generate_dbt_date_range(results: list[dict]) -> dict:
        from dbt_cosmos import generate_dbt_date_range as generate
        return generate(results, "context.partitions")


    @task.short_circuit(task_id="prepare_dbt_run", ignore_downstream_trigger_rules=False)
    def prepare_dbt_run(ti: TaskInstance, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_dbt_date_range")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False


    # def dbt_bigquery_ebay_order_group() -> DbtTaskGroup:
    #     from dbt_cosmos import dynamic_mapping_dbt_bigquery
    #     return dynamic_mapping_dbt_bigquery(
    #         group_id = "dbt_bigquery_ebay_order",
    #         selector = "ebay_order",
    #         ds_task_id = "generate_dbt_date_range",
    #     )

    # def dbt_postgres_ebay_order_group() -> DbtTaskGroup:
    #     from dbt_cosmos import dynamic_mapping_dbt_postgres
    #     return dynamic_mapping_dbt_postgres(
    #         group_id = "dbt_postgres_ebay_order",
    #         selector = "ebay_order",
    #         ds_task_id = "generate_dbt_date_range",
    #     )

    def dbt_postgres_ebay_order_summary_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_postgres
        return dynamic_mapping_dbt_postgres(
            group_id = "dbt_postgres_ebay_order_summary",
            selector = "ebay_order_summary",
            ds_task_id = "generate_dbt_date_range",
        )


    etl_results = (etl_ebay_order
        .partial(configs=read_configs())
        .expand(credentials=read_credentials()))

    dbt_date_range = generate_dbt_date_range(etl_results)
    # dbt_run = [dbt_bigquery_ebay_order_group(), dbt_postgres_ebay_order_group()]
    dbt_run = dbt_postgres_ebay_order_summary_group()

    dbt_date_range >> prepare_dbt_run() >> dbt_run
