"""
# 쿠팡 마켓플레이스 반품, 취소 및 교환 요청 ETL 파이프라인

## 인증(Credentials)
인증 정보 파일의 'coupang.api' 목록에서 access_key, secret_key, vendor_id를 읽는다.

## 추출(Extract)
기본적으로 현재 실행 시점('data_interval_end')의 30일 전부터 1분 전까지를 조회 기간으로 설정하고,
반품 및 취소 요청, 반품철회 이력, 교환 요청을 병렬로 수집한다.

Airflow UI에서 Dag을 트리거하면서 Configuration JSON에 ISO 8601 형식의
'start_datetime', 'end_datetime'을 'vendor_id' 각각에 지정하면
해당 값을 조회 기간으로 적용한다. 모든 업체에 동일한 조회 기간을 적용하고 싶다면
'vendor_id' 키를 '*'로 대체할 수 있다.

업체별 설정에서 'skip'을 True로 지정하면 ETL Task가 Skipped 처리된다.

```json
{
    "vendors": {
        "A00000001": {
            "start_datetime": "2026-10-02T09:00+09:00",
            "end_datetime": "2026-10-31T09:59+09:00",
            "skip": false
        }
    }
}
```

## 변환(Transform)
각각의 JSON 응답 본문을 DuckDB 테이블에 적재하고, Postgres의 반품 및 교환 요청과 주문을
결합하여 클레임 데이터를 생성한다.

## 적재(Load)
상품별 반품 및 취소 요청, 반품철회 이력, 상품별 교환 요청을
각각 대응되는 BigQuery/Postgres 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
이후 Postgres에서 주문일시를 결합한 클레임 데이터를 생성하고,
결과에 포함된 주문일 파티션만 BigQuery/Postgres의 'coupang.order_claim' 테이블에 병합한다.
"""

from airflow.sdk import DAG, task
from airflow.models.dagrun import DagRun
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.models.taskinstance import TaskInstance
from cosmos import DbtTaskGroup
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "coupang_order_claim",
    schedule = "30 0 * * *",
    start_date = pendulum.datetime(2026, 11, 2, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:coupang-api", "objective:sales", "credentials:api-key",
        "schedule:daily", "time:night", "write:merge", "upstream:dagrun"
    ],
) as dag:

    PATH = "coupang.api.claim"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list[dict]:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    def get_datetime_range(
            vendor_id: str,
            dag_run: DagRun,
            kwargs: dict,
            format: str = "YYYY-MM-DDTHH:mm:ss",
        ) -> dict[str, str]:
        from airflow.sdk.exceptions import AirflowSkipException
        from airflow_utils import get_datetime
        conf = (dag_run.conf or dict()).get("vendors") or dict()
        vendor_conf = conf.get(vendor_id) or conf.get("*") or dict()
        data_interval_end = get_datetime(kwargs)

        if vendor_conf.get("skip") is True:
            from airflow.sdk.exceptions import AirflowSkipException
            raise AirflowSkipException(f"Task skipped '{vendor_id}' because the 'skip' flag is enabled")

        if "end_datetime" in vendor_conf:
            end_datetime = pendulum.parse(vendor_conf["end_datetime"]).in_timezone("Asia/Seoul")
        else:
            end_datetime = data_interval_end.subtract(minutes=1)

        if "start_datetime" in vendor_conf:
            start_datetime = pendulum.parse(vendor_conf["start_datetime"]).in_timezone("Asia/Seoul")
        else:
            start_datetime = end_datetime.subtract(days=30, minutes=-1)

        return {
            "start_datetime": start_datetime.format(format),
            "end_datetime": end_datetime.format(format),
        }


    @task(task_id="etl_return_request", map_index_template="{{ credentials['vendor_id'] }}")
    def etl_return_request(credentials: dict, configs: dict, dag_run: DagRun, **kwargs) -> dict:
        datetime_format = "YYYY-MM-DDTHH:mm"
        datetime_range = get_datetime_range(credentials["vendor_id"], dag_run, kwargs, datetime_format)
        return main_return_request(**credentials, **datetime_range, **configs)

    def main_return_request(
            access_key: str,
            secret_key: str,
            vendor_id: str,
            start_datetime: str,
            end_datetime: str,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.api import return_request
        from dual_load import merge_table_from_duckdb
        source = "coupang_return_request_item"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            return_request(
                access_key = access_key,
                secret_key = secret_key,
                vendor_id = vendor_id,
                start_datetime = start_datetime,
                end_datetime = end_datetime,
                cancel_type = "ALL",
                status = "ALL",
                connection = conn,
                progress = False,
                return_type = "none",
            )

            partitions = conn.unique(source, "DATE(register_dt)")

            return {
                "context": {
                    "partitions": sorted(map(str, partitions)),
                },
                "params": {
                    "vendor_id": vendor_id,
                    "start_datetime": start_datetime,
                    "end_datetime": end_datetime,
                    "cancel_type": ["RETURN", "CANCEL"],
                    "status": ["RU", "UC", "CC", "PR"],
                },
                "result": merge_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables["return_request"],
                    **merge["return_request"],
                    where_clause = conn.expr_datetime_range("T.register_dt", partitions),
                    execute = bool(partitions),
                ),
            }


    @task(task_id="etl_return_withdraw", map_index_template="{{ credentials['vendor_id'] }}")
    def etl_return_withdraw(credentials: dict, configs: dict, dag_run: DagRun, **kwargs) -> dict:
        date_format = "YYYY-MM-DD"
        date_range = get_datetime_range(credentials["vendor_id"], dag_run, kwargs, date_format)
        return main_return_withdraw(**credentials, **date_range, **configs)

    def main_return_withdraw(
            access_key: str,
            secret_key: str,
            vendor_id: str,
            start_datetime: str,
            end_datetime: str,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.api import return_withdraw
        from dual_load import merge_table_from_duckdb
        source = "coupang_return_withdraw"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            return_withdraw(
                access_key = access_key,
                secret_key = secret_key,
                vendor_id = vendor_id,
                start_date = start_datetime,
                end_date = end_datetime,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            partitions = conn.unique(source, "DATE(cancel_dt)")

            return {
                "context": {
                    "partitions": sorted(map(str, partitions)),
                },
                "params": {
                    "vendor_id": vendor_id,
                    "start_datetime": start_datetime,
                    "end_datetime": end_datetime,
                },
                "result": merge_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables["return_withdraw"],
                    **merge["return_withdraw"],
                    where_clause = conn.expr_datetime_range("T.cancel_dt", partitions),
                    execute = bool(partitions),
                ),
            }


    @task(task_id="etl_exchange_request", map_index_template="{{ credentials['vendor_id'] }}")
    def etl_exchange_request(credentials: dict, configs: dict, dag_run: DagRun, **kwargs) -> dict:
        datetime_format = "YYYY-MM-DDTHH:mm:ss"
        datetime_range = get_datetime_range(credentials["vendor_id"], dag_run, kwargs, datetime_format)
        return main_exchange_request(**credentials, **datetime_range, **configs)

    def main_exchange_request(
            access_key: str,
            secret_key: str,
            vendor_id: str,
            start_datetime: str,
            end_datetime: str,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.coupang.api import exchange_request
        from dual_load import merge_table_from_duckdb
        source = "coupang_exchange_request_item"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            exchange_request(
                access_key = access_key,
                secret_key = secret_key,
                vendor_id = vendor_id,
                start_datetime = start_datetime,
                end_datetime = end_datetime,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            partitions = conn.unique(source, "DATE(register_dt)")

            return {
                "context": {
                    "partitions": sorted(map(str, partitions)),
                },
                "params": {
                    "vendor_id": vendor_id,
                    "start_datetime": start_datetime,
                    "end_datetime": end_datetime,
                    "status": None,
                },
                "results": merge_table_from_duckdb(
                    connection = conn,
                    source_table = source,
                    target_table = tables["exchange_request"],
                    **merge["exchange_request"],
                    where_clause = conn.expr_datetime_range("T.register_dt", partitions),
                    execute = bool(partitions),
                ),
            }


    @task(task_id="generate_claim_date_range", trigger_rule="all_done")
    def generate_claim_date_range(
            return_results: list[dict],
            withdraw_results: list[dict],
            exchange_results: list[dict],
        ) -> dict:
        from dbt_cosmos import generate_dbt_date_range as generate
        etl_results = list(return_results or []) + list(withdraw_results or []) + list(exchange_results or [])
        return generate(etl_results, "context.partitions")


    @task.short_circuit(task_id="prepare_claim_run")
    def prepare_claim_run(ti: TaskInstance, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_claim_date_range")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False


    @task(task_id="build_return_claim")
    def build_return_claim(date_range: dict, configs: dict) -> dict:
        return build_order_claim("return", date_range, **configs)

    @task(task_id="build_exchange_claim")
    def build_exchange_claim(date_range: dict, configs: dict) -> dict:
        return build_order_claim("exchange", date_range, **configs)

    def build_order_claim(
            claim_type: str,
            date_range: dict,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from airflow.sdk import Connection
        from dual_load import merge_table_from_duckdb
        from linkmerce.common.load import DuckDBConnection
        staging_table = f"coupang_{claim_type}_claim"

        postgres = Connection.get("postgres")
        dsn = f"postgresql://{postgres.login}:{postgres.password}@{postgres.host}:{postgres.port}/{postgres.schema}"
        db_name = "db"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            conn.execute("INSTALL postgres")
            conn.execute("LOAD postgres")
            conn.execute(f"ATTACH '{dsn}' AS {db_name} (TYPE postgres, READ_ONLY)")
            claim_query = exchange_claim_query if claim_type == "exchange" else return_claim_query
            conn.execute(f"CREATE TABLE {staging_table} AS {claim_query(db_name, **date_range)}")

            partitions = conn.unique(staging_table, "DATE(order_dt)")

            return {
                "context": {
                    "partitions": sorted(map(str, partitions)),
                },
                "result": merge_table_from_duckdb(
                    connection = conn,
                    source_table = staging_table,
                    target_table = tables["order_claim"],
                    **merge["order_claim"],
                    where_clause = conn.expr_datetime_range("T.order_dt", partitions),
                    execute = bool(partitions),
                ),
            }

    def return_claim_query(db_name: str, ds_start_date: str, ds_end_date: str) -> str:
        return dedent(f"""
            SELECT
                claim.vendor_id
                , claim.receipt_id AS claim_id
                , claim.shipment_box_id
                , claim.order_id
                , claim.option_id
                , claim.receipt_type AS claim_type
                , COALESCE(claim.receipt_status = 4, FALSE)
                    AND withdraw.cancel_dt IS NULL AS is_success
                , claim.cancel_count AS quantity
                , ord.order_dt
                , claim.register_dt
                , withdraw.cancel_dt
            FROM {db_name}.coupang.return_request AS claim
            INNER JOIN {db_name}.coupang.order AS ord
                ON claim.vendor_id = ord.vendor_id
                AND claim.shipment_box_id = ord.shipment_box_id
            LEFT JOIN (
                SELECT
                    vendor_id
                    , order_id
                    , option_id
                    , MAX(cancel_dt) AS cancel_dt
                FROM {db_name}.coupang.return_withdraw
                WHERE cancel_dt >= '{ds_start_date}'::timestamp without time zone
                    AND cancel_dt < ('{ds_end_date}'::date + 1)::timestamp without time zone
                GROUP BY vendor_id, order_id, option_id
            ) AS withdraw
                ON claim.vendor_id = withdraw.vendor_id
                AND claim.order_id = withdraw.order_id
                AND claim.option_id = withdraw.option_id
            WHERE claim.register_dt >= '{ds_start_date}'::timestamp without time zone
                AND claim.register_dt < ('{ds_end_date}'::date + 1)::timestamp without time zone
        """).strip()

    def exchange_claim_query(db_name: str, ds_start_date: str, ds_end_date: str) -> str:
        return dedent(f"""
            SELECT
                claim.vendor_id
                , claim.exchange_id AS claim_id
                , claim.shipment_box_id
                , claim.order_id
                , claim.order_option_id AS option_id
                , 2::smallint AS claim_type
                , claim.exchange_status = 2 AS is_success
                , claim.exchange_count AS quantity
                , ord.order_dt
                , claim.register_dt
                , NULL::timestamp without time zone AS cancel_dt
            FROM {db_name}.coupang.exchange_request AS claim
            INNER JOIN {db_name}.coupang.order AS ord
                ON claim.vendor_id = ord.vendor_id
                AND claim.shipment_box_id = ord.shipment_box_id
            WHERE claim.register_dt >= '{ds_start_date}'::timestamp without time zone
                AND claim.register_dt < ('{ds_end_date}'::date + 1)::timestamp without time zone
        """).strip()


    @task(task_id="generate_dbt_date_range")
    def generate_dbt_date_range(return_result: dict, exchange_result: dict) -> dict:
        from dbt_cosmos import generate_dbt_date_range as generate
        return generate([return_result, exchange_result], "context.partitions")


    @task.short_circuit(task_id="prepare_dbt_run")
    def prepare_dbt_run(ti: TaskInstance, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_dbt_date_range")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False


    def dbt_postgres_coupang_order_summary_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_postgres
        return dynamic_mapping_dbt_postgres(
            group_id = "dbt_postgres_coupang_order_summary",
            selector = "coupang_order_summary",
            ds_task_id = "generate_dbt_date_range",
        )


    @task(task_id="finalize_dag_run", trigger_rule="all_done")
    def finalize_dag_run(ti: RuntimeTaskInstance):
        from dbt_cosmos import raise_on_failure
        raise_on_failure(ti)


    configs = read_configs()
    credentials = read_credentials()

    etl_return_results = (etl_return_request
        .partial(configs=configs)
        .expand(credentials=credentials))

    etl_withdraw_results = (etl_return_withdraw
        .partial(configs=configs)
        .expand(credentials=credentials))

    etl_exchange_results = (etl_exchange_request
        .partial(configs=configs)
        .expand(credentials=credentials))

    claim_date_range = generate_claim_date_range(etl_return_results, etl_withdraw_results, etl_exchange_results)
    etl_return_claims = build_return_claim(claim_date_range, configs)
    etl_exchange_claims = build_exchange_claim(claim_date_range, configs)

    claim_date_range >> prepare_claim_run() >> [etl_return_claims, etl_exchange_claims]

    dbt_date_range = generate_dbt_date_range(etl_return_claims, etl_exchange_claims)
    dbt_run = dbt_postgres_coupang_order_summary_group()

    dbt_date_range >> prepare_dbt_run() >> dbt_run >> finalize_dag_run()
