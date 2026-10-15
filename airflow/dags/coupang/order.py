"""
# 쿠팡 마켓플레이스 발주서 ETL 파이프라인

> 안내) 예약 실행이 매시 0분이면 'coupang_api_order_delivery' Dag을 트리거하고, 해당 Dag의 완료를 기다린다.

## 인증(Credentials)
쿠팡 Open API 인증 키(Access Key, Secret Key, 업체 코드)가 필요하다.

## 추출(Extract)
각 업체별 직전에 성공한 Task Instance 반환 값의 'params.end_datetime'부터
현재 실행 시점('data_interval_end')보다 1분 앞선 시점까지를 조회 기간으로 하여
전체 발주서 목록을 수집한다.

Airflow UI에서 Dag을 트리거하면서 Configuration JSON에 ISO 8601 형식의
'start_datetime', 'end_datetime'을 'vendor_id' 각각에 지정하면
해당 값을 기본 수집 기간 대신 적용한다. 모든 채널에 동일한 조회 기간을 적용하고 싶다면
'vendor_id' 키를 '*'로 대체할 수 있다.

채널별 설정에서 'skip'을 True로 지정하면 ETL Task가 Skipped 처리된다.

```json
{
    "vendors": {
        "A00000001": {
            "start_datetime": "2026-09-22T09:00+09:00",
            "end_datetime": "2026-09-22T09:59+09:00",
            "skip": false
        }
    },
    "dbt_run": true
}
```

## 변환(Transform)
JSON 형식의 발주서 내역으로부터 주문 정보, 결제 정보, 상품 정보를 분리해 각각의 DuckDB 테이블에 적재한다.

## 적재(Load)
- 주문 정보, 결제 정보 테이블은 BigQuery/Postgres 테이블 끝에 추가한다.
- 상품 정보 테이블은 대응되는 BigQuery/Postgres 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
- 'dbt_run: true'가 지정되거나 매시 0분의 실행에서, 수집한 발주서 주문일 파티션 범위를 바탕으로 후속 dbt 모델을 실행한다.
- 'dbt_run: false'가 지정되지 않은 모든 실행에서, 수집한 발주서 주문일 파티션 범위를 바탕으로 요약 dbt 모델을 실행한다.
"""

from airflow.sdk import DAG, task
from airflow.models.dagrun import DagRun
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.models.taskinstance import TaskInstance
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from cosmos import DbtTaskGroup
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "coupang_order",
    schedule = MultipleCronTriggerTimetable(
        "*/10 9-19 * * 1-5",
        "0 0-8,20-23 * * 1-5",
        "0 * * * 0,6",
        timezone = "Asia/Seoul",
    ),
    start_date = pendulum.datetime(2026, 10, 12, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    max_active_runs = 1,
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:coupang-api", "objective:sales", "objective:product",
        "credentials:api-key", "schedule:10min", "schedule:hourly",
        "time:morning", "time:afternoon", "time:night", "write:append", "write:merge", "plugin:dbt"
    ],
) as dag:

    PATH = "coupang.api.order"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list[dict]:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]


    @task(task_id="etl_coupang_order", map_index_template="{{ credentials['vendor_id'] }}")
    def etl_coupang_order(credentials: dict, configs: dict, dag_run: DagRun, **kwargs) -> dict:
        from airflow_api import get_next_datetime_range
        from airflow_utils import get_datetime
        conf = (dag_run.conf or dict()).get("vendors") or dict()

        datetime_range = get_next_datetime_range(
            dag_id = dag_run.dag_id,
            task_id = "etl_coupang_order",
            data_interval_end = get_datetime(kwargs),
            rendered_map_index = credentials["vendor_id"],
            states = ["success"],
            format = "YYYY-MM-DDTHH:mmZ",
            timedelta = { "minutes": 1 },
            conf = (conf.get(credentials["vendor_id"]) or conf.get('*') or dict()),
        )
        return main(**credentials, **configs, **datetime_range)

    def main(
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
        from linkmerce.api.coupang.api import order
        from dual_load import load_table_from_duckdb, merge_table_from_duckdb
        sources = {
            "order": "coupang_order",
            "detail": "coupang_order_detail",
            "option": "coupang_order_option",
        }

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            order(
                access_key = access_key,
                secret_key = secret_key,
                vendor_id = vendor_id,
                start_datetime = start_datetime,
                end_datetime = end_datetime,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            return {
                "context": {
                    "partitions": sorted(map(str, conn.unique(sources["order"], "DATE(order_dt)"))),
                },
                "params": {
                    "vendor_id": vendor_id,
                    "start_datetime": start_datetime,
                    "end_datetime": end_datetime,
                },
                "results": {
                    "order": load_table_from_duckdb(
                        connection = conn,
                        source_table = sources["order"],
                        target_table = tables["order"],
                    ),
                    "detail": load_table_from_duckdb(
                        connection = conn,
                        source_table = sources["detail"],
                        target_table = tables["detail"],
                    ),
                    "option": merge_table_from_duckdb(
                        connection = conn,
                        source_table = sources["option"],
                        target_table = tables["option"],
                        **merge["option"],
                    ),
                }
            }


    @task(task_id="trigger_order_delivery", trigger_rule="all_done")
    def trigger_order_delivery(dag_run: DagRun, **kwargs) -> dict:
        from airflow_utils import get_datetime
        return trigger_child_dag("coupang_order_delivery", dag_run.run_id, get_datetime(kwargs))

    def trigger_child_dag(dag_id: str, dag_run_id: str, data_interval_end: pendulum.DateTime) -> dict:
        from airflow_api import authenticate, trigger_dagrun, wait_for_completion, get_task_xcom_values

        if (not dag_run_id.startswith("scheduled__")) or (data_interval_end.minute != 0):
            from airflow.sdk.exceptions import AirflowSkipException
            raise AirflowSkipException(f"'{dag_id}' Dag is triggered only for scheduled runs at minute 0")

        access_token = authenticate()
        run_id = f"expanded__{data_interval_end.isoformat()}"
        conf = {"load_method": "merge"} if data_interval_end.hour == 0 else None
        dag_run_info = {"dag_id": dag_id, "run_id": run_id, "triggered": True}

        trigger_dagrun(dag_id, run_id, access_token, data_interval_end, conf)
        dag_run_info["state"] = wait_for_completion(dag_id, run_id, access_token, poke_interval=15, timeout=60*4)
        dag_run_info["results"] = get_task_xcom_values(dag_id, run_id, f"etl_{dag_id}", access_token)
        return dag_run_info


    @task(task_id="generate_dbt_date_range", trigger_rule="all_done")
    def generate_dbt_date_range(etl_results: list[dict], delivery_dag_run: dict | None) -> dict:
        from dbt_cosmos import generate_dbt_date_range as generate

        etl_results = list(etl_results)
        if isinstance(delivery_dag_run, dict) and isinstance(delivery_dag_run.get("results"), list):
            etl_results += delivery_dag_run["results"]

        return generate(etl_results, "context.partitions")


    # @task.short_circuit(task_id="prepare_dbt_run_1h", ignore_downstream_trigger_rules=False)
    # def prepare_dbt_run_1h(ti: TaskInstance, dag_run: DagRun, **kwargs) -> bool:
    #     from airflow_utils import get_datetime

    #     date_range = ti.xcom_pull(task_ids="generate_dbt_date_range")
    #     if isinstance(date_range, dict):
    #         return bool(
    #                 date_range.get("ds_start_date")
    #             and date_range.get("ds_end_date")
    #             and (((dag_run.conf or dict()).get("dbt_run") is True)
    #                 or (dag_run.run_id.startswith("scheduled__") and (get_datetime(kwargs).minute == 0)))
    #         )
    #     return False


    # def dbt_bigquery_coupang_order_group() -> DbtTaskGroup:
    #     from dbt_cosmos import dynamic_mapping_dbt_bigquery
    #     return dynamic_mapping_dbt_bigquery(
    #         group_id = "dbt_bigquery_coupang_order",
    #         selector = "coupang_order",
    #         ds_task_id = "generate_dbt_date_range",
    #     )

    # def dbt_postgres_coupang_order_1h_group() -> DbtTaskGroup:
    #     from dbt_cosmos import dynamic_mapping_dbt_postgres
    #     return dynamic_mapping_dbt_postgres(
    #         group_id = "dbt_postgres_coupang_order_1h",
    #         selector = "coupang_order",
    #         ds_task_id = "generate_dbt_date_range",
    #     )


    @task.short_circuit(task_id="prepare_dbt_run_10m", ignore_downstream_trigger_rules=False)
    def prepare_dbt_run_10m(ti: TaskInstance, dag_run: DagRun, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_dbt_date_range")
        if isinstance(date_range, dict):
            return bool(
                    date_range.get("ds_start_date")
                and date_range.get("ds_end_date")
                and ((dag_run.conf or dict()).get("dbt_run") is not False)
            )
        return False

    def dbt_postgres_coupang_order_10m_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_postgres
        return dynamic_mapping_dbt_postgres(
            group_id = "dbt_postgres_coupang_order_10m",
            selector = "coupang_order_summary",
            ds_task_id = "generate_dbt_date_range",
        )


    @task(task_id="finalize_dag_run", trigger_rule="all_done")
    def finalize_dag_run(delivery_dag_run: dict | None, ti: RuntimeTaskInstance):
        from airflow.exceptions import AirflowException
        from dbt_cosmos import raise_on_failure

        if isinstance(delivery_dag_run, dict) and delivery_dag_run.get("triggered"):
            if delivery_dag_run.get("state") != "success":
                raise AirflowException(
                    f"Triggered Dag '{delivery_dag_run['dag_id']}' finished "
                    f"with state '{delivery_dag_run.get('state')}'"
                    )
        raise_on_failure(ti)


    etl_results = (etl_coupang_order
        .partial(configs=read_configs())
        .expand(credentials=read_credentials()))

    delivery_dag_run = trigger_order_delivery()
    etl_results >> delivery_dag_run

    dbt_date_range = generate_dbt_date_range(etl_results, delivery_dag_run)

    # prepare_1h = prepare_dbt_run_1h()
    # dbt_run_1h = [dbt_bigquery_coupang_order_group(), dbt_postgres_coupang_order_1h_group()]

    prepare_10m = prepare_dbt_run_10m()
    dbt_run_10m = dbt_postgres_coupang_order_10m_group()

    # dbt_date_range >> [prepare_1h, prepare_10m]
    # prepare_1h >> dbt_run_1h
    # prepare_10m >> dbt_run_10m

    finalize = finalize_dag_run(delivery_dag_run)
    # [*dbt_run_1h, dbt_run_10m] >> finalize
    dbt_date_range >> prepare_10m >> dbt_run_10m >> finalize
