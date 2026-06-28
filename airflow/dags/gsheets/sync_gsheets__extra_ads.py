"""
# 기타광고 구글시트-DB 동기화 파이프라인

구글 스프레드시트에서 기타광고 데이터를 읽어서 BigQuery와 PostgreSQL에 덮어쓰기로 적재한다.
primary_key를 기준으로 신규 행이 있다면 날짜 파티션 범위를 바탕으로 후속 dbt 모델을 실행한다.

## Manual 실행 시 dbt 설정
Airflow UI에서 Dag을 트리거할 때 Configuration JSON을 전달해 후속 dbt 모델 실행을 제어할 수 있다.

### 키-값 설명
- 'date_range' (dict): dbt 모델 실행 시 전달할 기간 변수.
- 'run' (bool): dbt 모델을 실행할지 여부. 기본값은 'True'

### 사용 예시
```json
// 특정 기간에 대해 dbt 모델 실행
{ "dbt": { "ds_start_date": "2026-06-27", "ds_end_date": "2026-06-27" } }

// dbt 실행 생략
{ "dbt": { "run": false } }
```
"""

from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.sdk import DAG, task
from cosmos import DbtTaskGroup
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "sync_gsheets__extra_ads",
    schedule = None, # 담당자가 구글 스프레드시트 수정 후 FastAPI를 연결되는 링크를 호출하여 트리거
    start_date = pendulum.datetime(2026, 6, 27, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    doc_md = __doc__,
    tags = ["priority:medium", "platform:gsheets", "objective:ads", "schedule:none", "write:overwrite", "plugin:dbt"],
) as dag:

    PATH = "gsheets.core__extra_ads"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH)


    @task(task_id="dual_load_from_gsheets")
    def dual_load_from_gsheets(configs: dict) -> dict:
        from dual_load import overwrite_table_from_gsheets

        new_rows = overwrite_table_from_gsheets(**configs)
        return {
            "new_rows": new_rows,
            "new_partitions": sorted({row["ymd"] for row in new_rows}),
        }


    @task(task_id="generate_dbt_date_range")
    def generate_dbt_date_range(result: dict, dag_run: DagRun) -> dict:
        dbt_conf = (dag_run.conf or dict()).get("dbt") or dict()
        if dbt_conf.get("run") is False:
            return dict()
        if isinstance(date_range := dbt_conf.get("date_range"), dict):
            return date_range

        from dbt_cosmos import generate_dbt_date_range as generate
        return generate(result, "new_partitions")


    @task.short_circuit(task_id="prepare_dbt_run")
    def prepare_dbt_run(ti: TaskInstance) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_dbt_date_range")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False


    def dbt_bigquery_extra_ads_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_extra_ads",
            selector = "sync_gsheets__extra_ads",
            ds_task_id = "generate_dbt_date_range",
        )


    configs = read_configs()
    sync_result = dual_load_from_gsheets(configs)

    dbt_date_range = generate_dbt_date_range(sync_result)
    dbt_run = dbt_bigquery_extra_ads_group()

    dbt_date_range >> prepare_dbt_run() >> dbt_run
