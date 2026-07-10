"""
# 광고-사방넷 묶음품목 관계 구글시트-DB 동기화 파이프라인

구글 스프레드시트에서 광고 ID와 사방넷 묶음품목 관계 데이터를 읽어서 BigQuery와 PostgreSQL에 덮어쓰기로 적재한다.
primary_key를 기준으로 신규 행이 있다면 후속 dbt 모델을 실행한다.

## Manual 실행 시 dbt 설정
Airflow UI에서 Dag을 트리거할 때 Configuration JSON을 전달해 후속 dbt 모델 실행을 제어할 수 있다.

### 키-값 설명
- 'run' (bool): dbt 모델을 실행할지 여부. 기본값은 'True'

### 사용 예시
```json
// dbt 실행 생략
{ "dbt": { "run": false } }
```
"""

from airflow.models.dagrun import DagRun
from airflow.sdk import DAG, task
from cosmos import DbtTaskGroup
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "sync_gsheets__ads_master",
    schedule = None, # 담당자가 구글 스프레드시트 수정 후 FastAPI를 연결되는 링크를 호출하여 트리거
    start_date = pendulum.datetime(2026, 6, 27, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:medium", "platform:gsheets", "objective:ads", "schedule:none",
        "write:overwrite", "upstream:fastapi", "plugin:dbt"
    ],
) as dag:

    PATH = "gsheets.relation__ad_id_to_sbn_ids"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH)


    @task(task_id="dual_load_from_gsheets")
    def dual_load_from_gsheets(configs: dict) -> dict:
        from dual_load import overwrite_table_from_gsheets
        return {"new_rows": overwrite_table_from_gsheets(**configs)}


    @task.short_circuit(task_id="prepare_dbt_run")
    def prepare_dbt_run(result: dict, dag_run: DagRun) -> bool:
        dbt_conf = (dag_run.conf or dict()).get("dbt") or dict()
        return (dbt_conf.get("run") is not False) and bool(result.get("new_rows"))


    # def dbt_bigquery_coupang_ads_master_group() -> DbtTaskGroup:
    #     from dbt_cosmos import dynamic_mapping_dbt_bigquery
    #     return dynamic_mapping_dbt_bigquery(
    #         group_id = "dbt_bigquery_coupang_ads_master",
    #         selector = "coupang_ads_master",
    #     )


    def dbt_bigquery_google_ads_master_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_google_ads_master",
            selector = "google_ads_master",
        )


    def dbt_bigquery_meta_ads_master_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_meta_ads_master",
            selector = "meta_ads_master",
        )


    def dbt_bigquery_searchad_master_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_searchad_master",
            selector = "searchad_master",
        )


    configs = read_configs()
    sync_result = dual_load_from_gsheets(configs)

    prepare_dbt_run(sync_result) >> [
        # dbt_bigquery_coupang_ads_master_group(),
        dbt_bigquery_google_ads_master_group(),
        dbt_bigquery_meta_ads_master_group(),
        dbt_bigquery_searchad_master_group(),
    ]
