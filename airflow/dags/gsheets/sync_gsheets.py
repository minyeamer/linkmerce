"""
# 구글시트-DB 동기화 파이프라인

구글 스프레드시트에서 테이블 데이터를 읽어서 BigQuery와 PostgreSQL에 덮어쓰기로 적재한다.

다음 'task_id'를 지원한다.
- core__delivery_group
- core__item
- core__expense
- core__extra_ads
- core__extra_profit
- core__extra_sales
- core__opex
- core__order_status
- coupang__vendor
- coupang_rfm__inventory_exp
- ecount__schedule
- google_ads__account
- meta_ads__account
- naver_shp__category
- naver_shp__keyword
- relation__ad_id_to_sbn_ids
- relation__cpg_opt_to_sbn_ids
- relation__nsh_kwd_to_prd_id
- relation__smt_opt_to_sbn_ids
- relation__smt_prd_to_sbn_ids
- sabangnet__account
- sabangnet__shop
- searchad__account
- smartstore__channel
- ss_hcenter__category_group
- ss_hcenter__mall

## Manual 실행 시 설정
Airflow UI에서 Dag을 트리거할 때 Configuration JSON을 전달해 'task_ids'를 전달할 수 있다.

### 사용 예시
```json
{ "task_ids": [ "core__item", "core__opex" ] }
```
"""

from airflow.sdk import DAG, task
from airflow.models.dagrun import DagRun
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "sync_gsheets",
    schedule = None, # 담당자가 구글 스프레드시트 수정 후 FastAPI를 연결되는 링크를 호출하여 트리거
    start_date = pendulum.datetime(2026, 6, 27, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:medium", "platform:gsheets", "schedule:none",
        "write:overwrite", "upstream:fastapi", "plugin:dbt"
    ],
) as dag:

    PATH = "gsheets"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs(dag_run: DagRun) -> list:
        from airflow_utils import read_config
        task_ids: list[str] = dag_run.conf.get("task_ids") or list()
        tasks: dict[str, dict] = read_config(PATH)
        return [{"task_id": task_id, **tasks[task_id]} for task_id in task_ids if task_id in tasks]


    @task(task_id="dual_load_from_gsheets", map_index_template="{{ configs['task_id'] }}")
    def dual_load_from_gsheets(configs: dict) -> list:
        from dual_load import overwrite_table_from_gsheets
        payload = dict(configs)
        payload.pop("task_id", None)
        return overwrite_table_from_gsheets(**payload)


    dual_load_from_gsheets.expand(configs=read_configs())
