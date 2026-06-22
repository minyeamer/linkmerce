"""
# 쿠팡 Wing/광고 통합 ETL 파이프라인

## 동작 방식
쿠팡 계정 목록을 순회하면서 한 번에 하나씩 [로그인 >> 5개의 SubDag 순차 실행] 사이클을 반복한다.
(단, Dag ID가 'scheduled__'로 시작하는 Dag run은 실행 시간에 따라 일부 Dag만 선택적으로 실행된다.)
로그인은 쿠팡 Wing 인증 후 광고센터 탭으로 전환하여 Wing/광고에 대한 쿠키를 한번에 수집한다.

## 주의 사항
쿠팡 로그인 정책 강화로 인해 한 번에 하나의 판매자 계정만 로그인 가능하다.
따라서, 기존에 쿠키를 저장해두고 병렬로 실행하던 동작을 폐기하고 엄격한 직렬 실행 방식으로 전환했다.

## 예외 처리
- 로그인 실패 시: 해당 업체의 전체 SubDag을 건너뛰고 다음 업체를 시도한다.
- SubDag 실패 시: 해당 SubDag의 오류를 기록하고 나머지 SubDag은 계속 실행한다.

## 트리거 대상 Dag
1. 'coupang_adreport' (광고)
2. 'coupang_campaign' (광고)
3. 'coupang_inventory' (Wing)
4. 'coupang_product_option' (Wing)
5. 'coupang_rocket_sales' (Wing)

## 적재 후처리
실행된 SubDag 중 dbt 후속 모델이 연결된 대상만 결과 파티션을 수집한 뒤 dbt 모델을 실행한다.

## Manual 실행 시 필터 설정
Airflow UI에서 Dag을 트리거할 때 {vendor_id: dag_ids} 형식의 Configuration JSON을 전달해
특정 판매자 계정이나 특정 SubDag만 선택적으로 실행할 수 있다.
(설정이 비어있으면 필터하지 않는다.)

### 키-값 설명
- 'vendor_id' (str): 실행할 판매자 계정의 업체코드.
    "*"를 키로 사용하면 모든 계정을 대상으로 동일한 Dag 필터를 적용할 수 있다.
- 'dag_ids' (list): 실행할 Dag ID 목록.
    "*" 값을 포함하면 모든 Dag을 실행한다.

### 사용 예시
```json
// 특정 판매자 계정만 전체 SubDag 실행
{ "filters": { "A00000001": ["*"] } }

// 특정 판매자 계정의 특정 SubDag만 실행
{ "filters": { "A00000001": ["coupang_rocket_sales"] } }

// 모든 판매자 계정의 특정 SubDag만 실행
{ "filters": { "*": ["coupang_rocket_sales"] } }
```
"""

from airflow.sdk import DAG, task
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.exceptions import AirflowException
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from cosmos import DbtTaskGroup
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "coupang",
    schedule = MultipleCronTriggerTimetable(
        "0 9,11,23 * * *",
        "30 17 * * *",
        timezone = "Asia/Seoul",
    ),
    start_date = pendulum.datetime(2026, 4, 9, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(hours=1), # Airflow 액세스 토큰의 기본 유효 기간만큼 동작
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:coupang-wing", "platform:coupang-ads", "objective:login",
        "objective:ads", "objective:sales", "objective:product", "credentials:userid",
        "schedule:daily", "time:morning", "time:afternoon", "time:night",
        "plugin:playwright", "plugin:rest-api", "plugin:dbt"
    ],
) as dag:

    PATH = "coupang.vendor"

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_credentials as read
        return read(PATH)


    SUB_DAGS = [
        # Wing
        ("coupang_rocket_sales", 10), # 평균 15초 소요
        ("coupang_inventory", 10), # 평균 10초 소요
        ("coupang_product_option", 30), # 상품 수에 비례 (20초 ~ 2분+)
        # 광고
        ("coupang_adreport", 10), # 평균 15초 소요
        ("coupang_campaign", 10), # 평균 10초 소요
    ]

    SCHEDULES = {
        9: ["coupang_rocket_sales", "coupang_adreport"],
        11: ["coupang_inventory"],
        17: ["coupang_inventory"],
        23: ["coupang_product_option", "coupang_campaign"],
    }

    def filter_subdag_ids(vendor_id: str, filters: dict) -> list[tuple[str, int]]:
        DAG_ID = 0
        if not filters:
            return SUB_DAGS
        elif vendor_id in filters:
            if "*" in filters[vendor_id]:
                return SUB_DAGS
            return [pair for pair in SUB_DAGS if pair[DAG_ID] in filters[vendor_id]]
        elif "*" in filters:
            if "*" in filters["*"]:
                return SUB_DAGS
            return [pair for pair in SUB_DAGS if pair[DAG_ID] in filters["*"]]
        return list()

    @task(task_id="etl_coupang_integration")
    def etl_coupang_integration(credentials: list, dag_run: DagRun, **kwargs) -> dict:
        from airflow_api import authenticate, trigger_dagrun, wait_for_completion
        from airflow_utils import get_datetime
        from pw_actions import login_coupang
        import logging
        import time

        logger = logging.getLogger(__name__)
        start_time = time.time()
        data_interval_end = get_datetime(kwargs)

        # 1. 스케줄 실행 여부 확인 및 실행할 SubDag 목록 결정
        if dag_run.run_id.startswith("scheduled__"):
            hour = data_interval_end.hour
            filters = {"*": SCHEDULES[hour]} # `SCHEDULES`에 없는 시각은 KeyError 발생
            logger.info(f"Scheduled run at {hour}h: {', '.join(SCHEDULES[hour])}")
        else:
            filters = dag_run.conf.get("filters") or dict()

        # 2. Airflow API 사용을 위한 액세스 토큰 발급 (유효 기간 1시간)
        try:
            access_token = authenticate()
            logger.info("Successfully authenticated with Airflow API")
        except Exception as exception:
            logger.error(f"Failed to authenticate with Airflow API: {exception}")
            raise exception

        results = {
            "context": {
                "access_token": access_token,
                "login_failed_count": 0,
                "subdag_failed_count": 0,
            }
        }

        for i, creds in enumerate(credentials):
            if not (isinstance(creds, dict) and ("vendor_id" in creds)):
                continue
            vendor_id = creds["vendor_id"]
            subdag_ids = filter_subdag_ids(vendor_id, filters)

            if not subdag_ids:
                logger.info(f"[{vendor_id}] Skipped (not in filters)")
                continue

            exec_info = {"vendor_id": vendor_id, "cookies": dict(), "login": None, "subdags": dict()}
            user_info = (creds["userid"], creds["passwd"])

            # 3. 쿠팡 Wing/광고 로그인 (최대 10회 재시도)
            login_error_flag = True
            for retry in range(1, 10+1):
                try:
                    exec_info["cookies"] = login_coupang(*user_info, navigate_to_ads=True, timeout=(30*retry))
                    exec_info["login"] = "success"
                    logger.info(f"[{vendor_id}] Login succeeded")
                    login_error_flag = False
                    break
                except Exception as exception:
                    logger.error(f"[{vendor_id}] Login failed: {exception}")
                    exec_info["login"] = f"failed: {exception}"
                    time.sleep(1)
            if login_error_flag:
                results["context"]["login_failed_count"] += 1
                results[vendor_id] = exec_info
                continue

            # 4. 전체 SubDag 흐름 제어
            conf = {
                "vendor_id": vendor_id,
                "cookies": exec_info["cookies"],
                "nca": creds.get("nca", False),
            }

            for dag_id, poke_interval in subdag_ids:
                # 5. 개별 SubDag 실행 (REST API로 트리거)
                elapsed_seconds = int(time.time() - start_time)
                logical_date = data_interval_end.add(seconds=elapsed_seconds)
                run_id = f"expanded__{i}__{logical_date.isoformat()}"

                trigger_dagrun(dag_id, run_id, access_token, logical_date, conf)
                state = wait_for_completion(dag_id, run_id, access_token, poke_interval, timeout=(poke_interval*20))
                logger.info(f"[{vendor_id}] Dag run {state}: /dags/{dag_id}/runs/{run_id}")
                exec_info["subdags"][dag_id] = {"run_id": run_id, "state": state}
                results["context"]["subdag_failed_count"] += int(state != "success")

            results[vendor_id] = exec_info

        return results


    def generate_dbt_date_range(results: dict, subdag_id: str) -> dict:
        """SubDag에 대한 파티션 범위를 계산한다."""
        from airflow_api import get_xcom_value
        from dbt_cosmos import generate_dbt_date_range as generate
        import logging
        import time

        logger = logging.getLogger(__name__)

        # 1. ETL 작업에서 발급한 `access_token` 재사용
        access_token = results["context"]["access_token"]
        return_values = list()

        for vendor_id, vendor_result in results.items():
            subdag_results = (vendor_result or dict()).get("subdags")
            for dag_id, result in (subdag_results or dict()).items():
                # 2. 전체 실행 결과로부터 `dag_id`에 해당하는 SubDag의 실행 내역 확인
                if dag_id == subdag_id:
                    try:
                        # 3. SubDag의 실행 결과(JSON)를 API로 요청
                        return_values.append(
                            get_xcom_value(dag_id, result["run_id"], f"etl_{dag_id}", access_token)
                        ); time.sleep(.5)
                    except Exception as exception:
                        if isinstance(result, dict) and result.get("run_id"):
                            dag_run_id = dag_id + '/' + result["run_id"]
                        else:
                            dag_run_id = dag_id + '/' + 'unknown'
                        logger.warning(f"[{vendor_id}] XCom read failed: {dag_run_id} ({exception})")

        # 4. SubDags의 실행 결과로부터 파티션 범위를 계산하여 반환
        return generate(return_values, "context.partitions")


    # 1. subdag_id = "coupang_rocket_sales"

    @task(task_id="generate_dbt_date_range__rocket_sales", trigger_rule="all_done")
    def generate_dbt_date_range__rocket_sales(results: dict) -> dict:
        return generate_dbt_date_range(results, subdag_id="coupang_rocket_sales")

    @task.short_circuit(task_id="prepare_dbt_run__rocket_sales", ignore_downstream_trigger_rules=False)
    def prepare_dbt_run__rocket_sales(ti: TaskInstance, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_dbt_date_range__rocket_sales")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False

    def dbt_bigquery_coupang_rocket_sales_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_coupang_rocket_sales",
            selector = "coupang_rocket_sales",
            ds_task_id = "generate_dbt_date_range__rocket_sales",
        )

    # 4. subdag_id = "coupang_adreport"

    @task(task_id="generate_dbt_date_range__adreport", trigger_rule="all_done")
    def generate_dbt_date_range__adreport(results: dict) -> dict:
        return generate_dbt_date_range(results, subdag_id="coupang_adreport")

    @task.short_circuit(task_id="prepare_dbt_run__adreport", ignore_downstream_trigger_rules=False)
    def prepare_dbt_run__adreport(ti: TaskInstance, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_dbt_date_range__adreport")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False

    def dbt_bigquery_coupang_adreport_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_coupang_adreport",
            selector = "coupang_adreport",
            ds_task_id = "generate_dbt_date_range__adreport",
        )

    # 5. subdag_id = "coupang_campaign"

    @task(task_id="generate_dbt_date_range__campaign", trigger_rule="all_done")
    def generate_dbt_date_range__campaign(results: dict) -> dict:
        return generate_dbt_date_range(results, subdag_id="coupang_campaign")

    @task.short_circuit(task_id="prepare_dbt_run__campaign", ignore_downstream_trigger_rules=False)
    def prepare_dbt_run__campaign(ti: TaskInstance, **kwargs) -> bool:
        date_range = ti.xcom_pull(task_ids="generate_dbt_date_range__campaign")
        if isinstance(date_range, dict):
            return bool(date_range.get("ds_start_date") and date_range.get("ds_end_date"))
        return False

    def dbt_bigquery_coupang_campaign_group() -> DbtTaskGroup:
        from dbt_cosmos import dynamic_mapping_dbt_bigquery
        return dynamic_mapping_dbt_bigquery(
            group_id = "dbt_bigquery_coupang_campaign",
            selector = "coupang_campaign",
            ds_task_id = "generate_dbt_date_range__campaign",
        )


    etl_results = etl_coupang_integration(credentials=read_credentials())

    # 1. subdag_id = "coupang_rocket_sales"

    dbt_date_range__rocket_sales = generate_dbt_date_range__rocket_sales(etl_results)
    dbt_run__rocket_sales = dbt_bigquery_coupang_rocket_sales_group()

    dbt_date_range__rocket_sales >> prepare_dbt_run__rocket_sales() >> dbt_run__rocket_sales

    # 4. subdag_id = "coupang_adreport"

    dbt_date_range__adreport = generate_dbt_date_range__adreport(etl_results)
    dbt_run__adreport = dbt_bigquery_coupang_adreport_group()

    dbt_date_range__adreport >> prepare_dbt_run__adreport() >> dbt_run__adreport

    # 5. subdag_id = "coupang_campaign"

    dbt_date_range__campaign = generate_dbt_date_range__campaign(etl_results)
    dbt_run__campaign = dbt_bigquery_coupang_campaign_group()

    dbt_date_range__campaign >> prepare_dbt_run__campaign() >> dbt_run__campaign

    # Finalize Dag run

    @task(task_id="finalize_dag_run", trigger_rule="all_done")
    def finalize_dag_run(results: dict, ti: RuntimeTaskInstance):
        context = results.get("context") or dict()

        for key, name in [("login_failed_count", "login"), ("subdag_failed_count", "SubDag")]:
            if (count := context.get(key, 0)):
                n_failed_task_s = "{} {} task{}".format(count, name, 's' if count > 1 else '')
                raise AirflowException(f"{n_failed_task_s} failed before Dag completion.")

        from dbt_cosmos import raise_on_failure
        raise_on_failure(ti)

    [dbt_run__rocket_sales, dbt_run__adreport, dbt_run__campaign] >> finalize_dag_run(etl_results)
