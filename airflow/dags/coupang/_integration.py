from airflow.sdk import DAG, task
from airflow.models.dagrun import DagRun
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "coupang",
    schedule = "20 8 * * *",
    start_date = pendulum.datetime(2026, 4, 9, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(hours=1), # Airflow 액세스 토큰의 기본 유효 기간만큼 동작
    catchup = False,
    tags = [
        "priority:high", "coupang:adreport", "coupang:campaign", "coupang:option", "coupang:sales",
        "login:coupang", "schedule:daily", "time:morning"
    ],
    doc_md = dedent("""
        # 쿠팡 윙/광고 통합 ETL 파이프라인

        ## 동작 방식
        쿠팡 계정 목록을 순회하면서 한 번에 하나씩 [로그인 >> 4개의 SubDAG 순차 실행] 사이클을 반복한다.
        로그인은 쿠팡 윙 인증 후 광고센터 탭으로 전환하여 윙/광고에 대한 쿠키를 한번에 수집한다.

        ## 주의 사항
        쿠팡 로그인 정책 강화로 인해 한 번에 하나의 판매자 계정만 로그인 가능하다.
        따라서, 기존에 쿠키를 저장해두고 병렬로 실행하던 동작을 폐기하고 엄격한 직렬 실행 방식으로 전환했다.

        ## 예외 처리
        - 로그인 실패 시: 해당 업체의 전체 SubDAG을 건너뛰고 다음 업체를 시도한다.
        - SubDAG 실패 시: 해당 SubDAG의 오류를 기록하고 나머지 SubDAG은 계속 실행한다.

        ## 트리거 대상 DAG
        1. 'coupang_rocket_sales' (윙)
        2. 'coupang_adreport' (광고)
        3. 'coupang_product_option' (윙)
        4. 'coupang_campaign' (광고)

        ## Manual 실행 시 필터 설정
        Airflow UI에서 DAG을 트리거할 때 {vendor_id: dag_ids} 형식의 Configuration JSON을 전달해
        특정 판매자 계정이나 특정 SubDAG만 선택적으로 실행할 수 있다.
        (설정이 비어있으면 필터하지 않는다.)

        ### 키-값 설명
        - 'vendor_id' (str): 실행할 판매자 계정의 업체코드.
            "*"를 키로 사용하면 모든 계정을 대상으로 동일한 DAG 필터를 적용할 수 있다.
        - 'dag_ids' (list): 실행할 DAG ID 목록.
            "*" 값을 포함하면 모든 DAG을 실행한다.

        ### 사용 예시
        ```json
        // 특정 판매자 계정만 전체 SubDAG 실행
        { "filters": { "A00000001": ["*"] } }

        // 특정 판매자 계정의 특정 SubDAG만 실행
        { "filters": { "A00000001": ["coupang_rocket_sales"] } }

        // 모든 판매자 계정의 특정 SubDAG만 실행
        { "filters": { "*": ["coupang_rocket_sales"] } }
        ```
    """).strip(),
) as dag:

    PATH = "coupang.users"

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_credentials as read
        return read(PATH)


    SUB_DAGS = [
        ("coupang_rocket_sales", 10), # 평균 15초 소요
        ("coupang_adreport", 10), # 평균 15초 소요
        ("coupang_product_option", 30), # 상품 수에 비례 (20초 ~ 2분+)
        ("coupang_campaign", 10), # 평균 10초 소요
    ]

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
        from pw_actions import login_coupang
        import logging
        import time

        filters = dag_run.conf.get("filters") or dict() # vendor-DAG 필터
        logger = logging.getLogger(__name__)
        result = dict()

        # 1. Airflow API 사용을 위한 액세스 토큰 발급 (유효 기간 1시간)
        try:
            access_token = authenticate()
            logger.info("Successfully authenticated with Airflow API")
        except Exception as exception:
            logger.error(f"Failed to authenticate with Airflow API: {exception}")
            raise exception

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

            # 2. 쿠팡 윙/광고 로그인 (최대 3회 재시도)
            error_flag = False
            for _ in range(3):
                try:
                    exec_info["cookies"] = login_coupang(*user_info, navigate_to_ads=True)
                    exec_info["login"] = "success"
                    logger.info(f"[{vendor_id}] Login succeeded")
                    error_flag = False
                    break
                except Exception as exception:
                    error_flag = True
                    logger.error(f"[{vendor_id}] Login failed: {exception}")
                    exec_info["login"] = f"failed: {exception}"
                    time.sleep(60)
            if error_flag:
                result[vendor_id] = exec_info
                continue

            # 3. 전체 SubDAG 흐름 제어
            conf = {
                "vendor_id": vendor_id,
                "cookies": exec_info["cookies"],
                "nca": creds.get("nca", False),
            }

            for dag_id, poke_interval in subdag_ids:
                # 4. 개별 SubDAG 실행 (REST API로 트리거)
                logical_date = pendulum.now("UTC")
                run_id = f"expanded__{i}__{logical_date.isoformat()}"
                trigger_dagrun(dag_id, run_id, access_token, logical_date, conf)
                state = wait_for_completion(dag_id, run_id, access_token, poke_interval, timeout=(poke_interval*20))
                logger.info(f"[{vendor_id}] DAG run {state}: /dags/{dag_id}/runs/{run_id}")
                exec_info["subdags"][dag_id] = state

            result[vendor_id] = exec_info

        return result


    etl_coupang_integration(credentials=read_credentials())
