from airflow.sdk import DAG, task
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
        쿠팡 계정 목록을 순회하면서 한 번에 하나씩 [로그인 >> 개별 ETL Dag 실행] 사이클을 반복한다.
        로그인은 쿠팡 윙 인증 후 광고센터 탭으로 전환하여 윙/광고에 대한 쿠키를 한번에 수집한다.

        ## 주의 사항
        쿠팡 로그인 정책 강화로 인해 한 번에 하나의 판매자 계정만 로그인 가능하다.
        따라서, 기존에 쿠키를 저장해두고 병렬로 실행하던 동작을 폐기하고 엄격한 직렬 실행 방식으로 전환했다.

        ## 예외 처리
        - 로그인 실패 시: 해당 업체의 전체 ETL Dag을 건너뛰고 다음 업체를 시도한다.
        - ETL Dag 실패 시: 해당 Dag의 오류를 기록하고 나머지 ETL Dag은 계속 실행한다.

        ## 트리거 대상 Dag
        1. 'coupang_rocket_sales' (윙)
        2. 'coupang_adreport' (광고)
        3. 'coupang_product_option' (윙)
        4. 'coupang_campaign' (광고)
    """).strip(),
) as dag:

    PATH = "coupang.users"

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        from airflow_utils import read_credentials as read
        return read(PATH)


    ETL_DAGS = [
        ("coupang_rocket_sales", 10), # 평균 15초 소요
        ("coupang_adreport", 10), # 평균 15초 소요
        ("coupang_product_option", 30), # 상품 수에 비례 (20초 ~ 2분+)
        ("coupang_campaign", 10), # 평균 10초 소요
    ]

    @task(task_id="etl_coupang_integration")
    def etl_coupang_integration(credentials: list, **kwargs) -> dict:
        from airflow_api import authenticate, trigger_dagrun, wait_for_completion
        from pw_actions import coupang_login
        import logging

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
            exec_info = {"vendor_id": vendor_id, "cookies": dict(), "status": dict()}

            # 2. 쿠팡 윙/광고 로그인
            try:
                exec_info["cookies"] = coupang_login(creds["userid"], creds["passwd"], navigate_to_ads=True)
                exec_info["login"] = "success"
                logger.info(f"[{vendor_id}] Login succeeded")
            except Exception as exception:
                logger.error(f"[{vendor_id}] Login failed: {exception}")
                exec_info["login"] = f"failed: {str(exception)}"
                result[vendor_id] = exec_info
                continue

            # 3. 전체 ETL Dag 흐름 제어
            conf = {
                "vendor_id": vendor_id,
                "cookies": exec_info["cookies"],
                "nca": creds.get("nca", False),
            }

            for dag_id, poke_interval in ETL_DAGS:
                # 4. 개별 ETL Dag 실행 (REST API로 트리거)
                logical_date = pendulum.now("UTC")
                run_id = f"expanded__{i}__{logical_date.isoformat()}"
                trigger_dagrun(dag_id, run_id, access_token, logical_date, conf)
                state = wait_for_completion(dag_id, run_id, access_token, poke_interval, timeout=(poke_interval*20))
                logger.info(f"[{vendor_id}] DAG run {state}: /dags/{dag_id}/runs/{run_id}")
                exec_info["status"][dag_id] = state

            result[vendor_id] = exec_info

        return result


    etl_coupang_integration(credentials=read_credentials())
