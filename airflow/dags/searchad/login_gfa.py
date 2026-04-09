from airflow.sdk import DAG, task
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "searchad_login_gfa",
    schedule = "0 5 * * *",
    start_date = pendulum.datetime(2026, 4, 9, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    tags = ["priority:high", "searchad:cookies", "login:gfa", "schedule:daily", "time:morning"],
    doc_md = dedent("""
        # 네이버 성과형 디스플레이 광고 로그인 파이프라인

        ## 동작 방식
        1. 저장된 네이버 로그인 상태(storage_state)를 불러와 업데이트된 로그인 쿠키를 추출한다.
        2. 네이버 로그인 쿠키를 가지고 네이버 광고주센터의 디스플레이 광고 계정을 인증한다.
        3. 쿠키에 'XSRF-TOKEN'이 추가되었다면 인증된 것으로 인식하고, 완성된 쿠키 문자열을 지정된 파일에 덮어쓴다.

        ## 인증 정보
        - 네이버 계정 목록(searchad.users)과 디스플레이 광고 계정 목록(searchad.gfa)을 'customer_id'로 매칭한다.
        - 네이버 계정 정보에는 저장된 네이버 로그인 상태(states)가 필요하다.
        - 디스플레이 광고 계정 정보에는 쿠키 문자열을 저장할 파일 경로(cookies)가 필요하다.

        ## 예외 상황
        - 계정 보호조치, 비밀번호 변경 등의 사례로 저장된 네이버 로그인 상태가 만료될 수 있다.
        - 만약 저장된 상태가 없거나 상태가 만료되면 모바일 환경에서 로그인을 수행한다.
        - 네이버 로그인 정책 강화로 빈 프로필의 Playwright 브라우저에서 로그인 시 CAPTCHA 인증이 발생한다.
        - 따라서, 저장된 네이버 로그인 상태가 만료되면 직접 수동 로그인 후 상태를 업데이트해야 한다.
    """).strip(),
) as dag:

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        """네이버 광고 계정 목록(`searchad.gfa`)과 사용자 목록(`searchad.users`)을 읽고,   
        `customer_id`로 매칭하여 로그인에 필요한 인증 정보를 구성한다."""
        from airflow.sdk import Variable
        from airflow_utils import read_credentials as read
        from linkmerce.utils.regex import regexp_extract
        states_dir = Variable.get("states")

        gfa_accounts = read("searchad.gfa", skip_subpath=True)
        naver_users = read("searchad.users", skip_subpath=True, path_strings={"$states": states_dir})
        user_map = {user["customer_id"]: user for user in naver_users}

        credentials = list()
        for account in gfa_accounts:
            user = user_map.get(account["customer_id"])
            if not user:
                continue
            credentials.append({
                "account_no": account["account_no"],
                "customer_id": account["customer_id"],
                "userid": user["userid"],
                "passwd": user["passwd"],
                "save_to": regexp_extract(r"Path\(([^)]+)\)", account["cookies"]),
                "storage_state": user["states"],
            })
        return credentials


    @task(task_id="login_gfa")
    def login_gfa(credentials: list, **kwargs) -> dict:
        from pw_actions import login_naver
        from linkmerce.api.searchad.center import login as center_login
        import logging

        logger = logging.getLogger(__name__)
        result = dict()

        for creds in credentials:
            account_no = creds["account_no"]
            userid = creds["userid"]
            exec_info = {"account_no": account_no, "userid": userid, "cookies": None, "status": dict()}

            # 1. 네이버 로그인 (Playwright 브라우저 활용)
            try:
                exec_info["cookies"] = login_naver(userid, creds["passwd"], creds["storage_state"])
                exec_info["status"]["naver"] = "success"
                logger.info(f"[{account_no}] Naver login succeeded for '{userid}'")
            except Exception as exception:
                logger.error(f"[{account_no}] Naver login failed for '{userid}': {exception}")
                exec_info["status"]["naver"] = f"failed: {exception}"
                result[str(account_no)] = exec_info
                continue

            # 2. 네이버 광고주센터 인증 (`XSRF-TOKEN` 발급 및 쿠키 저장)
            try:
                exec_info["cookies"] = center_login(account_no, exec_info["cookies"], creds["save_to"])
                exec_info["status"]["center"] = "success"
                logger.info(f"[{account_no}] GFA login succeeded ('XSRF-TOKEN' issued)")
            except Exception as exception:
                logger.error(f"[{account_no}] GFA login failed: {exception}")
                exec_info["status"]["center"] = f"failed: {exception}"

            result[str(account_no)] = exec_info

        return result


    login_gfa(credentials=read_credentials())
