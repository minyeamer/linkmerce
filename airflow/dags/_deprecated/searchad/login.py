from airflow.sdk import DAG, task
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "searchad_login",
    schedule = "10 1 * * *",
    start_date = pendulum.datetime(2025, 9, 8, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(hours=1),
    catchup = False,
    tags = ["priority:high", "searchad:cookies", "login:naver", "schedule:daily", "time:night", "playwright:true"],
    doc_md = dedent("""
        # 네이버 로그인 파이프라인

        > 안내) 네이버 로그인 정책 강화로 사용 중지

        네이버 계정 목록을 순회하면서 네이버 로그인 페이지에 접속해 로그인을 진행한다.
        로그인 후 광고 계정이 참조하는 쿠키 경로에 쿠키 문자열을 덮어쓴다.
        (동일한 도커 네트워크의 Playwright 컨테이너가 실행 중이어야 한다.)
    """).strip(),
) as dag:

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        """검색광고(`searchad`) 인증 정보에서 네이버 계정(`users`)과 광고 계정(`manage`)을 조회한다.   
        둘을 결합하여, 네이버 계정 로그인 후 광고 계정이 참조하는 쿠키 경로에 로그인 쿠키를 덮어쓰는 설정을 만든다."""
        from linkmerce.api.config import read_config
        from linkmerce.utils.regex import regexp_extract
        from airflow.sdk import Variable
        from typing import Sequence
        import random

        credentials = read_config(Variable.get("credentials"))["searchad"]
        users = users if isinstance((users := credentials["users"]), Sequence) else [users]
        random.shuffle(users)

        cookies_arr = cookies if isinstance((cookies := credentials["manage"]), Sequence) else [cookies]
        cookies_map = {cookie["customer_id"]: cookie["cookies"] for cookie in cookies_arr}
        cookies_path = {"$cookies": Variable.get("cookies")}

        def extract_path(cookies: str) -> str:
            return regexp_extract(r"Path\(([^)]+)\)", cookies.format(cookies_path))

        return [{
            "userid": user["userid"],
            "passwd": user["passwd"],
            "customer_id": user["customer_id"],
            "save_to": extract_path(cookies_map[user["customer_id"]]),
        } for user in users if user["customer_id"] in cookies_map]


    @task(task_id="login_naver", map_index_template="{{ credentials['customer_id'] }}", retries=3, retry_delay=timedelta(minutes=1), pool="login_pool")
    def login_naver(credentials: dict, **kwargs) -> str:
        """Playwright 브라우저로 네이버 로그인하고 쿠키를 지정된 경로에 덮어쓴다."""
        from playwright.sync_api import Page, sync_playwright
        from linkmerce.api.searchad.manage import has_cookies
        from linkmerce.common.exceptions import AuthenticationError
        import random
        import time

        def login(userid: str, passwd: str, save_to: str, **kwargs) -> str:
            """Playwright 브라우저를 실행하고 네이버 로그인 동작과 쿠키 저장을 제어한다."""
            with sync_playwright() as playwright:
                ws_endpoint = "ws://playwright:3000/" # 실행 중인 Playwright 컨테이너에 연결
                browser = playwright.chromium.connect(ws_endpoint)
                try:
                    page = browser.new_page()
                    try:
                        login_action(page, userid, passwd)
                        return save_cookies(page, save_to)
                    finally:
                        page.close()
                finally:
                    browser.close()

        def login_action(page: Page, userid: str, passwd: str, wait_seconds: int = 10, wait_interval: int = 1):
            """네이버 로그인 페이지로 이동해 입력창에 아이디, 비밀번호를 작성하고 로그인 버튼을 누른다."""
            login_url = "https://nid.naver.com/nidlogin.login?realname=Y&type=modal&svctype=262144&returl=https%3A%2F%2Fmy.naver.com"
            page.goto(login_url)

            page.type("input#id", userid, delay=100)
            time.sleep(random.uniform(0.3, 0.6))

            page.type("input#pw", passwd, delay=100)
            time.sleep(random.uniform(0.3, 0.6))

            page.click("button#submit_btn")
            wait_cookies(page, wait_seconds, wait_interval)

        def wait_cookies(page: Page, wait_seconds: int = 10, wait_interval: int = 1):
            """네이버 로그인 후 `NID_AUT` 쿠키가 추가될 때까지 기다린다."""
            for _ in range(wait_seconds // wait_interval):
                time.sleep(wait_interval)
                cookies = page.context.cookies()
                for cookie in cookies:
                    if cookie["name"] == "NID_AUT":
                        return

        def save_cookies(page: Page, save_to: str) -> str:
            """광고 계정이 참조하는 쿠키 경로에 현재 네이버 로그인된 계정의 쿠키를 덮어쓴다."""
            cookies = '; '.join([f"{cookie['name']}={cookie['value']}" for cookie in page.context.cookies()])
            if has_cookies(cookies):
                with open(save_to, 'w', encoding="utf-8") as file:
                    file.write(cookies)
                return cookies
            else:
                raise AuthenticationError("Login failed.")

        def preview(cookies: str, size: int = 100) -> str:
            """쿠키를 최대 100자까지 로그에 출력한다."""
            return (cookies[:size] + "...") if len(cookies) > size else cookies

        return {"cookies": preview(login(**credentials))}


    login_naver.expand(credentials=read_credentials())
