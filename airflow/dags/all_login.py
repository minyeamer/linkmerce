from airflow.sdk import DAG, TaskGroup, task
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "all_login",
    schedule = "0 1 * * *",
    start_date = pendulum.datetime(2025, 9, 8, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(hours=1),
    catchup = False,
    tags = ["priority:high", "all:cookies", "login:partnercenter", "login:searchad", "schedule:daily", "time:night"],
) as dag:

    RETRY_OPTIONS = dict(retries=3, retry_delay=timedelta(minutes=1))

    def preview(cookies: str, size: int = 100) -> str:
        return (cookies[:size] + "...") if len(cookies) > size else cookies

    def extract_path(cookies: str) -> str:
        from linkmerce.utils.regex import regexp_extract
        return regexp_extract(r"Path\(([^)]+)\)", cookies)


    with TaskGroup(group_id="smartstore_group") as smartstore_group:

        @task(task_id="read_smartstore_credentials", **RETRY_OPTIONS)
        def read_smartstore_credentials() -> list:
            from linkmerce.api.config import read_config
            from airflow.sdk import Variable
            from typing import Sequence

            credentials = read_config(Variable.get("credentials"))["smartstore"]
            user = users[0] if isinstance((users := credentials["users"]), Sequence) else users
            return [dict(
                userid = user["userid"],
                passwd = user["passwd"],
                channel_seq = user["channel_seq"],
                save_to = extract_path(credentials["brand"]["cookies"]),
            )]


        @task(task_id="login_partner_center", map_index_template="{{ credentials['channel_seq'] }}", **RETRY_OPTIONS, pool="login_pool")
        def login_partner_center(credentials: dict, **kwargs) -> str:
            from linkmerce.api.smartstore.brand import login
            return dict(cookies = preview(login(**credentials)))


        login_partner_center.expand(credentials=read_smartstore_credentials())


    with TaskGroup(group_id="searchad_group") as searchad_group:

        @task(task_id="read_smartstore_credentials", retries=3, retry_delay=timedelta(minutes=1))
        def read_searchad_credentials() -> list:
            from linkmerce.api.config import read_config
            from airflow.sdk import Variable
            from typing import Sequence
            import random

            credentials = read_config(Variable.get("credentials"))["searchad"]
            users = users if isinstance((users := credentials["users"]), Sequence) else [users]
            random.shuffle(users)

            cookies_arr = cookies if isinstance((cookies := credentials["manage"]), Sequence) else [cookies]
            cookies_map = {cookie["customer_id"]: cookie["cookies"] for cookie in cookies_arr}

            return [dict(
                userid = user["userid"],
                passwd = user["passwd"],
                customer_id = user["customer_id"],
                save_to = extract_path(cookies_map[user["customer_id"]]),
            ) for user in users if user["customer_id"] in cookies_map]


        @task(task_id="login_naver", map_index_template="{{ credentials['customer_id'] }}", **RETRY_OPTIONS, pool="login_pool")
        def login_naver(credentials: dict, **kwargs) -> str:
            from playwright.sync_api import Page, sync_playwright
            from linkmerce.api.searchad.manage import has_cookies
            from linkmerce.common.exceptions import AuthenticationError
            import random
            import time

            def login(userid: str, passwd: str, save_to: str, **kwargs) -> str:
                with sync_playwright() as playwright:
                    ws_endpoint = "ws://playwright:3000/"
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
                login_url = "https://nid.naver.com/nidlogin.login?realname=Y&type=modal&svctype=262144&returl=https%3A%2F%2Fmy.naver.com"
                page.goto(login_url)

                page.type("input#id", userid, delay=100)
                time.sleep(random.uniform(0.3, 0.6))

                page.type("input#pw", passwd, delay=100)
                time.sleep(random.uniform(0.3, 0.6))

                page.click("button#submit_btn")
                wait_cookies(page, wait_seconds, wait_interval)

            def wait_cookies(page: Page, wait_seconds: int = 10, wait_interval: int = 1):
                for _ in range(wait_seconds // wait_interval):
                    time.sleep(wait_interval)
                    cookies = page.context.cookies()
                    for cookie in cookies:
                        if cookie["name"] == "NID_AUT":
                            return

            def save_cookies(page: Page, save_to: str) -> str:
                cookies = '; '.join([f"{cookie['name']}={cookie['value']}" for cookie in page.context.cookies()])
                if has_cookies(cookies):
                    with open(save_to, 'w', encoding="utf-8") as file:
                        file.write(cookies)
                    return cookies
                else:
                    raise AuthenticationError("Login failed.")

            return dict(cookies = preview(login(**credentials)))


        login_naver.expand(credentials=read_searchad_credentials())


    smartstore_group >> searchad_group
