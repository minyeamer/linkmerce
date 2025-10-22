from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
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

    LOGIN_RETRIES = 3

    def preview(cookies: str, size: int = 100) -> str:
        return (cookies[:size] + "...") if len(cookies) > size else cookies


    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> dict:
        from airflow.sdk import Variable
        from linkmerce.api.config import read_config
        return read_config(Variable.get("credentials"))

    def extract_path(cookies: str) -> str:
        from linkmerce.utils.regex import regexp_extract
        return regexp_extract(r"Path\(([^)]+)\)", cookies)


    @task(task_id="login_partner_center", retries=LOGIN_RETRIES)
    def login_partner_center(ti: TaskInstance, **kwargs) -> str:
        from linkmerce.api.smartstore.brand import login
        from typing import Sequence

        credentials = ti.xcom_pull(task_ids="read_credentials")
        user = users[0] if isinstance((users := credentials["smartstore"]["users"]), Sequence) else users
        save_to = extract_path(credentials["smartstore"]["brand"]["cookies"])
        return dict(cookies = preview(login(**user, save_to=save_to)))


    @task(task_id="login_searchad")
    def login_searchad(ti: TaskInstance, **kwargs) -> str:
        from playwright.sync_api import Playwright, Page, sync_playwright
        from linkmerce.api.searchad.manage import has_cookies
        from linkmerce.common.exceptions import AuthenticationError
        from typing import Sequence
        import logging
        import random
        import time

        def login_naver(playwright: Playwright, userid: str, passwd: str, save_to: str) -> str:
            ws_endpoint = "ws://playwright:3000/"
            browser = playwright.chromium.connect(ws_endpoint)
            mobile_url = "https://nid.naver.com/nidlogin.login?realname=Y&type=modal&svctype=262144&returl=https%3A%2F%2Fmy.naver.com"

            try:
                page = browser.new_page()
                page.goto(mobile_url)
                login_action(page, userid, passwd)
                return save_cookies(page, save_to)
            finally:
                browser.close()

        def login_action(page: Page, userid: str, passwd: str, wait_seconds: int = 10, wait_interval: int = 1):
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

        credentials = ti.xcom_pull(task_ids="read_credentials")
        cookies_arr = cookies if isinstance((cookies := credentials["searchad"]["manage"]), Sequence) else [cookies]
        cookies_map = {cookie["customer_id"]: cookie["cookies"] for cookie in cookies_arr}

        users = users if isinstance((users := credentials["searchad"]["users"]), Sequence) else [users]
        results = {user["customer_id"]: None for user in users if user["customer_id"] in cookies_map}
        random.shuffle(users)

        exceptions = list()

        for user in users:
            customer_id = user["customer_id"]
            if customer_id in cookies_map:
                for i in range(LOGIN_RETRIES):
                    with sync_playwright() as playwright:
                        try:
                            cookies = login_naver(
                                playwright = playwright,
                                userid = user["userid"],
                                passwd = user["passwd"],
                                save_to = extract_path(cookies_map[customer_id]),
                            )
                            logging.info(f"[{customer_id}] SUCCESS ({i})")
                            results[customer_id] = preview(cookies)
                            break
                        except Exception as exception:
                            logging.error(f"[{customer_id}] FAILED ({i}) - [{exception.__class__.__name__}] {exception}")
                            results[customer_id] = preview(str(exception))
                            if i == (LOGIN_RETRIES-1):
                                exceptions.append(exception)
                            else:
                                time.sleep(1)
                time.sleep(1)

        if exceptions:
            raise exceptions[0]
        else:
            return results


    read_credentials() >> [login_partner_center(), login_searchad()]
