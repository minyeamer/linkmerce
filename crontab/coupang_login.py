from typing import Literal
import argparse
import os


LOGIN_RETRIES = 3


def read_yaml(file_path: str) -> dict | list:
    from ruamel.yaml import YAML
    with open(file_path, 'r', encoding="utf-8") as file:
        yaml = YAML(typ="safe")
        return yaml.load(file.read())


def extract_path(cookies: str) -> str:
    import re
    return re.search(r"Path\(([^)]+)\)", cookies).groups()[0]


def preview(cookies: str, size: int = 100) -> str:
    return (cookies[:size] + "...") if len(cookies) > size else cookies


def login_coupang(
        credentials: dict,
        ws_endpoint: str,
        domain: Literal["advertising","wing"],
        log_file: str | None = None,
    ) -> dict[str,dict]:
    from playwright.sync_api import Playwright, Page, sync_playwright
    from typing import Sequence
    import logging
    import random
    import time

    logging.basicConfig(
        filename = log_file,
        level = logging.INFO,
        format = "[%(asctime)s] %(levelname)s - %(message)s",
        datefmt = "%Y-%m-%d, %H:%M:%S",
    )

    LOGIN_URL = {
        "advertising": "https://advertising.coupang.com/user/login",
        "wing": "https://wing.coupang.com/login?ui_locales=ko-KR&service_cmdb_role=wing",
    }

    def login(playwright: Playwright, userid: str, passwd: str, domain: Literal["advertising","wing"], save_to: str) -> str:
        # ws_endpoint = "ws://playwright:3000/"
        browser = playwright.chromium.connect(ws_endpoint)

        try:
            page = browser.new_page()
            page.goto(LOGIN_URL[domain])
            if domain == "advertising":
                page.click('a[href="/user/wing/authorization"]')
            login_action(page, userid, passwd)
            return save_cookies(page, save_to)
        finally:
            browser.close()

    def login_action(page: Page, userid: str, passwd: str, wait_seconds: int = 10, wait_interval: int = 1):
        page.type("input#username", userid, delay=100)
        time.sleep(random.uniform(0.3, 0.6))

        page.type("input#password", passwd, delay=100)
        time.sleep(random.uniform(0.3, 0.6))

        page.click('input[type="submit"]')
        time.sleep(3)
        wait_cookies(page, wait_seconds, wait_interval)

    def wait_cookies(page: Page, wait_seconds: int = 10, wait_interval: int = 1):
        page.wait_for_load_state("load")
        for _ in range(wait_seconds // wait_interval):
            time.sleep(wait_interval)
            cookies = page.context.cookies()
            if len([cookie for cookie in cookies if cookie["name"] == "XSRF-TOKEN"]) > 0:
                return

    def save_cookies(page: Page, save_to: str) -> str:
        cookies = '; '.join([f"{cookie['name']}={cookie['value']}" for cookie in page.context.cookies()])
        with open(save_to, 'w', encoding="utf-8") as file:
            file.write(cookies)
        return cookies

    cookies_arr = cookies if isinstance((cookies := credentials["coupang"][domain]), Sequence) else [cookies]
    cookies_map = {cookie["vendor_id"]: cookie["cookies"] for cookie in cookies_arr}

    users = users if isinstance((users := credentials["coupang"]["users"]), Sequence) else [users]
    exceptions = list()
    results = {user["vendor_id"]: None for user in users if user["vendor_id"] in cookies_map}
    random.shuffle(users)

    for user in users:
        vendor_id = user["vendor_id"]
        if vendor_id in cookies_map:
            for i in range(LOGIN_RETRIES):
                with sync_playwright() as playwright:
                    try:
                        result = login(
                            playwright = playwright,
                            userid = user["userid"],
                            passwd = user["passwd"],
                            domain = domain,
                            save_to = extract_path(cookies_map[vendor_id]),
                        )
                        logging.info(f"[{vendor_id}] SUCCESS ({i})")
                        results[vendor_id] = preview(result)
                        break
                    except Exception as exception:
                        logging.error(f"[{vendor_id}] FAILED ({i}) - [{exception.__class__.__name__}] {exception}")
                        results[vendor_id] = preview(str(exception))
                        if i == (LOGIN_RETRIES-1):
                            exceptions.append(exception)
                        else:
                            time.sleep(1)
            time.sleep(1)

    if exceptions:
        raise exceptions[0]
    else:
        logging.info(str(results).replace('\'', '\"'))
        return results


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--credentials", "-c", type=str, required=True, help="Credentials file path")
    parser.add_argument("--playwright", "-p", type=str, required=True, help="Browser websocket endpoint")
    parser.add_argument("--domain", "-d", type=str, required=True, help="Coupang domain (advertising, wing)")
    parser.add_argument("--chdir", "-r", type=str, default=None, help="Execution path (Optional)")
    parser.add_argument("--logfile", "-l", type=str, default=None, help="Log file path (Optional)")
    return parser.parse_args()

if __name__ == "__main__":
    try:
        args = parse_arguments()
        credentials_path = args.credentials
        ws_endpoint = args.playwright
        domain = args.domain
        chdir = args.chdir
        log_file = args.logfile
    except:
        raise ValueError("The following arguments are required: credentials, playwright.")

    if chdir:
        if os.path.exists(chdir):
            os.chdir(chdir)
        else:
            raise ValueError("The execution path does not exist.")

    try:
        credentials = read_yaml(credentials_path)
    except:
        raise ValueError("Unable to read credentials.")

    login_coupang(credentials, ws_endpoint, domain, log_file)
