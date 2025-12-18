from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "cj_loisparcel_invoice",
    schedule = "0 2 * * *",
    start_date = pendulum.datetime(2025, 12, 3, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:high", "loisparcel:invoice", "login:cj-loisparcel", "schedule:daily", "time:night", "playwright:true"],
) as dag:

    PATH = ["cjlogistics", "loisparcel", "cj_loisparcel_invoice"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, credentials="expand", tables=True, service_account=True)


    @task(task_id="etl_loisparcel_invoice")
    def etl_loisparcel_invoice(ti: TaskInstance, **kwargs) -> dict:
        variables = ti.xcom_pull(task_ids="read_variables")
        if "query_dates" not in variables:
            variables["query_dates"] = generate_query_range(**kwargs)
        return main(**variables)

    def generate_query_range(data_interval_end: pendulum.DateTime = None, **kwargs) -> dict[str,str]:
        from variables import strftime
        yesterday = strftime(data_interval_end, subdays=1)
        return {
            "접수일자": dict(start_date=strftime(data_interval_end, subdays=3), end_date=yesterday),
            "집화일자": dict(start_date=yesterday, end_date=yesterday),
            "배송완료일자": dict(start_date=yesterday, end_date=yesterday),
        }

    def main(
            userid: str,
            passwd: str,
            mail_info: dict[str,str],
            query_dates: dict[str,dict[str,str]],
            service_account: dict,
            tables: dict[str,str],
            merge: dict[str,dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.extensions.bigquery import BigQueryClient
        date_by = {"접수일자": "by_register", "집화일자": "by_pickup", "배송완료일자": "by_delivery"}
        query_dates = {date_type: query_dates[date_type] for date_type in date_by.keys() if date_type in query_dates}
        if not query_dates:
            raise ValueError("조회 기간이 존재하지 않습니다")

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            results = extract(userid, passwd, mail_info, query_dates)

            source_table = "data"
            conn.execute(create_table(source_table))
            insert_query = insert_into_table(source_table, values=select_table())
            for date_type in list(query_dates.keys())[::-1]:
                if results[date_type]:
                    conn.execute(insert_query, obj=results[date_type])
            date_array = conn.unique(source_table, "register_date")

            with BigQueryClient(service_account) as client:
                return dict(
                    params = dict(
                        userid = userid,
                        **{"2fa_email": mail_info["email"]},
                        **{date_by[date_type]: params for date_type, params in query_dates.items()},
                    ),
                    counts = dict(
                        total = conn.count_table(source_table),
                        **{date_by[date_type]: len(values) for date_type, values in results.items()},
                    ),
                    dates = dict(
                        invoice = list(map(str, date_array)),
                    ),
                    status = dict(
                        invoice = (client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = source_table,
                            staging_table = tables["temp_invoice"],
                            target_table = tables["invoice"],
                            **merge["invoice"],
                            where_clause = conn.expr_date_range("T.register_date", date_array),
                            progress = False,
                        ) if date_array else True),
                    ),
                )


    def extract(
            userid: str,
            passwd: str,
            mail_info: dict[str,str],
            query_dates: dict[str,dict[str,str]],
        ) -> dict[str,list[dict]]:
        """조회가능기간 = 16days"""
        from playwright.sync_api import Page, sync_playwright
        from typing import Literal
        import time

        LONG = 5
        MEDIUM = 1
        SHORT = 0.5

        def login_begin(page: Page, userid: str, passwd: str):
            login_url = "https://loisparcelp.cjlogistics.com/index.do"
            page.goto(login_url)
            page.type('input[type="text"]', userid, delay=100); time.sleep(MEDIUM)
            page.type('input[type="password"]', passwd, delay=100); time.sleep(MEDIUM)
            page.click("div.btn-login > a"); time.sleep(SHORT)

        def two_login_action(page: Page, code: str):
            page.type("input.cl-text", code, delay=100); time.sleep(SHORT)
            page.click("div.btn-login > a"); time.sleep(SHORT)
            page.wait_for_selector("div.main-logintime", timeout=30*1000)

        def close_popup(page: Page, retry_count: int = 10):
            for _ in range(retry_count):
                time.sleep(LONG)
                if (page.locator("div.cl-dialog-close").count() > 0):
                    click_button(page, "div.cl-dialog-close", nth="last"); time.sleep(MEDIUM)
                else:
                    return

        def goto_menu(page: Page):
            cl_level_1 = 'a.cl-level-1[title="고객실적"]'
            cl_level_2 = 'a.cl-level-2[title="고객실적"]'
            cl_level_3 = 'a.cl-level-3[title="기업고객일별배송상세"]'
            for selector in [cl_level_1, cl_level_2, cl_level_3]:
                page.click(selector); time.sleep(SHORT)
            wait_for_menu(page, title="기업고객일별배송상세")

        def wait_for_menu(page: Page, title: str, timeout: int = 30):
            time.sleep(LONG)
            for i in range(timeout):
                if page.locator("div.app-tit").last.inner_text() == title:
                    wait_for_loading(page, timeout=(timeout - i)); time.sleep(SHORT)
                    return
                time.sleep(MEDIUM)

        def wait_for_loading(page: Page, timeout: int = 30):
            for _ in range(timeout):
                if page.locator('div[style*="loader.gif"]').count() <= 1:
                    return
                time.sleep(MEDIUM)

        def search(page: Page, delay: float = 3, timeout: int = 30):
            click_button(page, "div.btn-i-search > a", nth="last"); time.sleep(delay)
            wait_for_loading(page, timeout); time.sleep(MEDIUM)

        def change_date_type(page: Page, selected: str, change_to: str):
            try:
                click_button(page, f'div[aria-label="{selected}"]'); time.sleep(MEDIUM)
            except:
                raise ValueError(f"'{selected}' 항목이 존재하지 않습니다")

            for item in page.locator('div[role="listbox"] > div.cl-combobox-item').all():
                if item.inner_text() == change_to:
                    item.click(); time.sleep(SHORT)
                    return
            raise ValueError(f"'{date_type}' 항목이 존재하지 않습니다")

        def select_date(page: Page, start_date: str, end_date: str, **kwargs):
            for i, date in zip([0, 1], [start_date, end_date]):
                click_button(page, 'input[aria-label="기준일자"]', nth=i, width=0.25); time.sleep(SHORT)
                year, month, day = date.split('-')
                page.keyboard.type(year[0:2]); time.sleep(SHORT)
                page.keyboard.type(year[2:4]); time.sleep(SHORT)
                page.keyboard.type(month); time.sleep(SHORT)
                page.keyboard.type(day); time.sleep(SHORT)
                page.keyboard.press("Enter"); time.sleep(SHORT)

        def count_total(page: Page) -> int:
            label = page.locator('div[aria-label^="총"][aria-label$="건"]').first.get_attribute("aria-label")
            return int(label[1:-1].strip())

        def download(page: Page, timeout: int = 30) -> list[dict]:
            from playwright.sync_api import TimeoutError
            from linkmerce.utils.excel import excel2json
            import os
            import tempfile

            try:
                with page.expect_download(timeout=timeout*1000) as download_info:
                    click_button(page, "div.btn-i-excel > a", nth="last")
            except TimeoutError:
                page.locator("div.cl-text", has_text="엑셀로 전환할 자료가 없습니다.").click()
                return list()

            download = download_info.value
            with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
                tmp_path = tmp_file.name
                download.save_as(tmp_path)

            try:
                with open(tmp_path, "rb") as file:
                    return excel2json(file.read(), warnings=False)[:-1] # Remove total(-1)
            finally:
                os.unlink(tmp_path)

        def click_button(
                page: Page,
                selector: str,
                nth: int | Literal["last"] = 0,
                width: float = 0.5,
                height: float = 0.5,
                timeout: float = 3000
            ):
            locator = page.locator(selector).last if nth == "last" else page.locator(selector).nth(nth)
            box = locator.bounding_box(timeout=timeout)
            page.mouse.move(box["x"] + box["width"] * width, box["y"] + box["height"] * height)
            page.mouse.down()
            return page.mouse.up()

        with sync_playwright() as playwright:
            ws_endpoint = "ws://playwright:3000/"
            browser = playwright.chromium.connect(ws_endpoint)
            try:
                page = browser.new_page()
                try:
                    login_begin(page, userid, passwd)
                    two_login_action(page, code=get_2fa_code(**mail_info))
                    close_popup(page)
                    goto_menu(page)
                    search(page)

                    results, selected = dict(), "집화일자"
                    for date_type, date_range in query_dates.items():
                        change_date_type(page, selected, change_to=date_type)
                        select_date(page, **date_range)
                        selected = date_type
                        search(page)
                        results[date_type] = download(page) if count_total(page) else list()
                        time.sleep(MEDIUM)
                    return results
                finally:
                    page.close()
            finally:
                browser.close()


    def create_table(table: str) -> str:
        return """
        CREATE TABLE IF NOT EXISTS {{ table }} (
            invoice_no VARCHAR PRIMARY KEY
            , order_id VARCHAR
            , product_id VARCHAR
            , product_name VARCHAR
            , order_quantity INTEGER
            , delivery_fee INTEGER
            , shipper_name VARCHAR
            , receiver_name VARCHAR
            , zipcode VARCHAR
            , address VARCHAR
            , phone1 VARCHAR
            , phone2 VARCHAR
            , delivery_message VARCHAR
            , box_type VARCHAR
            , delivery_type VARCHAR
            , order_status VARCHAR
            , delivery_status VARCHAR
            , register_date DATE NOT NULL
            , pickup_date DATE
            , delivery_date DATE
        );
        """.strip().replace("{{ table }}", table)

    def select_table(array: str = "obj") -> str:
        return """
        SELECT
            "운송장번호" AS invoice_no
            , "주문번호" AS order_id
            , "단품코드" AS product_id
            , "품명" AS product_name
            , TRY_CAST("수량" AS INTEGER) AS order_quantity
            , TRY_CAST("운임" AS INTEGER) AS delivery_fee
            , "송화인" AS shipper_name
            , "받는분" AS receiver_name
            , "우편번호" AS zipcode
            , "주소" AS address
            , "전화번호" AS phone1
            , "휴대번호" AS phone2
            , "특이사항(배송메시지)" AS delivery_message
            , "박스타입" AS box_type
            , "운임구분" AS delivery_type
            , "예약구분" AS order_status
            , "접수구분" AS delivery_status
            , CAST("접수일자" AS DATE) AS register_date
            , CAST("집화일자" AS DATE) AS pickup_date
            , CAST("배송일자" AS DATE) AS delivery_date
        FROM {{ array }}
        WHERE ("운송장번호" IS NOT NULL) AND (CAST("접수일자" AS DATE) IS NOT NULL);
        """.strip().replace("{{ array }}", f"(SELECT {array}.* FROM (SELECT UNNEST(${array}) AS {array}))")[:-1]

    def insert_into_table(table: str, values: str) -> str:
        return ("INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;"
                .replace("{{ table }}", table)
                .replace("{{ values }}", values))


    def get_2fa_code(
            origin: str,
            email: str,
            passwd: str,
            wait_seconds: int = (60*3-10),
            wait_interval: int = 1,
            **kwargs
        ) -> str:
        from linkmerce.utils.headers import build_headers
        import requests
        import time

        def login_action(session: requests.Session, origin: str, email: str, passwd: str):
            url = f"https://auth-api.{origin}/office-web/login"
            body = {"id": email,"password": passwd, "ip_security_level": "1"}
            headers = build_headers(contents="json", host=f"auth-api.{origin}", origin=f"https://login.{origin}", referer=f"https://login.{origin}/")
            session.post(url, json=body, headers=headers)

        def wait_2fa_mail(session: requests.Session, origin: str) -> int:
            url = f"https://mail-api.{origin}/v2/mails"
            params = {"page[limit]": 30, "page[offset]": 0, "sort[received_date]": "desc", "filter[mailbox_id][eq]": "b0",}
            headers = build_headers(host=f"mail-api.{origin}", origin=f"https://mails.{origin}", referer=f"https://mails.{origin}/")
            headers["x-skip-session-refresh"] = "true"
            for _ in range(wait_seconds):
                with session.get(url, params=params, headers=headers) as response:
                    for mail in response.json()["data"][:5]:
                        if (mail["subject"] == "CJ대한통운 LoIS Parcel 로그인 2차 인증") and mail["is_new"]:
                            return mail["no"]
                time.sleep(wait_interval)
            raise ValueError("인증코드가 전달되지 않았습니다")

        def retrieve_2fa_code(session: requests.Session, origin: str, mail_no: int) -> str:
            import re
            url = f"https://mail-api.{origin}/v2/mails/{mail_no}"
            headers = build_headers(host=f"mail-api.{origin}", origin=f"https://mails.{origin}", referer=f"https://mails.{origin}/")
            with session.get(url, headers=headers) as response:
                try:
                    content = response.json()["data"]["message"]["content"]
                    return re.search(r"\<font size=\'7\'\>(\d{6})\</font\>", content).group(1)
                finally:
                    make_mail_as_read(session, origin, mail_no)

        def make_mail_as_read(session: requests.Session, origin: str, mail_no: int):
            url = f"https://mail-api.{origin}/v2/mails/{mail_no}"
            headers = build_headers(accept="application/json;charset=UTF-8", contents="application/json;charset=UTF-8",
                host=f"mail-api.{origin}", origin=f"https://mails.{origin}", referer=f"https://mails.{origin}/")
            session.patch(url, json={"is_read": True}, headers=headers)

        with requests.Session() as session:
            login_action(session, origin, email, passwd)
            mail_no = wait_2fa_mail(session, origin)
            return retrieve_2fa_code(session, origin, mail_no)


    read_variables() >> etl_loisparcel_invoice()
