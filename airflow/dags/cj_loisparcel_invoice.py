from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "cj_loisparcel_invoice",
    schedule = "0 2 * * *",
    start_date = pendulum.datetime(2025, 12, 3, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:high", "loisparcel:invoice", "login:cj-loisparcel", "schedule:daily", "time:night", "playwright:true"],
    doc_md = dedent("""
        # CJ 로이스파셀 운송장 ETL 파이프라인

        ## 인증(Credentials)
        CJ 로이스파셀 로그인을 위한 아이디, 비밀번호와
        2단계 인증을 위한 이메일 계정 로그인 정보가 필요하다.
        (동일한 도커 네트워크의 Playwright 컨테이너가 실행 중이어야 한다.)

        ## 추출(Extract)
        접수일자, 집화일자, 배송완료일자 기준으로 기업고객일별배송상세 데이터를 다운로드 받는다.
        실행 시점(data_interval_end)을 기준으로 접수일자는 3일전부터 1일전 기준,
        집화일자와 배송완료일자는 1일전을 기준으로 조회한다.

        ## 변환(Transform)
        3가지 날짜 기준으로 다운로드 받은 엑셀 바이너리 형식의 데이터를 파싱하여
        하나의 DuckDB 테이블에 중복 없이 적재한다.

        ## 적재(Load)
        기존 BigQuery 테이블과 MERGE 문으로 병합해 중복 없이 적재한다.
    """).strip(),
) as dag:

    PATH = "cjlogistics.loisparcel.invoice"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, credentials="expand", tables=True, service_account=True)


    @task(task_id="etl_loisparcel_invoice")
    def etl_loisparcel_invoice(ti: TaskInstance, **kwargs) -> dict:
        configs = ti.xcom_pull(task_ids="read_configs")
        if "query_dates" not in configs:
            configs["query_dates"] = generate_query_range(**kwargs)
        return main(**configs)

    def generate_query_range(data_interval_end: pendulum.DateTime = None, **kwargs) -> dict[str, str]:
        """날짜 기준에 따른 조회 기간을 정의한다."""
        from airflow_utils import format_date
        yesterday = format_date(data_interval_end, subdays=1)
        return {
            "접수일자": {"start_date": format_date(data_interval_end, subdays=3), "end_date": yesterday},
            "집화일자": {"start_date": yesterday, "end_date": yesterday},
            "배송완료일자": {"start_date": yesterday, "end_date": yesterday},
        }

    def main(
            userid: str,
            passwd: str,
            mail_info: dict[str, str],
            query_dates: dict[str, dict[str, str]],
            service_account: dict,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        """Playwright 브라우저로 기업고객일별배송상세 데이터를 다운로드 받고 BigQuery 테이블에 맞게 변환 및 적재한다."""
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.extensions.bigquery import BigQueryClient
        date_by = {"접수일자": "by_register", "집화일자": "by_pickup", "배송완료일자": "by_delivery"}
        query_dates = {date_type: query_dates[date_type] for date_type in date_by.keys() if date_type in query_dates}
        if not query_dates:
            raise ValueError("조회 기간이 존재하지 않습니다")

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            results = extract(userid, passwd, mail_info, query_dates)

            source_table = "cj_loisparcel_invoice"
            conn.execute(create(source_table))
            insert_query = insert_into(source_table)
            for date_type in list(query_dates.keys())[::-1]:
                if results[date_type]:
                    conn.execute(insert_query, params={"rows": results[date_type]})
            date_array = conn.unique(source_table, "register_date")

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "userid": userid,
                        **{"2fa_email": mail_info["email"]},
                        **{date_by[date_type]: params for date_type, params in query_dates.items()},
                    },
                    "counts": {
                        "total": conn.count_table(source_table),
                        **{date_by[date_type]: len(values) for date_type, values in results.items()},
                    },
                    "dates": {
                        "invoice": sorted(map(str, date_array)),
                    },
                    "status": {
                        "table": (client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = source_table,
                            staging_table = tables["temp_table"],
                            target_table = tables["table"],
                            **merge["table"],
                            where_clause = conn.expr_date_range("T.register_date", date_array),
                            progress = False,
                        ) if date_array else True),
                    },
                }


    def extract(
            userid: str,
            passwd: str,
            mail_info: dict[str, str],
            query_dates: dict[str, dict[str, str]],
        ) -> dict[str, list[dict]]:
        """Playwright 브라우저로 기업고객일별배송상세 데이터를 다운로드 받고 JSON 형식으로 변환해서 반환한다."""
        from playwright.sync_api import Page, sync_playwright
        from typing import Literal
        import time

        LONG = 5
        MEDIUM = 1
        SHORT = 0.5

        def login_begin(page: Page, userid: str, passwd: str):
            """CJ 로이스파셀 로그인을 시도한다."""
            login_url = "https://loisparcelp.cjlogistics.com/index.do"
            page.goto(login_url)
            page.type('input[type="text"]', userid, delay=100); time.sleep(MEDIUM)
            page.type('input[type="password"]', passwd, delay=100); time.sleep(MEDIUM)
            page.click("div.btn-login > a"); time.sleep(SHORT)

        def two_login_action(page: Page, code: str):
            """CJ 로이스파셀 로그인 요청 후 2단계 인증을 처리한다."""
            page.type("input.cl-text", code, delay=100); time.sleep(SHORT)
            page.click("div.btn-login > a"); time.sleep(SHORT)
            page.wait_for_selector("div.main-logintime", timeout=30*1000)

        def close_popup(page: Page, retry_count: int = 10):
            """로그인 후 발생하는 팝업을 전부 닫는다."""
            for _ in range(retry_count):
                time.sleep(LONG)
                if (page.locator("div.cl-dialog-close").count() > 0):
                    click_button(page, "div.cl-dialog-close", nth="last"); time.sleep(MEDIUM)
                else:
                    return

        def goto_menu(page: Page):
            """사이드 메뉴에서 다단계 카테고리를 펼치고 기업고객일별배송상세 메뉴를 연다."""
            cl_level_1 = 'a.cl-level-1[title="고객실적"]'
            cl_level_2 = 'a.cl-level-2[title="고객실적"]'
            cl_level_3 = 'a.cl-level-3[title="기업고객일별배송상세"]'
            for selector in [cl_level_1, cl_level_2, cl_level_3]:
                page.click(selector); time.sleep(SHORT)
            wait_for_menu(page, title="기업고객일별배송상세")

        def wait_for_menu(page: Page, title: str, timeout: int = 30):
            """기업고객일별배송상세 메뉴가 열릴 때까지, 메뉴의 제목이 표시될 때까지 기다린다."""
            time.sleep(LONG)
            for i in range(timeout):
                if page.locator("div.app-tit").last.inner_text() == title:
                    wait_for_loading(page, timeout=(timeout - i)); time.sleep(SHORT)
                    return
                time.sleep(MEDIUM)

        def wait_for_loading(page: Page, timeout: int = 30):
            """기업고객일별배송상세 메뉴를 열고 로딩 아이콘이 사라질 때까지 기다린다."""
            for _ in range(timeout):
                if page.locator('div[style*="loader.gif"]').count() <= 1:
                    return
                time.sleep(MEDIUM)

        def search(page: Page, delay: float = 3, timeout: int = 30):
            """기업고객일별배송상세 메뉴에서 조회 버튼을 누른다."""
            click_button(page, "div.btn-i-search > a", nth="last"); time.sleep(delay)
            wait_for_loading(page, timeout); time.sleep(MEDIUM)

        def change_date_type(page: Page, selected: str, change_to: str):
            """날짜 기준 드롭다운을 열고 지정된 항목을 클릭해 날짜 기준을 변경한다."""
            try:
                click_button(page, f'div[aria-label="{selected}"]'); time.sleep(MEDIUM)
            except:
                raise ValueError(f"'{selected}' 항목이 존재하지 않습니다")

            for item in page.locator('div[role="listbox"] > div.cl-combobox-item').all():
                if item.inner_text() == change_to:
                    item.click(); time.sleep(SHORT)
                    return
            raise ValueError(f"'{date_type}' 항목이 존재하지 않습니다")

        def set_date_range(page: Page, start_date: str, end_date: str, **kwargs):
            """조회 기간 입력창을 클릭하고 연도, 월, 일 순으로 타이핑해 시작/종료 날짜를 지정한다."""
            for i, date in zip([0, 1], [start_date, end_date]):
                for j in range(5):
                    time.sleep(j)
                    input_value = page.locator('input[aria-label="기준일자"]').nth(i).input_value()
                    if date == input_value:
                        break
                    input_date(page, date, nth=i)

        def input_date(page: Page, date: str, nth: int):
            """시작 또는 종료 날짜 영역을 클릭하고 연도, 월, 일 순으로 날짜를 입력한다."""
            click_button(page, 'input[aria-label="기준일자"]', nth=nth, width=0.25); time.sleep(SHORT)
            year, month, day = date.split('-')
            page.keyboard.type(year[0:2]); time.sleep(SHORT)
            page.keyboard.type(year[2:4]); time.sleep(SHORT)
            page.keyboard.type(month); time.sleep(SHORT)
            page.keyboard.type(day); time.sleep(SHORT)
            page.keyboard.press("Enter"); time.sleep(SHORT)

        def count_total(page: Page) -> int:
            """조회된 항목 수를 카운팅한다. 조회 결과가 없다면 다운로드하지 않는다."""
            label = page.locator('div[aria-label^="총"][aria-label$="건"]').first.get_attribute("aria-label")
            return int(label[1:-1].replace(',', '').strip())

        def download(page: Page, timeout: int = 30) -> list[dict]:
            """조회 결과가 있다면 엑셀 파일을 임시 경로에 저장하고,
            저장된 파일을 바이너리 형식으로 불러와 JSON 형식으로 변환한다."""
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
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp_file:
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
            """다운로드 버튼의 중앙 좌표를 계산해서 실제 마우스 클릭을 수행한다."""
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
                        set_date_range(page, **date_range)
                        selected = date_type
                        search(page)
                        results[date_type] = download(page) if count_total(page) else list()
                        time.sleep(MEDIUM)
                    return results
                finally:
                    page.close()
            finally:
                browser.close()


    def create(table: str) -> str:
        """기업고객일별배송상세 데이터에서 필요한 항목만 선택한 테이블을 생성하는 쿼리를 반환한다."""
        return dedent(f"""
            CREATE TABLE IF NOT EXISTS {table} (
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
            )
            """).strip()

    def insert_into(table: str) -> str:
        """기업고객일별배송상세 데이터를 DuckDB 테이블에 적재하는 쿼리를 반환한다."""
        return dedent(f"""
            INSERT INTO {table}
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
            FROM (SELECT rows.* FROM (SELECT UNNEST($rows) AS rows))
            WHERE ("운송장번호" IS NOT NULL) AND (CAST("접수일자" AS DATE) IS NOT NULL)
            ON CONFLICT DO NOTHING
            """).strip()


    def get_2fa_code(
            origin: str,
            email: str,
            passwd: str,
            wait_seconds: int = (60*3-10),
            wait_interval: int = 1,
            **kwargs
        ) -> str:
        """CJ 로이스파셀 로그인을 위해 2단계 인증을 수행한다."""
        from linkmerce.utils.headers import build_headers
        import requests
        import time

        def login_action(session: requests.Session, origin: str, email: str, passwd: str):
            """메일 서버에 로그인한다."""
            url = f"https://auth-api.{origin}/office-web/login"
            body = {"id": email, "password": passwd, "ip_security_level": "1"}
            headers = build_headers(contents="json", host=f"auth-api.{origin}", origin=f"https://login.{origin}", referer=f"https://login.{origin}/")
            session.post(url, json=body, headers=headers)

        def wait_2fa_mail(session: requests.Session, origin: str) -> int:
            """2단계 인증 메일이 발송될 때까지 기다린다. 이미 읽은 메일은 무시한다."""
            url = f"https://mail-api.{origin}/v2/mails"
            params = {"page[limit]": 30, "page[offset]": 0, "sort[received_date]": "desc", "filter[mailbox_id][eq]": "b0"}
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
            """새로 발송된 2단계 인증 메일에서 인증코드를 추출한다."""
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
            """방금 읽은 2단계 인증 메일을 읽음 처리한다."""
            url = f"https://mail-api.{origin}/v2/mails/{mail_no}"
            headers = build_headers(accept="application/json;charset=UTF-8", contents="application/json;charset=UTF-8",
                host=f"mail-api.{origin}", origin=f"https://mails.{origin}", referer=f"https://mails.{origin}/")
            session.patch(url, json={"is_read": True}, headers=headers)

        with requests.Session() as session:
            login_action(session, origin, email, passwd)
            mail_no = wait_2fa_mail(session, origin)
            return retrieve_2fa_code(session, origin, mail_no)


    read_configs() >> etl_loisparcel_invoice()
