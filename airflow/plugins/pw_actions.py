from __future__ import annotations

from typing import TYPE_CHECKING
import os

if TYPE_CHECKING:
    from typing import Iterable, Sequence
    from playwright.sync_api import BrowserContext

WS_ENDPOINT = f"ws://playwright:{os.getenv('PLAYWRIGHT_PORT', '3000')}/"


def get_browser_cookies(
        context: BrowserContext,
        urls: str | Sequence[str] | None = None,
        requires: Iterable[str] | None = None,
        err_msg: str = str(),
        timeout: int = 30,
    ) -> str:
    """Playwright 브라우저의 세션 쿠키를 추출해 문자열 형태로 반환한다."""
    import time
    for _ in range(timeout):
        cookies = context.cookies(urls)
        if requires:
            for kv in cookies:
                if kv["name"] in requires:
                    return "; ".join(f"{kv['name']}={kv['value']}" for kv in cookies)
        elif cookies:
            return "; ".join(f"{kv['name']}={kv['value']}" for kv in cookies)
        time.sleep(1)

    from linkmerce.common.exceptions import AuthenticationError
    raise AuthenticationError(err_msg)


def coupang_login(userid: str, passwd: str, navigate_to_ads: bool = True) -> dict:
    """Playwright 브라우저로 쿠팡 윙/광고 로그인을 수행하고 `{wing: "...", ads: "..."}` 형태의 쿠키를 반환한다.

    주의) Playwright 브라우저가 `headless=False` 옵션 또는 가상 렌더링을 지원하지 않으면   
    `Access Denied` 페이지가 발생하며 로그인이 실패한다."""
    from playwright.sync_api import sync_playwright
    import time
    cookies = dict()

    with sync_playwright() as playwright:
        browser = playwright.chromium.connect(WS_ENDPOINT)
        try:
            context = browser.new_context()
            page = context.new_page()

            # [WING LOGIN] 쿠팡 윙 로그인
            page.goto("https://wing.coupang.com/", wait_until="domcontentloaded")

            page.wait_for_selector("input#username", timeout=60_000)
            page.locator("input#username").type(userid, delay=110)
            page.locator("input#password").type(passwd, delay=110)
            page.locator("input[type='submit'], button[type='submit']").first.click()

            # [WING MAIN] 메인 페이지 이동 후 쿠키 추출
            page.wait_for_url("https://wing.coupang.com/**", timeout=60_000)
            page.wait_for_load_state("domcontentloaded", timeout=30_000)

            cookies["wing"] = get_browser_cookies(context, ["https://wing.coupang.com"],
                requires=["XSRF-TOKEN"], err_msg="Failed to login in to Coupang Wing.")

            if not navigate_to_ads:
                return cookies

            # [ADS LOGIN] 광고센터 탭 클릭해서 새 탭 열기
            with context.expect_page() as new_page_info:
                page.locator('#wing-top-main-side-menu [data-menu-code="ADS_CENTER_ALL"] a').first.click()
            ad_page = new_page_info.value
            ad_page.wait_for_url("https://advertising.coupang.com/**", timeout=60_000)
            ad_page.wait_for_load_state("domcontentloaded", timeout=30_000)

            # [ADS MAIN] 광고 대시보드 이동 후 쿠키 추출
            time.sleep(3)
            cookies["ads"] = get_browser_cookies(context, ["https://advertising.coupang.com"])
            return cookies
        finally:
            browser.close()
