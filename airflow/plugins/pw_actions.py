from __future__ import annotations

from typing import TYPE_CHECKING
import os

if TYPE_CHECKING:
    from typing import Iterable, Literal, Sequence
    from playwright.sync_api import Browser, BrowserContext, Page
    from pathlib import Path


def _ws_endpoint() -> str:
    """환경변수에서 Playwright 서버의 WebSocket 엔드포인트 URL을 구성하여 반환한다."""
    import os
    url = os.environ.get("PLAYWRIGHT_CONNECT_URL", "ws://playwright")
    port = os.environ.get("PLAYWRIGHT_PORT", "3000")
    return f"{url}:{port}"


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
            cookie_names = {cookie["name"] for cookie in cookies}
            if set(requires).issubset(cookie_names):
                return "; ".join(f"{cookie['name']}={cookie['value']}" for cookie in cookies)
        elif cookies:
            return "; ".join(f"{kv['name']}={kv['value']}" for kv in cookies)
        time.sleep(1)

    from linkmerce.common.exceptions import AuthenticationError
    raise AuthenticationError(err_msg)


def save_browser_cookies(cookies: str, save_to: str | Path, mkdir: bool = True):
    """Playwright 브라우저에서 추출한 쿠키를 지정된 경로에 저장한다."""
    from pathlib import Path
    file_path = save_to if isinstance(save_to, Path) else Path(save_to)
    if mkdir:
        file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(cookies, encoding="utf-8")


def get_browser_version() -> int:
    """Playwright 브라우저의 크롬 메이저 버전을 반환한다."""
    from playwright.sync_api import sync_playwright

    with sync_playwright() as playwright:
        browser = playwright.chromium.connect(_ws_endpoint())
        try:
            return int(browser.version.split(".", maxsplit=1)[0])
        finally:
            browser.close()


###################################################################
############################# Coupang #############################
###################################################################

def login_coupang(
        userid: str,
        passwd: str,
        navigate_to_ads: bool = True,
        timeout: float = 60.,
    ) -> dict:
    """Playwright 브라우저로 쿠팡 Wing/광고 로그인을 수행하고
    `{wing: "...", ads: "..."}` 형태의 쿠키를 반환한다.

    주의) Playwright 브라우저가 `headless=False` 옵션 또는 가상 렌더링을 지원하지 않으면   
    `Access Denied` 페이지가 발생하며 로그인이 실패한다.
    """
    from playwright.sync_api import sync_playwright
    import time

    cookies = dict()

    def wing_login_action(page: Page):
        """쿠팡 Wing 판매자센터로 이동하여 로그인을 수행한다."""
        page.goto("https://wing.coupang.com/", wait_until="domcontentloaded")

        page.wait_for_selector("input#username", timeout=(timeout*1000))
        page.locator("input#username").type(userid, delay=110)
        page.locator("input#password").type(passwd, delay=110)
        page.locator("input[type='submit'], button[type='submit']").first.click()

        page.wait_for_url("https://wing.coupang.com/**", timeout=(timeout*1000))
        page.wait_for_load_state("domcontentloaded", timeout=30_000)


    def get_wing_cookies(context: BrowserContext) -> str:
        """쿠팡 Wing 판매자센터에서 쿠키에 `XSRF-TOKEN` 값이 추가될 때까지 대기하고, 쿠키 문자열을 추출한다."""
        return get_browser_cookies(
            context,
            urls = ["https://wing.coupang.com"],
            requires = ["XSRF-TOKEN"],
            err_msg = "Failed to login in to Coupang Wing.",
        )


    def open_ads_menu(context: BrowserContext, page: Page):
        """쿠팡 Wing 판매자센터의 사이드 메뉴에서 '광고센터' 탭을 클릭해서 새 탭에 쿠팡 광고센터를 연다."""
        try:
            with context.expect_page() as new_page_info:
                selector = '#wing-top-main-side-menu [data-menu-code="ADS_CENTER_ALL"] a'
                page.locator(selector).first.click()
                ad_page = new_page_info.value
        except Exception:
            ad_url = "https://advertising.coupang.com/relay/wing/home?from=WING_LNB"
            page.goto(ad_url, wait_until="domcontentloaded")
            ad_page = page

        ad_page.wait_for_url("https://advertising.coupang.com/**", timeout=(timeout*1000))
        ad_page.wait_for_load_state("domcontentloaded", timeout=30_000)


    def get_ads_cookies(context: BrowserContext):
        """쿠팡 Wing 광고센터로 이동한 후 쿠키 문자열을 추출한다."""
        return get_browser_cookies(
            context,
            urls = ["https://advertising.coupang.com"],
            err_msg = "Failed to login in to Coupang Ads.",
        )


    with sync_playwright() as playwright:
        browser = playwright.chromium.connect(_ws_endpoint())
        try:
            context = browser.new_context()
            page = context.new_page()
            wing_login_action(page)
            cookies["wing"] = get_wing_cookies(context)

            if not navigate_to_ads:
                return cookies

            open_ads_menu(context, page)
            time.sleep(3)
            cookies["ads"] = get_ads_cookies(context)
            return cookies
        finally:
            browser.close()


###################################################################
############################### Ebay ##############################
###################################################################

def login_ebay(
        userid: str,
        passwd: str,
        site_type: Literal["esmplus", "auction", "gmarket"] = "esmplus",
        where: Literal["esmplus", "ad", "adcenter"] = "esmplus",
        timeout: float = 60,
    ) -> str:
    """Playwright 브라우저로 ESM PLUS 또는 광고센터에 로그인하고 쿠키 문자열을 반환한다.

    ESM PLUS 로그인 화면은 `face.js`로 브라우저 지문을 수집하므로,
    안정적인 세션을 만들기 위해 브라우저를 활용해야 한다.

    Returns:
        `'key=value; key=value; ...'` 형식의 ESM PLUS 쿠키 문자열
    """
    from playwright.sync_api import sync_playwright

    site_map = {"esmplus": "esm", "auction": "auction", "gmarket": "gmarket"}
    if site_type not in site_map:
        raise ValueError(f"Invalid site: {site_type}")

    def login_esmplus(page: Page):
        """ESM PLUS로 이동하여 로그인을 수행한다."""
        page.goto("https://signin.esmplus.com/login", wait_until="domcontentloaded")

        page.locator(f".button__tab--{site_map[site_type]}").first.click()

        page.locator("#typeMemberInputId01").first.type(userid, delay=110)
        page.locator("#typeMemberInputPassword01").first.type(passwd, delay=110)
        page.locator('.box__submit > button').first.click()

        page.wait_for_url("https://www.esmplus.com/**", timeout=(timeout*1000))
        page.wait_for_load_state("domcontentloaded", timeout=30_000)


    def get_esmplus_cookies(context: BrowserContext):
        """ESM PLUS에서 쿠키에 `ESM_REQUEST_AUTH_PC` 값이 추가될 때까지 대기하고, 쿠키 문자열을 추출한다."""
        return get_browser_cookies(
                context,
                urls = ["https://www.esmplus.com/"],
                requires = ["ESM_REQUEST_AUTH_PC"],
                err_msg = "Failed to login in to ESM PLUS.",
            )


    def login_auction_adcenter(page: Page):
        """AUCTION 광고센터로 이동하여 로그인을 수행한다."""
        login_url = f"https://ad.esmplus.com/Member/SignIn/LogOn?ReturnUrl=%2fcpc%2fmain"
        page.goto(login_url, wait_until="domcontentloaded")

        page.locator(f".button__tab--{site_map[site_type]}").first.click()

        page.locator('div[style*="display: block"] input[type="text"]').first.type(userid, delay=110)
        page.locator('div[style*="display: block"] input[type="password"]').first.type(passwd, delay=110)
        page.locator('#lnkLogin').first.click()

        page.wait_for_url("https://ad.esmplus.com/cpc/main**", timeout=(timeout*1000))
        page.wait_for_load_state("networkidle", timeout=30_000)
        page.reload(wait_until="networkidle", timeout=(timeout*1000))


    def get_ad_cookies(context: BrowserContext):
        """AUCTION 광고센터에서 인증 및 `/cpc` 세션 쿠키가 추가될 때까지 대기하고, 쿠키 문자열을 추출한다."""
        return get_browser_cookies(
                context,
                urls = ["https://ad.esmplus.com/cpc/main"],
                requires = ["AD_AUTH", "ASP.NET_SessionId"],
                err_msg = "Failed to login in to AUCTION Ad Center.",
            )


    def login_gmarket_adcenter(page: Page):
        """Gmarket 광고센터로 이동하여 로그인을 수행한다."""
        page.goto("https://adcenter.esmplus.com/login", wait_until="domcontentloaded")

        page.locator(f".button__tab--{site_map[site_type]}").first.click()

        page.locator("#login-username").first.type(userid, delay=110)
        page.locator("#login-password").first.type(passwd, delay=110)
        page.locator('form > button[type="submit"]').first.click()

        page.wait_for_url("https://adcenter.esmplus.com/**", timeout=(timeout*1000))
        page.wait_for_load_state("domcontentloaded", timeout=30_000)


    def get_adcenter_cookies(context: BrowserContext):
        """Gmarket 광고센터에서 쿠키에 `ADC_AUTH` 값이 추가될 때까지 대기하고, 쿠키 문자열을 추출한다."""
        return get_browser_cookies(
                context,
                urls = ["https://adcenter.esmplus.com"],
                requires = ["ADC_AUTH"],
                err_msg = "Failed to login in to Gmarket Ad Center.",
            )


    with sync_playwright() as playwright:
        browser = playwright.chromium.connect(_ws_endpoint())
        try:
            context = browser.new_context()
            page = context.new_page()

            if where == "adcenter":
                login_gmarket_adcenter(page)
                return get_adcenter_cookies(context)

            login_esmplus(page)
            cookies = get_esmplus_cookies(context)

            if where == "ad":
                login_auction_adcenter(page)
                return get_ad_cookies(context)
            return cookies
        finally:
            browser.close()


###################################################################
############################## Naver ##############################
###################################################################

class NaverLoginError(RuntimeError):
    """네이버 로그인 과정에서 발생하는 기본 예외."""
    ...

class NaverLoginFailedError(NaverLoginError):
    """아이디 또는 비밀번호가 틀렸을 때 발생하는 예외."""
    ...

class NaverCaptchaError(NaverLoginError):
    """자동입력 방지 문자(CAPTCHA)가 표시되어 자동 로그인이 불가능할 때 발생하는 예외."""
    ...

class NaverAccountProtectedError(NaverLoginError):
    """계정 보호조치가 적용되어 로그인이 차단되었을 때 발생하는 예외."""
    ...

class NaverPasswordChangeError(NaverLoginError):
    """비밀번호 변경이 요구되어 로그인이 차단되었을 때 발생하는 예외."""
    ...

class NaverStateExpiredError(NaverLoginError):
    """저장된 네이버 로그인 상태가 만료되었고, 네이버 로그인도 실패했을 때 발생하는 예외."""
    ...


def login_naver(
        userid: str,
        passwd: str,
        storage_state: str | Path | None = None,
    ) -> str:
    """Playwright 브라우저로 네이버 로그인을 수행하고 쿠키 문자열을 반환한다.

    저장된 세션(storage_state)이 있으면 불러와서 로그인을 스킵하고,
    세션이 만료된 경우 아이디/비밀번호로 재로그인을 시도한다.

    Returns:
        `'key=value; key=value; ...'` 형식의 네이버 쿠키 문자열

    Raises:
        NaverCaptchaError: 자동입력 방지 문자(CAPTCHA)가 표시된 경우
        NaverAccountProtectedError: 계정 보호조치가 적용된 경우
        NaverPasswordChangeError: 비밀번호 변경이 요구된 경우
        NaverLoginFailedError: 아이디/비밀번호 오류 등 기타 로그인 실패
        NaverStateExpiredError: 세션 만료 후 재로그인에도 실패한 경우
    """
    from playwright.sync_api import sync_playwright
    from linkmerce.common.exceptions import AuthenticationError
    import time

    has_state = storage_state and os.path.exists(storage_state)

    def new_context_with_state(browser: Browser) -> BrowserContext:
        """저장된 네이버 로그인 상태가 있다면 불러온다. 추가로, 모바일 에뮬레이션을 사용한다."""
        options = {"storage_state": storage_state} if has_state else dict()
        mobile_device = playwright.devices["Galaxy S24"]
        return browser.new_context(**mobile_device, **options)


    def goto_login_page(page: Page):
        """네이버 모바일 메인 페이지에서 로그인 페이지로 이동한다."""
        page.locator('[href="/aside/"]').tap()
        page.locator(".ah_link_user").tap()


    def naver_login_action(page: Page):
        """네이버 모바일 로그인 페이지에서 아이디/비밀번호를 입력하고 로그인 버튼을 누른다."""
        import random

        # 아이디 입력
        page.locator("input#id").tap()
        time.sleep(random.uniform(0.3, 0.6))
        page.locator("input#id").type(userid, delay=110)
        time.sleep(random.uniform(0.3, 0.6))

        # 비밀번호 입력
        page.locator("input#pw").tap()
        time.sleep(random.uniform(0.3, 0.6))
        page.locator("input#pw").type(passwd, delay=110)
        time.sleep(random.uniform(0.3, 0.6))

        # 로그인 버튼
        page.locator("buttom#submit_btn").tap()


    def validate_login(page: Page):
        """네이버 로그인 후 페이지 내용을 확인하여
        자동입력 방지 문자, 보호조치, 비밀번호 변경 등의 사례가 감지되면 오류를 발생시킨다."""
        url = page.url

        if page.locator("#rcapt").count() > 0:
            # 자동입력 방지 문자(CAPTCHA)가 표시되었습니다.
            raise NaverCaptchaError("CAPTCHA challenge detected.")

        if page.locator("#divWarning").count() > 0:
            # 계정 보호 조치가 적용되어 로그인이 차단되었습니다.
            raise NaverAccountProtectedError("Account protected or restricted.")

        if page.locator("#error_message").count() > 0:
            message = page.locator("#error_message").first.text_content().strip()
            raise NaverLoginFailedError(f"Login failed - {message}")

        if "passwd" in url or "renewal" in url:
            # 비밀번호 변경이 필요합니다.
            raise NaverPasswordChangeError("Mandatory password change required.")


    def get_naver_cookies(context: BrowserContext) -> str:
        """네이버 로그인 후 쿠키에 `NID_SES`가 추가될 때까지 대기하고, 쿠키 문자열을 추출한다."""
        return get_browser_cookies(
            context,
            urls = ["https://m.naver.com/"],
            requires = ["NID_SES"],
            err_msg = "Failed to login in to Naver.",
        )


    with sync_playwright() as playwright:
        browser = playwright.chromium.connect(_ws_endpoint())
        try:
            context = new_context_with_state(browser)
            page = context.new_page()
            page.goto("https://m.naver.com/", wait_until="domcontentloaded")

            # 저장된 네이버 로그인 상태가 있다면 쿠키 유효성을 우선 확인한다.
            if has_state:
                try:
                    cookies = get_naver_cookies(context)
                    context.storage_state(path=storage_state)
                    return cookies
                except AuthenticationError:
                    pass

            goto_login_page(page)
            naver_login_action(page)
            time.sleep(3)
            validate_login(page)

            try:
                cookies = get_naver_cookies(context)
                if storage_state:
                    context.storage_state(path=storage_state)
                return cookies
            except AuthenticationError as error:
                if has_state:
                    msg = "The provided `storage_state` is expired or invalid. Login attempt failed."
                    raise NaverStateExpiredError(msg)
                raise error
        finally:
            browser.close()
