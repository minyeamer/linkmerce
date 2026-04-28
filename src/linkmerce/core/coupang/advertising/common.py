from __future__ import annotations

from linkmerce.common.extract import Extractor, LoginHandler

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal


class CoupangAds(Extractor):
    """쿠팡 광고센터 로그인 쿠키를 가지고 데이터를 조회하는 공통 클래스.

    - **URL**: https://advertising.coupang.com

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.
    """

    method: str | None = None
    origin = "https://advertising.coupang.com"
    path: str | None = None

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.path)

    def post_init(self, **kwargs):
        self.require_cookies()

    def fetch_dashboard(self):
        """쿠팡 광고 대시보드로 이동한다."""
        from linkmerce.utils.headers import build_headers
        url = self.origin + "/dashboard"
        headers = build_headers(url, metadata="navigate", https=True)
        self.request("GET", url, headers=headers)


class CoupangLogin(LoginHandler):
    """쿠팡 광고센터 로그인을 수행하여 쿠키를 발급하는 클래스.

    - **URL**: https://advertising.coupang.com/user/login
    """
    origin = "https://advertising.coupang.com"

    @LoginHandler.with_session
    def login(
            self,
            userid: str,
            passwd: str,
            domain: Literal["wing", "supplier"] = "wing",
            **kwargs
        ) -> str:
        """로그인 요청 후 응답 헤더에서 `Location` 대상의 리다이렉트를 처리한다.

        Parameters
        ----------
        userid: str
            쿠팡 Wing 또는 서플라이어 허브 로그인 아이디
        passwd: str
            쿠팡 Wing 또는 서플라이어 허브 로그인 비밀번호
        domain: str
            로그인할 판매자 계정의 도메인
                - `"wing"`: 쿠팡 Wing (기본값)
                - `"supplier"`: 쿠팡 서플라이어 허브

        Returns
        -------
        str
            쿠팡 광고센터 로그인 쿠키
        """
        login_url = self.login_redirect(domain)
        # login_url = "https://xauth.coupang.com/auth/realms/seller/protocol/openid-connect/auth?client_id=wing-compat&scope={scope}&response_type=code&redirect_uri=https%3A%2F%2Fadvertising.coupang.com%2Fuser%2Fwing%2Fauthorization-callback&state={state}&code_challenge={code_challenge}&code_challenge_method=S256"
        xauth_url = self.login_begin(login_url)
        # xauth_url = "https://xauth.coupang.com/auth/realms/seller/login-actions/authenticate?session_code={session_code}&execution={execution}&client_id=wing-compat&tab_id={tab_id}&kc_locale=ko-KR"
        redirect_url = self.login_action(xauth_url, userid, passwd)
        # redirect_url = "https://advertising.coupang.com/user/wing/authorization-callback?state={state}&session_state={session_state}&code={code}"
        redirect_url = self.ads_redirect(redirect_url)
        # redirect_url = "/"
        self.fetch_dashboard()
        return self.get_cookies(to="str")

    def login_redirect(self, domain: Literal["wing", "supplier"] = "wing") -> str:
        url = f"{self.origin}/user/{domain}/authorization"
        referer = f"{self.origin}/user/login?returnUrl=%2Fdashboard"
        headers = self.build_headers(self.origin, referer=referer, metadata="navigate", https=True)
        with self.request("GET", url, headers=headers, allow_redirects=False) as response:
            return response.headers.get("Location")

    def login_begin(self, login_url: str) -> str:
        from bs4 import BeautifulSoup
        headers = self.build_headers(login_url, referer=self.origin, metadata="navigate", https=True)
        with self.request("GET", login_url, headers=headers, allow_redirects=False) as response:
            return BeautifulSoup(response.text, "html.parser").select_one("form").attrs.get("action")

    def login_action(self, xauth_url: str, userid: str, passwd: str) -> str:
        body = {"username": userid, "password": passwd}
        headers = self.build_headers(xauth_url, origin="null", metadata="navigate", https=True, ajax=True)
        with self.request("POST", xauth_url, data=body, headers=headers, allow_redirects=False) as response:
            return response.headers.get("Location")

    def ads_redirect(self, redirect_url: str) -> str:
        headers = self.build_headers(redirect_url, metadata="navigate", https=True)
        with self.request("GET", redirect_url, headers=headers, allow_redirects=False) as response:
            return response.headers.get("Location")

    def fetch_dashboard(self):
        """쿠팡 광고 로그인 후 대시보드로 이동한다."""
        url = self.origin + "/dashboard"
        headers = self.build_headers(url, metadata="navigate", https=True)
        self.request("GET", url, headers=headers)
