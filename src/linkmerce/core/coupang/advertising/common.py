from __future__ import annotations

from linkmerce.common.extract import Extractor, LoginHandler

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal


class CoupangAds(Extractor):
    method: str | None = None
    origin = "https://advertising.coupang.com"
    path: str | None = None

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.path)


class CoupangLogin(LoginHandler):
    origin = "https://advertising.coupang.com"

    @LoginHandler.with_session
    def login(
            self,
            userid: str,
            passwd: str,
            domain: Literal["wing","supplier"] = "wing",
            **kwargs
        ) -> dict:
        login_url = self.login_redirect(domain)
        # login_url = "https://xauth.coupang.com/auth/realms/seller/protocol/openid-connect/auth?client_id=wing-compat&scope={scope}&response_type=code&redirect_uri=https%3A%2F%2Fadvertising.coupang.com%2Fuser%2Fwing%2Fauthorization-callback&state={state}&code_challenge={code_challenge}&code_challenge_method=S256"
        xauth_url = self.login_begin(login_url)
        # xauth_url = "https://xauth.coupang.com/auth/realms/seller/login-actions/authenticate?session_code={session_code}&execution={execution}&client_id=wing-compat&tab_id={tab_id}&kc_locale=ko-KR"
        redirect_url = self.login_action(xauth_url, userid, passwd)
        # redirect_url = "https://advertising.coupang.com/user/wing/authorization-callback?state={state}&session_state={session_state}&code={code}"
        redirect_url = self.ads_redirect(redirect_url)
        # redirect_url = "/"
        self.fetch_dashboard()
        return self.get_cookies()

    def login_redirect(self, domain: Literal["wing","supplier"] = "wing") -> str:
        from linkmerce.utils.headers import build_headers
        url = f"https://advertising.coupang.com/user/{domain}/authorization"
        headers = build_headers(self.origin, referer=f"{self.origin}/user/login?returnUrl=%2Fdashboard", metadata="navigate", https=True)
        with self.request("GET", url, headers=headers, allow_redirects=False) as response:
            return response.headers.get("Location")

    def login_begin(self, login_url: str) -> str:
        from linkmerce.utils.headers import build_headers
        from bs4 import BeautifulSoup
        headers = build_headers(login_url, referer=self.origin, metadata="navigate", https=True)
        with self.request("GET", login_url, headers=headers, allow_redirects=False) as response:
            return BeautifulSoup(response.text, "html.parser").select_one("form").attrs.get("action")

    def login_action(self, xauth_url: str, userid: str, passwd: str) -> str:
        from linkmerce.utils.headers import build_headers
        body = dict(username=userid, password=passwd)
        headers = build_headers(xauth_url, origin="null", metadata="navigate", https=True, ajax=True)
        with self.request("POST", xauth_url, data=body, headers=headers, allow_redirects=False) as response:
            return response.headers.get("Location")

    def ads_redirect(self, redirect_url: str) -> str:
        from linkmerce.utils.headers import build_headers
        headers = build_headers(redirect_url, metadata="navigate", https=True)
        with self.request("GET", redirect_url, headers=headers, allow_redirects=False) as response:
            return response.headers.get("Location")

    def fetch_dashboard(self):
        from linkmerce.utils.headers import build_headers
        url = self.origin + "/dashboard"
        headers = build_headers(url, metadata="navigate", https=True)
        self.request("GET", url, headers=headers)
