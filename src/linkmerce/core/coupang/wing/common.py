from __future__ import annotations

from linkmerce.common.extract import Extractor, LoginHandler

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal


class CoupangWing(Extractor):
    """쿠팡 Wing 데이터를 조회하는 공통 클래스. 헤더에 로그인 쿠키가 제공되어야 한다."""

    method: str | None = None
    origin = "https://wing.coupang.com"
    path: str | None = None
    token_required: bool = False

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.path)

    def set_request_headers(self, **kwargs):
        """`token_required`가 활성화된 경우 세션 객체 또는 요청 헤더의 쿠키에서
        `XSRF-TOKEN` 값을 꺼내 `x-xsrf-token` 헤더로 추가한다."""
        from_cookies = {"XSRF-TOKEN": "x-xsrf-token"} if self.token_required else dict()
        super().set_request_headers(from_cookies=from_cookies, **kwargs)


class CoupangSupplierHub(CoupangWing):
    """쿠팡 서플라이어 허브 데이터를 수집하는 클래스."""
    origin = "https://supplier.coupang.com"


class CoupangLogin(LoginHandler):
    """쿠팡 Wing 로그인을 수행하여 쿠키를 발급하는 클래스."""
    origin = "https://wing.coupang.com"

    @LoginHandler.with_session
    def login(
            self,
            userid: str,
            passwd: str,
            domain: Literal["wing", "supplier"] = "wing",
            with_token: bool = True,
            **kwargs
        ) -> str:
        """로그인 수행 후 `XSRF-TOKEN`을 쿠키에 더한다."""
        self.origin = f"https://{domain}.coupang.com"
        self.vendor_login(userid, passwd)
        if with_token:
            self.fetch_xsrf_token()
        return self.get_cookies(to="str")

    def vendor_login(self, userid: str, passwd: str):
        """로그인 요청 후 응답 헤더에서 `Location` 대상의 리다이렉트를 처리한다."""
        login_url = self.fetch_main(allow_redirects=False)
        # login_url = "http://wing.coupang.com/login?ui_locales=ko-KR&service_cmdb_role=wing&sxauth_sdk_version={version}.RELEASE&returnUrl=http%3A%2F%2Fwing.coupang.com%2F"
        redirect_url = self.login_redirect(login_url)
        # redirect_url = "https://wing.coupang.com/sso/login?returnUrl=http%3A%2F%2Fwing.coupang.com%2F&max_age=&ui_locales=ko-KR&scope="
        xauth_url = self.login_begin(redirect_url)
        # xauth_url = "https://xauth.coupang.com/auth/realms/seller/login-actions/authenticate?session_code={session_code}&execution={execution}&client_id=wing&tab_id={tab_id}&kc_locale=ko-KR"
        redirect_url = self.login_action(xauth_url, userid, passwd)
        # redirect_url = "https://wing.coupang.com/sso/login?returnUrl=http%3A%2F%2Fwing.coupang.com%2F&state={state}&session_state={session_state}&code={code}"
        self.login_redirect(redirect_url, allow_redirects=True)
        self.fetch_main(allow_redirects=True)

    def fetch_main(self, allow_redirects: bool = True) -> str:
        headers = self.build_headers(self.origin, https=True)
        with self.request("GET", self.origin, headers=headers, allow_redirects=allow_redirects) as response:
            return response.headers.get("Location")

    def login_redirect(self, url: str, referer: str = str(), allow_redirects: bool = False) -> str:
        headers = self.build_headers(url, https=True, referer=referer)
        with self.request("GET", url, headers=headers, allow_redirects=allow_redirects) as response:
            return response.headers.get("Location")

    def login_begin(self, redirect_url: str) -> str:
        from bs4 import BeautifulSoup
        headers = self.build_headers(redirect_url, https=True)
        with self.request("GET", redirect_url, headers=headers) as response:
            source = BeautifulSoup(response.text, "html.parser")
            try:
                return source.select_one("form").attrs["action"]
            except:
                try:
                    return self.get_login_action_from_script(str(source.select_one("script")))
                except:
                    raise ConnectionError("Unable to find the xauth address.")

    def get_login_action_from_script(self, script: str) -> str:
        from linkmerce.utils.regex import regexp_extract, regexp_replace_map
        import json
        raw_json = regexp_extract(r"const out =\s+({[^;]+});", script)
        raw_json = regexp_replace_map({r"/\*.*\*/": '', r",\s*\}": '}', r",\s*\]": ']'}, raw_json)
        return json.loads(raw_json)["url"]["loginAction"]

    def login_action(self, xauth_url: str, userid: str, passwd: str) -> str:
        body = {"username": userid, "password": passwd}
        headers = self.build_headers(xauth_url, contents="form", https=True)
        with self.request("POST", xauth_url, data=body, headers=headers, allow_redirects=False) as response:
            return response.headers.get("Location")

    def fetch_xsrf_token(self):
        url = self.origin + "/tenants/sfl-portal/card/cre/resource"
        headers = self.build_headers(url, referer=self.origin, ajax=True)
        self.request("GET", url, headers=headers)
