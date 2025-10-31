from __future__ import annotations

from linkmerce.common.extract import Extractor
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import Variables


class CoupangAds(Extractor):
    method: str | None = None
    origin = "https://advertising.coupang.com"
    path: str | None = None

    def set_variables(self, variables: Variables = dict()):
        try:
            enum = {"advertising", "domain", "wing"}
            domain = variables.get("domain", "advertising")
            super().set_variables(dict(domain=(domain if domain in enum else "advertising")))
        except:
            pass

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.path)

    @property
    def domain(self) -> Literal["wing","supplier"]:
        return self.get_variable("domain")

    def authorize(func):
        @functools.wraps(func)
        def wrapper(self: CoupangAds, *args, **kwargs):
            if self.domain in {"wing", "supplier"}:
                self.redirect()
                xauth_url = self.auth_begin()
                # xauth_url = "https://xauth.coupang.com/auth/realms/seller/protocol/openid-connect/auth?client_id=wing-compat&scope={scope}&response_type=code&redirect_uri={redirect_uri}"
                # redirect_uri = quote("https://advertising.coupang.com/user/wing/authorization-callback&state={state}&code_challenge={code_challenge}&code_challenge_method=S256")
                callback_url = self.auth_action(xauth_url)
                # callback_url = "https://advertising.coupang.com/user/wing/authorization-callback?state={state}&session_state={session_state}&code={code}"
                self.auth_callback(callback_url)
                self.from_lnb()
                self.to_home()
            self.fetch_dashboard()
            return func(self, *args, **kwargs)
        return wrapper

    def redirect(self):
        from linkmerce.utils.headers import build_headers
        url = self.origin + "/home?from=WING_LNB"
        headers = build_headers(url, referer=f"https://{self.domain}.coupang.com/", metadata="navigate", https=True)
        self.request("GET", url, headers=headers)

    def auth_begin(self) -> str:
        from linkmerce.utils.headers import build_headers
        url = self.origin + "/user/wing/authorization"
        headers = build_headers(url, referer=f"https://{self.domain}.coupang.com/", metadata="navigate", https=True)
        with self.request("GET", url, headers=headers, allow_redirects=False) as response:
            return response.headers.get("Location")

    def auth_action(self, xauth_url: str) -> str:
        from linkmerce.utils.headers import build_headers
        headers = build_headers(xauth_url, referer=f"https://{self.domain}.coupang.com/", metadata="navigate", https=True)
        with self.request("GET", xauth_url, headers=headers, allow_redirects=False) as response:
            return response.headers.get("Location")

    def auth_callback(self, callback_url: str):
        from linkmerce.utils.headers import build_headers
        headers = build_headers(callback_url, metadata="navigate", https=True)
        self.request("GET", callback_url, headers=headers)

    def from_lnb(self):
        from linkmerce.utils.headers import build_headers
        url = self.origin + f"/from-{self.domain}-lnb"
        headers = build_headers(url, metadata="navigate", https=True)
        self.request("GET", url, headers=headers)

    def to_home(self):
        from linkmerce.utils.headers import build_headers
        url = self.origin + "/home"
        headers = build_headers(url, metadata="navigate", https=True)
        self.request("GET", url, headers=headers)

    def fetch_dashboard(self):
        from linkmerce.utils.headers import build_headers
        url = self.origin + "/dashboard"
        headers = build_headers(url, metadata="navigate", https=True)
        self.request("GET", url, headers=headers)
