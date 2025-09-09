from __future__ import annotations

from linkmerce.common.extract import Extractor, LoginHandler
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import Variables


class SabangnetAdmin(Extractor):
    method: str | None = None
    main_url: str = "https://www.sabangnet.co.kr"
    admin_url: str = "http://sbadmin{domain}.sabangnet.co.kr"
    path: str | None = None
    token: str = str()

    def set_variables(self, variables: Variables = dict()):
        try:
            self.set_account(**variables)
        except TypeError:
            raise TypeError("Sabangnet requires variables for userid, passwd, and domain to authenticate.")

    def set_account(self, userid: str, passwd: str, domain: int, **variables):
        super().set_variables(dict(userid=userid, passwd=passwd, domain=domain, **variables))

    @property
    def origin(self) -> str:
        return self.admin_url.format(domain=self.get_variable("domain"))

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.path)

    def with_token(func):
        @functools.wraps(func)
        def wrapper(self: SabangnetAdmin, *args, **kwargs):
            self.login_begin()
            self.set_token()
            self.login_redirect()
            return func(self, *args, **kwargs)
        return wrapper

    def login_begin(self):
        from linkmerce.utils.headers import build_headers
        url = self.main_url + "/login.php"
        body = {"mode": "acts", "id": self.get_variable("userid"), "passwd": self.get_variable("passwd")}
        headers = build_headers(host=url, contents=dict(type="form"), referer=self.main_url, origin=self.main_url, https=True)
        self.request("POST", url, data=self.encode_data(body), headers=headers)

    def login_redirect(self):
        from linkmerce.utils.headers import build_headers
        url = self.main_url + "/new_index.html"
        body = {
            "referr": "-1",
            "returl": "/index.html",
            "mode": "login",
            "id": self.get_variable("userid"),
            "passwd": self.get_variable("userid"),
            "admin_fg": " ",
        }
        headers = build_headers(host=url, contents=dict(type="form"), referer=(self.main_url + "/login.php"), origin=self.main_url, https=True)
        self.request("POST", url, data=self.encode_data(body), headers=headers)

    def encode_data(self, data: dict, sep='&') -> str:
        return str(sep).join([f"{k}={v}" for k,v in data.items()])

    def set_token(self):
        self.token = self.get_session().cookies.get("token")

    def get_authorization(self) -> str:
        return "Bearer " + self.token


class SabangnetLogin(LoginHandler, SabangnetAdmin):

    @LoginHandler.with_session
    def login(self, **kwargs) -> dict:
        self.login_begin()
        self.set_token()
        self.login_redirect()
        return dict(cookies=self.get_cookies(), token=self.token)
