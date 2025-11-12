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
    access_token: str = str()
    refresh_token: str = str()

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
            data = self.login_begin()
            self.set_token(**data)
            self.login_history()
            return func(self, *args, **kwargs)
        return wrapper

    def login_begin(self) -> dict:
        from linkmerce.utils.headers import build_headers
        url = self.main_url + "/hp-prod/users/login"
        referer = self.main_url + "/login/login-main"
        body = {"username": self.get_variable("userid"),"password": self.get_variable("passwd")}
        headers = build_headers(host=url, contents="json", referer=referer, origin=self.main_url)
        headers["program-name"] = "login-main"
        with self.request("POST", url, json=body, headers=headers) as response:
            return response.json()["data"]

    def set_token(self, accessToken: str, refreshToken: str, **kwargs):
        self.access_token = accessToken
        self.refresh_token = refreshToken

    def login_history(self):
        from linkmerce.utils.headers import build_headers
        url = self.main_url + "/hp-prod/users/login-history"
        referer = self.main_url + "/login/login-main"
        headers = build_headers(host=url, authorization=self.get_authorization(), referer=referer, origin=self.main_url)
        headers["program-name"] = "login-main"
        self.request("POST", url, headers=headers)

    def get_authorization(self) -> str:
        return "Bearer " + self.access_token


class SabangnetLogin(LoginHandler, SabangnetAdmin):

    @LoginHandler.with_session
    def login(self, **kwargs) -> dict:
        data = self.login_begin()
        self.set_token(**data)
        self.login_history()
        return dict(cookies=self.get_cookies(), access_token=self.access_token, refresh_token=self.refresh_token)
