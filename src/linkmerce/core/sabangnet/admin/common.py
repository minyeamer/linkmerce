from __future__ import annotations

from linkmerce.common.extract import Extractor, LoginHandler
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import Variables
    import datetime as dt


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
        body = {"username": self.get_variable("userid"), "password": self.get_variable("passwd")}
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

    def build_request_headers(self, **kwargs: str) -> dict[str,str]:
        from linkmerce.utils.headers import add_headers
        host = dict(host=self.origin, referer=self.origin, origin=self.origin)
        return add_headers(self.get_request_headers(), authorization=self.get_authorization(), **host)

    def set_request_headers(self, **kwargs: str):
        super().set_request_headers(contents="json", **kwargs)


class SabangnetLogin(LoginHandler, SabangnetAdmin):

    @LoginHandler.with_session
    def login(self, **kwargs) -> dict:
        data = self.login_begin()
        self.set_token(**data)
        self.login_history()
        return dict(cookies=self.get_cookies(), access_token=self.access_token, refresh_token=self.refresh_token)


def get_order_date_pair(
        start_date: dt.datetime | dt.date | str | Literal[":today:"] = ":today:",
        end_date: dt.datetime | dt.date | str | Literal[":start_date:",":now:"] = ":start_date:",
    ) -> tuple[str,str]:
    import datetime as dt

    def strftime(obj: dt.datetime | dt.date | str):
        if isinstance(obj, dt.datetime):
            date_string = obj.strftime("%Y%m%d%H%M%S")
        else:
            date_string = str(obj).replace('-', '').replace(':', '').replace(' ', '')

        while date_string[-2:] == "00":
            date_string = date_string[:-2]
        return date_string

    if isinstance(start_date, str) and (start_date == ":today:"):
        start_date = dt.date.today()
    start_date = strftime(start_date)

    if isinstance(end_date, str):
        if end_date == ":start_date:":
            return start_date, start_date[:8]
        elif end_date == ":now:":
            return start_date, dt.datetime.now().strftime("%Y%m%d%H%M%S")
    return start_date, strftime(end_date)


def get_product_date_pair(
        start_date: dt.date | str | Literal[":base_date:",":today:"] = ":base_date:",
        end_date: dt.date | str | Literal[":start_date:",":today:"] = ":today:",
    ) -> tuple[str,str]:
    import datetime as dt

    if isinstance(start_date, str):
        if start_date == ":base_date:":
            start_date = dt.date(1986, 1, 9)
        elif start_date == ":today:":
            start_date = dt.date.today()
    start_date = str(start_date).replace('-', '')

    if isinstance(end_date, str):
        if end_date == ":start_date:":
            return start_date, start_date
        elif end_date == ":today:":
            end_date = dt.date.today()
    return start_date, str(end_date).replace('-', '')
