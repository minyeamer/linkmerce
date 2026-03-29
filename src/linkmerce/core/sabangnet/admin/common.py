from __future__ import annotations

from linkmerce.common.extract import Extractor, LoginHandler
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import Configs
    import datetime as dt


class SabangnetAdmin(Extractor):
    """사방넷 시스템에서 데이터를 조회하는 공통 클래스.

    `userid`, `passwd`, `domain`를 사용하여 로그인 후 토큰 기반으로 요청한다."""

    method: str | None = None
    main_url: str = "https://www.sabangnet.co.kr"
    admin_url: str = "http://sbadmin{domain}.sabangnet.co.kr"
    path: str | None = None
    access_token: str = str()
    refresh_token: str = str()

    def set_configs(self, configs: Configs = dict()):
        try:
            self.set_account(**configs)
        except TypeError:
            raise TypeError("Sabangnet requires variables for userid, passwd, and domain to authenticate.")

    def set_account(self, userid: str, passwd: str, domain: int, **configs):
        super().set_configs(dict(userid=userid, passwd=passwd, domain=domain, **configs))

    @property
    def origin(self) -> str:
        return self.admin_url.format(domain=self.get_config("domain"))

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.path)

    def with_token(func):
        """데이터 요청 전 로그인하고 `access_token`을 발급받는 데코레이터."""
        @functools.wraps(func)
        def wrapper(self: SabangnetAdmin, *args, **kwargs):
            data = self.login_begin()
            self.set_token(**data)
            self.login_history()
            return func(self, *args, **kwargs)
        return wrapper

    def login_begin(self) -> dict:
        """`userid`, `passwd`, `domain`를 가지고 로그인 요청한다."""
        from linkmerce.utils.headers import build_headers
        url = self.main_url + "/hp-prod/users/login"
        referer = self.main_url + "/login/login-main"
        body = {"username": self.get_config("userid"), "password": self.get_config("passwd")}
        headers = build_headers(host=url, contents="json", referer=referer, origin=self.main_url)
        headers["program-name"] = "login-main"
        with self.request("POST", url, json=body, headers=headers) as response:
            return response.json()["data"]

    def set_token(self, accessToken: str, refreshToken: str, **kwargs):
        """로그인 응답 결과에서 `access_token`을 추출한다."""
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

    def build_request_headers(self, **kwargs: str) -> dict[str, str]:
        from linkmerce.utils.headers import add_headers
        host = {"host": self.origin, "referer": self.origin, "origin": self.origin}
        return add_headers(self.get_request_headers(), authorization=self.get_authorization(), **host)

    def set_request_headers(self, **kwargs: str):
        super().set_request_headers(contents="json", **kwargs)


class SabangnetLogin(LoginHandler, SabangnetAdmin):
    """사방넷 로그인을 수행하여 쿠키와 토큰을 발급하는 클래스."""

    @LoginHandler.with_session
    def login(self, **kwargs) -> dict:
        """사방넷 로그인을 수행하고 쿠키와 토큰을 반환한다."""
        data = self.login_begin()
        self.set_token(**data)
        self.login_history()
        return {"cookies": self.get_cookies(), "access_token": self.access_token, "refresh_token": self.refresh_token}


def get_order_date_pair(
        start_date: dt.datetime | dt.date | str | Literal[":today:"] = ":today:",
        end_date: dt.datetime | dt.date | str | Literal[":start_date:", ":now:"] = ":start_date:",
    ) -> tuple[str, str]:
    """사방넷 주문 조회용 날짜 쌍을 생성한다."""
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
        start_date: dt.date | str | Literal[":base_date:", ":today:"] = ":base_date:",
        end_date: dt.date | str | Literal[":start_date:", ":today:"] = ":today:",
    ) -> tuple[str, str]:
    """사방넷 상품 조회용 날짜 쌍을 생성한다."""
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
