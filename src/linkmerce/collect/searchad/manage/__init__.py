from __future__ import annotations

from linkmerce.collect import Collector
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Callable, Hashable, IO, Literal, TypeVar
    from linkmerce.types import JsonSerialize
    _KT = TypeVar("_KT", Hashable)
    _VT = TypeVar("_VT", Any)

    from requests import Session
    from aiohttp.client import ClientSession


class SearchAdManager(Collector):
    origin: str = "https://searchad.naver.com"
    main_url: str = "https://manage.searchad.naver.com"
    api_url: str = "https://gw.searchad.naver.com/api"
    auth_url: str = "https://gw.searchad.naver.com/auth"
    path: str
    access_token: str = str()
    refresh_token: str = str()

    def __init__(
            self,
            customer_id: int | str,
            session: Session | ClientSession | None = None,
            params: dict | list[tuple] | bytes | None = None,
            body: dict | dict | list[tuple] | bytes | IO | JsonSerialize | None = None,
            headers: dict[_KT,_VT] = dict(),
            parser: Literal["default"] | Callable | None = "default",
        ):
        self.set_customer_id(customer_id)
        super().__init__(session, params, body, headers, parser)

    def get_customer_id(self) -> int | str:
        return self.customer_id

    def set_customer_id(self, customer_id: int | str):
        self.customer_id = customer_id

    @property
    def url(self) -> str:
        return self.api_url + ('/' * (not self.path.startswith('/'))) + self.path

    def with_token(func):
        @functools.wraps(func)
        def wrapper(self: SearchAdManager, *args, **kwargs):
            self.validate()
            self.authorize()
            self.link_customer()
            return func(self, *args, **kwargs)
        return wrapper

    def validate(self):
        from urllib.parse import quote
        url = self.auth_url + "/local/naver-cookie/exist"
        redirect_url = f"{self.origin}/login?autoLogin=true&returnUrl={quote(self.main_url + '/front')}&returnMethod=get"
        headers = super().get_request_headers(referer=redirect_url, origin=self.origin)
        response = self.request_text("GET", url, headers=headers)
        if response.strip() != "true":
            from linkmerce.exceptions import AuthenticationError
            raise AuthenticationError("Authentication failed: cookies are invalid.")

    def authorize(self):
        from urllib.parse import quote
        url = self.auth_url + "/local/naver-cookie"
        redirect_url = f"{self.origin}/naver?returnUrl={quote(self.main_url + '/front')}&returnMethod=get"
        headers = super().get_request_headers(referer=redirect_url, origin=self.origin, **{"content-type":"text/plain"})
        response = self.request_json("POST", url, headers=headers)
        self.set_token(**response)

    def refresh(self, referer: str = str()):
        url = self.auth_url + "/local/extend"
        params = dict(refreshToken=self.refresh_token)
        referer = referer or (self.main_url + "/front")
        headers = super().get_request_headers(referer=referer, origin=self.main_url)
        response = self.request_json("PUT", url, params=params, headers=headers)
        self.set_token(**response)

    def set_token(self, token: str, refreshToken: str, **kwargs):
        self.access_token = token
        self.refresh_token = refreshToken

    def link_customer(self, referer: str = str()):
        url = f"{self.api_url}/customer-links/{self.get_customer_id()}/token"
        referer = referer or (self.main_url + "/front")
        headers = super().get_request_headers(authorization=self.get_authorization(), referer=referer, origin=self.main_url)
        self.access_token = self.request_json("GET", url, headers=headers)["token"]

    def get_authorization(self) -> str:
        return "Bearer " + self.access_token
