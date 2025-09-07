from __future__ import annotations

from linkmerce.common.extract import Extractor
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import Variables, JsonObject
    from requests import Session


def has_cookies(session: Session, cookies: str = str()) -> bool:
    from linkmerce.utils.headers import build_headers
    url = "https://gw.searchad.naver.com/auth/local/naver-cookie/exist"
    origin = "https://searchad.naver.com"
    referer = f"{origin}/membership/select-account?redirectUrl=https:%2F%2Fmanage.searchad.naver.com"
    headers = build_headers(cookies=cookies, referer=referer, origin=origin)
    with session.get(url, headers=headers) as response:
        return (response.text == "true")


def has_permission(session: Session, customer_id: int | str, cookies: str = str()) -> bool:
    from linkmerce.utils.headers import build_headers
    url = f"https://gw.searchad.naver.com/auth/local/naver-cookie/ads-accounts/{customer_id}"
    origin = "https://searchad.naver.com"
    referer = f"{origin}/membership/select-account?redirectUrl=https%3A//manage.searchad.naver.com"
    headers = build_headers(cookies=cookies, referer=referer, origin=origin)
    with session.get(url, headers=headers) as response:
        json = response.json()
        return isinstance(json, dict) and (json.get("status") != 403)


class SearchAdManager(Extractor):
    method: str | None = None
    origin: str = "https://searchad.naver.com"
    main_url: str = "https://manage.searchad.naver.com"
    api_url: str = "https://gw.searchad.naver.com/api"
    auth_url: str = "https://gw.searchad.naver.com/auth"
    path: str | None = None
    access_token: str = str()
    refresh_token: str = str()

    def set_variables(self, variables: Variables = dict()):
        try:
            self.set_customer_id(**variables)
        except TypeError:
            raise TypeError("Naver SearchAd requires variables for customer_id to authenticate.")

    def set_customer_id(self, customer_id: int | str, **variables):
        super().set_variables(dict(customer_id=customer_id, **variables))

    @property
    def url(self) -> str:
        return self.concat_path(self.api_url, self.path)

    @property
    def customer_id(self) -> int | str:
        return self.get_variable("customer_id")

    def with_token(func):
        @functools.wraps(func)
        def wrapper(self: SearchAdManager, *args, **kwargs):
            self.authenticate()
            self.authorize()
            return func(self, *args, **kwargs)
        return wrapper

    def authenticate(self):
        cookies = self.get_request_headers().get("cookies", str())
        if not has_permission(self.get_session(), self.customer_id, cookies):
            from linkmerce.common.exceptions import AuthenticationError
            raise AuthenticationError("You don't have permission to access this account.")

    def authorize(self):
        from urllib.parse import quote
        url = self.auth_url + "/local/naver-cookie"
        referer = f"{self.origin}/membership/select-account?redirectUrl={quote(self.main_url)}"
        headers = dict(self.get_request_headers(), referer=referer, origin=self.origin)
        response = self.get_session().post(url, headers=headers, params=dict(customerId=self.customer_id)).json()
        self.set_token(**response)

    def refresh(self, referer: str = str()):
        from urllib.parse import quote
        url = self.auth_url + "/local/extend"
        data = dict(refreshToken=self.refresh_token)
        referer = f"{self.origin}/membership/select-account?redirectUrl={quote(referer or self.main_url)}"
        headers = dict(self.get_request_headers(), referer=referer, origin=self.main_url)
        response = self.get_session().put(url, json=data, headers=headers).json()
        self.set_token(**response)

    def set_token(self, token: str, refreshToken: str, **kwargs):
        self.access_token = token
        self.refresh_token = refreshToken

    def get_authorization(self) -> str:
        return "Bearer " + self.access_token

    def is_valid_response(self, response: JsonObject) -> bool:
        if isinstance(response, dict):
            msg = response.get("title") or response.get("message") or str()
            if (msg == "Forbidden") or ("권한이 없습니다." in msg) or ("인증이 만료됐습니다." in msg):
                from linkmerce.common.exceptions import UnauthorizedError
                raise UnauthorizedError(msg)
            return (not response.get("code"))
        return False
