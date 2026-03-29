from __future__ import annotations

from linkmerce.common.extract import Extractor
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import Configs, JsonObject
    from requests import Session


def has_cookies(session: Session, cookies: str = str()) -> bool:
    """네이버 로그인 쿠키가 유효한지 검증한다."""
    from linkmerce.utils.headers import build_headers
    url = "https://gw.searchad.naver.com/auth/local/naver-cookie/exist"
    origin = "https://searchad.naver.com"
    referer = f"{origin}/membership/select-account?redirectUrl=https:%2F%2Fmanage.searchad.naver.com"
    headers = build_headers(cookies=cookies, referer=referer, origin=origin)
    with session.get(url, headers=headers) as response:
        return (response.text == "true")


def has_permission(session: Session, customer_id: int | str, cookies: str = str()) -> bool:
    """네이버 로그인 쿠키가 계정ID에 대한 접근 권한이 있는지 확인한다."""
    return bool(whoami(session, customer_id, cookies))


def whoami(session: Session, customer_id: int | str, cookies: str = str()) -> dict | None:
    """네이버에서 현재 로그인된 사용자의 검색광고 계정ID를 조회한다."""
    from linkmerce.utils.headers import build_headers
    import json
    url = f"https://gw.searchad.naver.com/auth/local/naver-cookie/ads-accounts/{customer_id}"
    origin = "https://searchad.naver.com"
    referer = f"{origin}/membership/select-account?redirectUrl=https%3A//manage.searchad.naver.com"
    headers = build_headers(cookies=cookies, referer=referer, origin=origin)
    with session.get(url, headers=headers) as response:
        body = json.loads(response.text)
        return body.get("customer") if isinstance(body, dict) else None


class SearchAdManager(Extractor):
    """네이버 검색광고 시스템에서 데이터를 조회하는 공통 클래스.

    네이버 로그인 쿠키와 `customer_id`를 사용하여 `access_token`을 발급받고 토큰 기반으로 요청한다."""

    method: str | None = None
    origin: str = "https://searchad.naver.com"
    main_url: str = "https://manage.searchad.naver.com"
    api_url: str = "https://gw.searchad.naver.com/api"
    auth_url: str = "https://gw.searchad.naver.com/auth"
    path: str | None = None
    access_token: str = str()
    refresh_token: str = str()

    def set_configs(self, configs: Configs = dict()):
        try:
            self.set_customer_id(**configs)
        except TypeError:
            raise TypeError("Naver SearchAd requires variables for customer_id to authenticate.")

    def set_customer_id(self, customer_id: int | str, **configs):
        super().set_configs(dict(customer_id=customer_id, **configs))

    @property
    def url(self) -> str:
        return self.concat_path(self.api_url, self.path)

    @property
    def customer_id(self) -> int | str:
        return self.get_config("customer_id")

    def with_token(func):
        """네이버 로그인 쿠키와 `customer_id`를 사용하여 `access_token`을 발급받는 데코레이터."""
        @functools.wraps(func)
        def wrapper(self: SearchAdManager, *args, **kwargs):
            self.authenticate()
            self.authorize()
            return func(self, *args, **kwargs)
        return wrapper

    def authenticate(self):
        """네이버 로그인 쿠키가 올바른지 검증한다."""
        cookies = self.get_request_headers().get("cookie", str())
        if not has_permission(self.get_session(), self.customer_id, cookies):
            from linkmerce.common.exceptions import AuthenticationError
            raise AuthenticationError("You don't have permission to access this account.")

    def authorize(self):
        """네이버 로그인 쿠키와 `customer_id`를 사용하여 `access_token`을 발급받는다."""
        from urllib.parse import quote
        url = self.auth_url + "/local/naver-cookie"
        referer = f"{self.origin}/membership/select-account?redirectUrl={quote(self.main_url)}"
        headers = dict(self.get_request_headers(), referer=referer, origin=self.origin)
        response = self.get_session().post(url, headers=headers, params={"customerId": self.customer_id}).json()
        self.set_token(**response)

    def refresh(self, referer: str = str()):
        """5분 유효기간이 있는 `access_token`이 만료되면 새로고침한다."""
        from urllib.parse import quote
        url = self.auth_url + "/local/extend"
        data = {"refreshToken": self.refresh_token}
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
        """요청 중 토큰 인증 오류가 발생하면 `UnauthorizedError`를 발생시킨다."""
        if isinstance(response, dict):
            msg = response.get("title") or response.get("message") or str()
            if (msg == "Forbidden") or ("권한이 없습니다." in msg) or ("인증이 만료됐습니다." in msg):
                from linkmerce.common.exceptions import UnauthorizedError
                raise UnauthorizedError(msg)
            return (not response.get("code"))
        return False
