from __future__ import annotations

from linkmerce.common.extract import Extractor, LoginHandler

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import Configs
    from requests import Session


def has_accounts(session: Session) -> bool:
    """네이버 로그인 세션이 광고 계정을 가지고 있는지 검증한다."""
    from linkmerce.utils.headers import build_headers
    url = "https://ads.naver.com/?fromLogin=true"
    headers = build_headers(referer="https://nid.naver.com/", metadata="navigate", https=True)
    session.get(url, headers=headers)
    return (dict(session.cookies.items()).get("NAVER_ADS_AVAILABLE_USER") == "true")


def get_accounts(session: Session, page: int = 0, size: int = 10) -> list[dict]:
    """네이버 로그인 세션이 가진 광고 계정 목록을 조회한다."""
    from linkmerce.utils.headers import build_headers
    import json
    url = "https://ads.naver.com/apis/ad-account/v1.1/adAccounts/access"
    params = {"page": page, "size": size}
    headers = build_headers(referer="https://nid.naver.com/", metadata="navigate", https=True)
    with session.get(url, params=params, headers=headers) as response:
        # ["adAccountNo", "naverId", "roleName", "lastAccessTime", "createdAt", "adAccount", "favorite"]
        return json.loads(response.text)["content"]


class SearchAdCenter(Extractor):
    """네이버 광고주센터 데이터를 조회하는 공통 클래스.

    - **URL**: https://ads.naver.com

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을,   
    `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    account_no: int | str
        광고 계정 번호
    customer_id: int | str
        검색광고 고객 ID
    """

    method: str | None = None
    origin = "https://ads.naver.com"
    api_url = "https://ads.naver.com/apis/sa/api"
    path: str | None = None

    def set_configs(self, configs: Configs = dict()):
        try:
            self.set_account(**configs)
        except TypeError:
            raise TypeError("Naver SearchAd requires configs for account_no and customer_id.")

    def set_account(self, account_no: int | str, customer_id: int | str, **configs):
        super().set_configs(dict(account_no=account_no, customer_id=customer_id, **configs))

    @property
    def url(self) -> str:
        return self.concat_path(self.api_url, self.path)

    @property
    def account_no(self) -> int | str:
        return self.get_config("account_no")

    @property
    def customer_id(self) -> int | str:
        return self.get_config("customer_id")

    def post_init(self, **kwargs):
        self.require_cookies()

    def set_request_headers(self, **kwargs):
        """세션 객체 또는 요청 헤더의 쿠키에서 `XSRF-TOKEN` 값을 꺼내 `x-xsrf-token` 헤더로 추가한다."""
        kwargs.update({"x-accept-language": "ko", "x-ad-customer-id": str(self.customer_id)})
        super().set_request_headers(from_cookies={"XSRF-TOKEN": "x-xsrf-token"}, **kwargs)


class NaverAdLogin(LoginHandler):
    """네이버 광고주센터 로그인을 수행하여 쿠키를 발급하는 클래스.

    - **URL**: https://ads.naver.com

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 네이버 로그인 쿠키 문자열을 전달해야 한다.
    """
    origin = "https://ads.naver.com"

    @property
    def xsrf_token(self) -> str:
        return self.get_cookies(to="dict")["XSRF-TOKEN"]

    @LoginHandler.with_session
    def login(self, account_no: str, cookies: str, **kwargs) -> str:
        """네이버 쿠키를 가지고 네이버 광고주센터에 로그인해 `XSRF-TOKEN`을 발급받는다."""
        self.validate_cookies(cookies)
        self.login_action(account_no)
        self.approach_to(account_no)
        return self.get_cookies(to="str")

    def validate_cookies(self, cookies: str):
        self.set_cookies(cookies)
        """네이버 쿠키가 유효하고 광고 계정을 가지고 있는지 검증한다."""
        from linkmerce.common.exceptions import AuthenticationError
        headers = self.build_headers(self.origin, referer="https://nid.naver.com/", metadata="navigate", https=True)
        self.request("GET", self.origin, params={"fromLogin": "true"}, headers=headers)
        if self.get_cookies(to="dict").get("NAVER_ADS_AVAILABLE_USER") != "true":
            raise AuthenticationError("No ad account found or the cookies are invalid.")

    def login_action(self, account_no: int | str):
        from uuid import uuid4
        url = self.origin + "/apis/user/v1.0/users/logged-in"
        referer = f"{self.origin}/manage/ad-accounts/{account_no}/dashboard"
        headers = self.build_headers(self.origin, referer=referer, **{"x-xsrf-token": str(uuid4())})
        self.request("GET", url, headers=headers)

    def approach_to(self, account_no: int | str):
        url = f"{self.origin}/apis/ad-account/v1/adAccounts/{account_no}/approach"
        referer = f"{self.origin}/manage/ad-accounts/{account_no}/dashboard"
        headers = self.build_headers(self.origin,
            contents="form", origin=self.origin, referer=referer, **{"x-xsrf-token": self.xsrf_token})
        self.request("POST", url, headers=headers)


###################################################################
######################## Deprecated v0.6.8 ########################
###################################################################

# """
# 네이버 검색광고 플랫폼이 네이버 광고플랫폼으로 통합되면서
# `SearchAdCenter` 공통 클래스의 동작을 수정.
# 공지: https://ads.naver.com/notice/30459?searchValue=&page=1
# """


# def has_cookies(session: Session, cookies: str = str()) -> bool:
#     """네이버 로그인 쿠키가 유효한지 검증한다."""
#     from linkmerce.utils.headers import build_headers
#     url = "https://gw.searchad.naver.com/auth/local/naver-cookie/exist"
#     origin = "https://searchad.naver.com"
#     referer = f"{origin}/membership/select-account?redirectUrl=https:%2F%2Fmanage.searchad.naver.com"
#     headers = build_headers(cookies=cookies, referer=referer, origin=origin)
#     with session.get(url, headers=headers) as response:
#         return (response.text == "true")


# def has_permission(session: Session, customer_id: int | str, cookies: str = str()) -> bool:
#     """네이버 로그인 쿠키가 계정ID에 대한 접근 권한이 있는지 확인한다."""
#     return bool(whoami(session, customer_id, cookies))


# def whoami(session: Session, customer_id: int | str, cookies: str = str()) -> dict | None:
#     """네이버에서 현재 로그인된 사용자의 검색광고 계정ID를 조회한다."""
#     from linkmerce.utils.headers import build_headers
#     import json
#     url = f"https://gw.searchad.naver.com/auth/local/naver-cookie/ads-accounts/{customer_id}"
#     origin = "https://searchad.naver.com"
#     referer = f"{origin}/membership/select-account?redirectUrl=https%3A//manage.searchad.naver.com"
#     headers = build_headers(cookies=cookies, referer=referer, origin=origin)
#     with session.get(url, headers=headers) as response:
#         body = json.loads(response.text)
#         return body.get("customer") if isinstance(body, dict) else None


# class SearchAdCenter(Extractor):
#     """네이버 검색광고 시스템에서 데이터를 조회하는 공통 클래스.

#     네이버 로그인 쿠키와 `customer_id`를 사용하여 `access_token`을 발급받고 토큰 기반으로 요청한다."""

#     method: str | None = None
#     origin: str = "https://searchad.naver.com"
#     main_url: str = "https://manage.searchad.naver.com"
#     api_url: str = "https://gw.searchad.naver.com/api"
#     auth_url: str = "https://gw.searchad.naver.com/auth"
#     path: str | None = None
#     access_token: str = str()
#     refresh_token: str = str()

#     def set_configs(self, configs: Configs = dict()):
#         try:
#             self.set_customer_id(**configs)
#         except TypeError:
#             raise TypeError("Naver SearchAd requires configs for customer_id to authenticate.")

#     def set_customer_id(self, customer_id: int | str, **configs):
#         super().set_configs(dict(customer_id=customer_id, **configs))

#     @property
#     def url(self) -> str:
#         return self.concat_path(self.api_url, self.path)

#     @property
#     def customer_id(self) -> int | str:
#         return self.get_config("customer_id")

#     def with_token(func):
#         """네이버 로그인 쿠키와 `customer_id`를 사용하여 `access_token`을 발급받는 데코레이터."""
#         @functools.wraps(func)
#         def wrapper(self: SearchAdCenter, *args, **kwargs):
#             self.authenticate()
#             self.authorize()
#             return func(self, *args, **kwargs)
#         return wrapper

#     def authenticate(self):
#         """네이버 로그인 쿠키가 올바른지 검증한다."""
#         cookies = self.get_request_headers().get("cookie", str())
#         if not has_permission(self.get_session(), self.customer_id, cookies):
#             from linkmerce.common.exceptions import AuthenticationError
#             raise AuthenticationError("You don't have permission to access this account.")

#     def authorize(self):
#         """네이버 로그인 쿠키와 `customer_id`를 사용하여 `access_token`을 발급받는다."""
#         from urllib.parse import quote
#         url = self.auth_url + "/local/naver-cookie"
#         referer = f"{self.origin}/membership/select-account?redirectUrl={quote(self.main_url)}"
#         headers = dict(self.get_request_headers(), referer=referer, origin=self.origin)
#         response = self.get_session().post(url, headers=headers, params={"customerId": self.customer_id}).json()
#         self.set_token(**response)

#     def refresh(self, referer: str = str()):
#         """5분 유효기간이 있는 `access_token`이 만료되면 새로고침한다."""
#         from urllib.parse import quote
#         url = self.auth_url + "/local/extend"
#         data = {"refreshToken": self.refresh_token}
#         referer = f"{self.origin}/membership/select-account?redirectUrl={quote(referer or self.main_url)}"
#         headers = dict(self.get_request_headers(), referer=referer, origin=self.main_url)
#         response = self.get_session().put(url, json=data, headers=headers).json()
#         self.set_token(**response)

#     def set_token(self, token: str, refreshToken: str, **kwargs):
#         self.access_token = token
#         self.refresh_token = refreshToken

#     def get_authorization(self) -> str:
#         return "Bearer " + self.access_token

#     def is_valid_response(self, response: JsonObject) -> bool:
#         """요청 중 토큰 인증 오류가 발생하면 `UnauthorizedError`를 발생시킨다."""
#         if isinstance(response, dict):
#             msg = response.get("title") or response.get("message") or str()
#             if (msg == "Forbidden") or ("권한이 없습니다." in msg) or ("인증이 만료됐습니다." in msg):
#                 from linkmerce.common.exceptions import UnauthorizedError
#                 raise UnauthorizedError(msg)
#             return (not response.get("code"))
#         return False
