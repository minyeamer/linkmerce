from __future__ import annotations

from linkmerce.common.extract import Extractor

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import Configs


class SearchAdGfa(Extractor):
    """네이버 성과형 디스플레이 광고 데이터를 조회하는 공통 클래스.

    - **URL**: https://ads.naver.com

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을,   
    `configs` 인자로 아래 설정값을 반드시 전달해야 한다.

    account_no: int | str
        광고 계정 번호
    """

    method: str | None = None
    origin = "https://ads.naver.com"
    version: str = "v1"
    path: str | None = None

    def set_configs(self, configs: Configs = dict()):
        try:
            self.set_account(**configs)
        except TypeError:
            raise TypeError("Naver Ad requires configs for account_no.")

    def set_account(self, account_no: int | str, **configs):
        super().set_configs(dict(account_no=account_no, **configs))

    @property
    def api_url(self) -> str:
        return f"{self.origin}/apis/gfa/{self.version}/adAccounts/{self.account_no}"

    @property
    def url(self) -> str:
        return self.concat_path(self.api_url, self.path)

    @property
    def account_no(self) -> int | str:
        return self.get_config("account_no")

    def post_init(self, **kwargs):
        self.require_cookies()

    def set_request_headers(self, **kwargs):
        """세션 객체 또는 요청 헤더의 쿠키에서 `XSRF-TOKEN` 값을 꺼내 `x-xsrf-token` 헤더로 추가한다."""
        super().set_request_headers(from_cookies={"XSRF-TOKEN": "x-xsrf-token"}, **kwargs)


###################################################################
######################## Deprecated v0.6.8 ########################
###################################################################

# """
# 네이버 성과형 디스플레이 광고가 네이버 광고플랫폼으로 통합되면서
# `SearchAdGfa` 공통 클래스를 `SearchAdCenter` 공통 클래스로 대체.
# 공지: https://ads.naver.com/notice/30459?searchValue=&page=1
# """


# def logged_in(session: Session, cookies: str = str()) -> bool:
#     """네이버 로그인 쿠키가 유효한지 검증한다."""
#     return bool(whoami(session, cookies))


# def whoami(session: Session, cookies: str = str()) -> str | None:
#     """네이버에서 현재 로그인된 사용자의 성과형 디스플레이 광고 계정ID를 조회한다."""
#     from linkmerce.utils.headers import build_headers
#     import json
#     url = "https://gfa.naver.com/apis/user/v1.0/users/logged-in"
#     headers = build_headers(cookies=cookies, referer="https://ads.naver.com/?fromLogin=true")
#     with session.get(url, headers=headers) as response:
#         body = json.loads(response.text)
#         return body.get("naverId") if isinstance(body, dict) else None


# class SearchAdGfa(Extractor):
#     """네이버 성과형 디스플레이 광고에서 데이터를 조회하는 공통 클래스.

#     네이버 로그인 쿠키와 `account_no`를 사용하여 `XSRF-TOKEN`을 발급받고 토큰 기반으로 요청한다."""

#     method: str | None = None
#     origin: str = "https://gfa.naver.com"
#     path: str | None = None

#     def set_configs(self, configs: Configs = dict()):
#         try:
#             self.set_account_no(**configs)
#         except TypeError:
#             raise TypeError("Naver SearchAd requires configs for account_no to authenticate.")

#     def set_account_no(self, account_no: int | str, **configs):
#         super().set_configs(dict(account_no=account_no, **configs))

#     @property
#     def url(self) -> str:
#         return self.concat_path(self.origin, self.path)

#     @property
#     def account_no(self) -> int | str:
#         return self.get_config("account_no")

#     @property
#     def token(self) -> str:
#         return self.get_session().cookies.get("XSRF-TOKEN")

#     def with_token(func):
#         """네이버 로그인 쿠키와 `account_no`를 사용하여 `XSRF-TOKEN`을 발급받는 데코레이터."""
#         @functools.wraps(func)
#         def wrapper(self: SearchAdGfa, *args, **kwargs):
#             self.authenticate()
#             self.authorize()
#             self.set_request_headers(cookies=self.get_cookies())
#             return func(self, *args, **kwargs)
#         return wrapper

#     def authenticate(self):
#         """네이버 로그인 쿠키가 올바른지 검증한다."""
#         cookies = self.get_request_headers(with_token=False).get("cookie", str())
#         if not logged_in(self.get_session(), cookies):
#             from linkmerce.common.exceptions import AuthenticationError
#             raise AuthenticationError("You don't have valid cookies.")
#         self.set_cookies(cookies)

#     def authorize(self):
#         """네이버 로그인 쿠키와 `account_no`를 사용하여 `XSRF-TOKEN`을 발급받는다."""
#         url = self.origin + "/apis/gfa/anonymous/v1/regulations/downtime.notice/entire"
#         referer = self.origin + f"/adAccount/accounts/{self.account_no}"
#         headers = dict(self.get_request_headers(with_token=False), referer=referer)
#         self.get_session().post(url, headers=headers)

#     def build_request_headers(self, with_token: bool = True, **kwargs: str) -> dict[str, str]:
#         return self.get_request_headers(with_token)

#     def get_request_headers(self, with_token: bool = True) -> dict[str, str]:
#         if with_token and self.token:
#             cookies = {"cookie": self.get_cookies(), "x-xsrf-token": self.token}
#             return dict(super().get_request_headers(), **cookies)
#         else:
#             return super().get_request_headers()

#     def is_valid_response(self, response: JsonObject) -> bool:
#         """요청 중 토큰 인증 오류가 발생하면 `UnauthorizedError`를 발생시킨다."""
#         if isinstance(response, dict):
#             if response.get("error") == "Unauthorized":
#                 from linkmerce.common.exceptions import UnauthorizedError
#                 raise UnauthorizedError(response.get("message") or str())
#             return (not response.get("error"))
#         return False

#     def set_request_headers(self, **kwargs: str):
#         super().set_request_headers(accessadaccountno=str(self.account_no), **kwargs)
