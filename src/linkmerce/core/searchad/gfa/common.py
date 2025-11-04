from __future__ import annotations

from linkmerce.common.extract import Extractor
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import Variables, JsonObject
    from requests import Session


def logged_in(session: Session, cookies: str = str()) -> bool:
    return bool(whoami(session, cookies))


def whoami(session: Session, cookies: str = str()) -> str | None:
    from linkmerce.utils.headers import build_headers
    import json
    url = "https://gfa.naver.com/apis/user/v1.0/users/logged-in"
    headers = build_headers(cookies=cookies, referer="https://ads.naver.com/?fromLogin=true")
    with session.get(url, headers=headers) as response:
        body = json.loads(response.text)
        return body.get("naverId") if isinstance(body, dict) else None


class SearchAdGFA(Extractor):
    method: str | None = None
    origin: str = "https://gfa.naver.com"
    path: str | None = None

    def set_variables(self, variables: Variables = dict()):
        try:
            self.set_account_no(**variables)
        except TypeError:
            raise TypeError("Naver SearchAd requires variables for account_no to authenticate.")

    def set_account_no(self, account_no: int | str, **variables):
        super().set_variables(dict(account_no=account_no, **variables))

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.path)

    @property
    def account_no(self) -> int | str:
        return self.get_variable("account_no")

    @property
    def token(self) -> str:
        return self.get_session().cookies.get("XSRF-TOKEN")

    def with_token(func):
        @functools.wraps(func)
        def wrapper(self: SearchAdGFA, *args, **kwargs):
            self.authenticate()
            self.authorize()
            self.set_request_headers(cookies=self.get_cookies())
            return func(self, *args, **kwargs)
        return wrapper

    def authenticate(self):
        cookies = self.get_request_headers(with_token=False).get("cookie", str())
        if not logged_in(self.get_session(), cookies):
            from linkmerce.common.exceptions import AuthenticationError
            raise AuthenticationError("You don't have valid cookies.")
        self.set_cookies(cookies)

    def authorize(self):
        url = self.origin + "/apis/gfa/anonymous/v1/regulations/downtime.notice/entire"
        referer = self.origin + f"/adAccount/accounts/{self.account_no}"
        headers = dict(self.get_request_headers(with_token=False), referer=referer)
        self.get_session().post(url, headers=headers)

    def build_request_headers(self, with_token: bool = True, **kwargs: str) -> dict[str,str]:
        return self.get_request_headers(with_token)

    def get_request_headers(self, with_token: bool = True) -> dict[str,str]:
        if with_token and self.token:
            cookies = {"cookie": self.get_cookies(), "x-xsrf-token": self.token}
            return dict(super().get_request_headers(), **cookies)
        else:
            return super().get_request_headers()

    def is_valid_response(self, response: JsonObject) -> bool:
        if isinstance(response, dict):
            if response.get("error") == "Unauthorized":
                from linkmerce.common.exceptions import UnauthorizedError
                raise UnauthorizedError(response.get("message") or str())
            return (not response.get("error"))
        return False

    @Extractor.cookies_required
    def set_request_headers(self, **kwargs: str):
        super().set_request_headers(accessadaccountno=str(self.account_no), **kwargs)
