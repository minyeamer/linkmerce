from __future__ import annotations

from linkmerce.common.extract import Extractor
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import Variables


class SearchAdManager(Extractor):
    origin: str = "https://searchad.naver.com"
    main_url: str = "https://manage.searchad.naver.com"
    api_url: str = "https://gw.searchad.naver.com/api"
    auth_url: str = "https://gw.searchad.naver.com/auth"
    path: str
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
        return self.api_url + ('/' * (not self.path.startswith('/'))) + self.path

    @property
    def customer_id(self) -> str | int:
        return self.get_variable("customer_id")

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
        headers = dict(self.get_request_headers(), referer=redirect_url, origin=self.origin)
        response = self.get_session().get(url, headers=headers).text
        if response.strip() != "true":
            from linkmerce.common.exceptions import AuthenticationError
            raise AuthenticationError("Authentication failed: cookies are invalid.")

    def authorize(self):
        from urllib.parse import quote
        url = self.auth_url + "/local/naver-cookie"
        redirect_url = f"{self.origin}/naver?returnUrl={quote(self.main_url + '/front')}&returnMethod=get"
        headers = dict(self.get_request_headers(), referer=redirect_url, origin=self.origin, **{"content-type":"text/plain"})
        response = self.get_session().post(url, headers=headers).json()
        self.set_token(**response)

    def refresh(self, referer: str = str()):
        url = self.auth_url + "/local/extend"
        params = dict(refreshToken=self.refresh_token)
        referer = referer or (self.main_url + "/front")
        headers = dict(self.get_request_headers(), referer=referer, origin=self.main_url)
        response = self.get_session().put(url, params=params, headers=headers).json()
        self.set_token(**response)

    def set_token(self, token: str, refreshToken: str, **kwargs):
        self.access_token = token
        self.refresh_token = refreshToken

    def link_customer(self, referer: str = str()):
        url = f"{self.api_url}/customer-links/{self.customer_id}/token"
        referer = referer or (self.main_url + "/front")
        headers = dict(self.get_request_headers(), authorization=self.get_authorization(), referer=referer, origin=self.main_url)
        self.access_token = self.get_session().get(url, headers=headers).json()["token"]

    def get_authorization(self) -> str:
        return "Bearer " + self.access_token
