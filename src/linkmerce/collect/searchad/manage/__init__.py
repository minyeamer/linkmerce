from linkmerce.collect import Collector

from typing import TypedDict


class Account(TypedDict):
    customer_id: int


class SearchAdManager(Collector):
    origin: str = "https://searchad.naver.com"
    main_url: str = "https://manage.searchad.naver.com"
    api_url: str = "https://gw.searchad.naver.com/api"
    auth_url: str = "https://gw.searchad.naver.com/auth"
    path: str
    access_token: str = str()
    refresh_token: str = str()

    @property
    def url(self) -> str:
        return self.api_url + ('/' * (not self.path.startswith('/'))) + self.path

    def __post_init__(self):
        self.validate()
        self.authorize()
        self.link_customer()

    def set_account(self, account: Account, **kwargs):
        super().set_account(account=account)

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
        url = f"{self.api_url}/customer-links/{self.get_account('customer_id')}/token"
        referer = referer or (self.main_url + "/front")
        headers = super().get_request_headers(authorization=self.get_authorization(), referer=referer, origin=self.main_url)
        self.access_token = self.request_json("GET", url, headers=headers)["token"]

    def get_authorization(self) -> str:
        return "Bearer " + self.access_token
