from linkmerce.collect import Collector

from typing import TypedDict


class Application(TypedDict):
    client_id: str
    client_secret: str


class NaverOpenAPI(Collector):
    origin: str = "https://openapi.naver.com"
    version: str = "v1"
    path: str

    @property
    def url(self) -> str:
        return self.origin + '/' + self.version + ('/' * (not self.path.startswith('/'))) + self.path

    def set_account(self, account: Application, **kwargs):
        super().set_account(account=account)

    def set_request_headers(self, **kwargs):
        super().set_request_headers(headers={
            "X-Naver-Client-Id": self.get_account("client_id"),
            "X-Naver-Client-Secret": self.get_account("client_secret"),
            "Content-Type": "application/json"
        })
