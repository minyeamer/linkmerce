from __future__ import annotations

from linkmerce.collect import Collector

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Callable, Hashable, IO, Literal, TypeVar
    from linkmerce.types import JsonSerialize
    _KT = TypeVar("_KT", Hashable)
    _VT = TypeVar("_VT", Any)

    from requests import Session
    from aiohttp.client import ClientSession


class NaverOpenAPI(Collector):
    origin: str = "https://openapi.naver.com"
    version: str = "v1"
    path: str

    def __init__(
            self,
            client_id: str,
            client_secret: str,
            session: Session | ClientSession | None = None,
            params: dict | list[tuple] | bytes | None = None,
            body: dict | dict | list[tuple] | bytes | IO | JsonSerialize | None = None,
            headers: dict[_KT,_VT] = dict(),
            parser: Literal["default"] | Callable | None = "default",
        ):
        self.set_api_key(client_id, client_secret)
        super().__init__(session, params, body, headers, parser)

    @property
    def url(self) -> str:
        return self.origin + '/' + self.version + ('/' * (not self.path.startswith('/'))) + self.path

    def set_api_key(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret

    def set_request_headers(self, **kwargs):
        super().set_request_headers(headers={
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret,
            "Content-Type": "application/json"
        })
