from __future__ import annotations
from abc import ABCMeta, abstractmethod
import functools

from typing import Dict, List, Union, TYPE_CHECKING
JsonObject = Union[Dict, List]

if TYPE_CHECKING:
    from typing import Any, IO, Literal, Sequence, Tuple
    from requests import Session, Response
    from requests.cookies import RequestsCookieJar
    from aiohttp.client import ClientSession, ClientResponse
    from aiohttp.typedefs import LooseCookies
    from bs4 import BeautifulSoup
    from pandas import DataFrame
    JsonSerialize = Union[Dict, List, bytes, IO]


GET = "GET"
POST = "POST"
OPTIONS = "OPTIONS"
HEAD = "HEAD"
PUT = "PUT"
PATCH = "PATCH"
DELETE = "DELETE"


class BaseSessionClient(metaclass=ABCMeta):
    method = GET
    url = str()

    def __init__(self, session: Session | ClientSession | None = None, **kwargs):
        self.init_session(session)
        self.init_constant(**kwargs)

    def init_session(self, session: Session | ClientSession | None = None):
        self.session = session

    def init_constant(self, **kwargs):
        self.set_request_headers(**kwargs)

    @abstractmethod
    def request(self, **kwargs):
        raise NotImplementedError("The request method must be implemented.")

    def set_request_headers(self,
            authority: str = str(),
            accept: str = "*/*",
            encoding: str = "gzip, deflate, br",
            language: Literal["ko","en"] | str = "ko",
            connection: str = "keep-alive",
            contents: Literal["form", "javascript", "json", "text", "multipart"] | str | Dict = str(),
            cookies: str = str(),
            host: str = str(),
            origin: str = str(),
            priority: str = "u=0, i",
            referer: str = str(),
            client: str = str(),
            mobile: bool = False,
            platform: str = str(),
            metadata: Literal["cors", "navigate"] | Dict[str,str] = "navigate",
            https: bool = False,
            user_agent: str = str(),
            ajax: bool = False,
        **kwargs):
        from utils.headers import make_headers
        kwargs = {key: value for key, value in locals().items() if key not in ("self","kwargs","make_headers")}
        self.headers = make_headers(**kwargs)

    def get_request_headers(self, **kwargs) -> Dict[str,str]:
        return dict(self.headers, **kwargs) if kwargs else self.headers


class RequestsSessionClient(BaseSessionClient):
    def request(self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | RequestsCookieJar = None,
        **kwargs) -> Response:
        return self.session.request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs)

    def request_status(self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | RequestsCookieJar = None,
        **kwargs) -> int:
        with self.session.request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.status_code

    def request_content(self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | RequestsCookieJar = None,
        **kwargs) -> bytes:
        with self.session.request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.content

    def request_text(self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | RequestsCookieJar = None,
        **kwargs) -> str:
        with self.session.request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.text

    def request_json(self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | RequestsCookieJar = None,
        **kwargs) -> JsonObject:
        with self.session.request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.json()

    def request_headers(self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | RequestsCookieJar = None,
        **kwargs) -> Dict[str,str]:
        with self.session.request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.headers

    def request_html(self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | RequestsCookieJar = None,
            features: str | Sequence[str] | None = "html.parser",
        **kwargs) -> BeautifulSoup:
        from bs4 import BeautifulSoup
        response = self.request_text(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs)
        return BeautifulSoup(response, features)

    def request_table(self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | RequestsCookieJar = None,
            content_type: Literal["excel", "csv", "html", "xml"] | Sequence = "xlsx",
            table_options: Dict = dict(),
        **kwargs) -> DataFrame:
        from utils.pandas import read_table
        response = self.request_content(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs)
        return read_table(response, table_format=content_type, **table_options)


class AiohttpSessionClient(BaseSessionClient):
    def request(self, *args, **kwargs):
        raise NotImplementedError("This feature does not support synchronous requests. Please use the request_async method instead.")

    async def request_async(self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | LooseCookies = None,
        **kwargs) -> ClientResponse:
        return await self.session.request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs)

    async def request_async_status(self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | LooseCookies = None,
        **kwargs) -> int:
        async with self.session.request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.status

    async def request_async_content(self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | LooseCookies = None,
        **kwargs) -> bytes:
        async with self.session.request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.content

    async def request_async_text(self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | LooseCookies = None,
        **kwargs) -> str:
        async with self.session.request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return await response.text()

    async def request_async_json(self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | LooseCookies = None,
        **kwargs) -> JsonObject:
        async with self.session.request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return await response.json()

    async def request_async_headers(self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | LooseCookies = None,
        **kwargs) -> Dict[str,str]:
        async with self.session.request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.headers

    async def request_async_html(self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | LooseCookies = None,
            features: str | Sequence[str] | None = "html.parser",
        **kwargs) -> BeautifulSoup:
        from bs4 import BeautifulSoup
        response = await self.request_async_text(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs)
        return BeautifulSoup(response, features)

    async def request_async_table(self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | LooseCookies = None,
            content_type: Literal["excel", "csv", "html", "xml"] | Sequence = "xlsx",
            table_options: Dict = dict(),
        **kwargs) -> DataFrame:
        from utils.pandas import read_table
        response = await self.request_async_content(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs)
        return read_table(response, table_format=content_type, **table_options)


class Collector(RequestsSessionClient, AiohttpSessionClient, metaclass=ABCMeta):
    @abstractmethod
    def collect(self, *args, **kwargs) -> Any:
        raise NotImplementedError("This feature does not support synchronous requests. Please use the collect_async method instead.")

    def with_session(func):
        @functools.wraps(func)
        def wrapper(self: Collector, *args, init_session=False, **context):
            if init_session and (self.session is None):
                import requests
                with requests.Session() as session:
                    self.init_session(session)
                    return func(self, *args, **context)
            else:
                return func(self, *args, **context)
        return wrapper

    async def collect_async(self, *args, **kwargs):
        raise NotImplementedError("This feature does not support asynchronous requests. Please use the collect method instead.")

    def with_client_session(func):
        @functools.wraps(func)
        async def wrapper(self: Collector, *args, init_session=False, **context):
            if init_session and (self.session is None):
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    self.init_session(session)
                    return await func(self, *args, **context)
            else:
                return await func(self, *args, **context)
        return wrapper

    def get_request_message(self, *args, **kwargs) -> Dict:
        raise NotImplementedError("The get_request_message is not implemented.")

    def parse(self, response: Any, *args, **kwargs) -> Any:
        raise NotImplementedError("The parse method is not implemented.")
