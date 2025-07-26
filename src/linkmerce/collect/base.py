from __future__ import annotations

from abc import ABCMeta, abstractmethod
import functools

from typing import Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Dict, IO, List, Literal, Sequence, Tuple
    from linkmerce.types import JsonObject, JsonSerialize
    from requests import Session, Response
    from requests.cookies import RequestsCookieJar
    from aiohttp.client import ClientSession, ClientResponse
    from aiohttp.typedefs import LooseCookies
    from bs4 import BeautifulSoup
    from pandas import DataFrame


class BaseSessionClient(metaclass=ABCMeta):
    method: str = "GET"
    url: str = str()

    def __init__(self, session: Session | ClientSession | None = None, headers: Dict = dict()):
        self.set_session(session)
        self.set_request_headers(**headers)

    @abstractmethod
    def request(self, **kwargs):
        raise NotImplementedError("The 'request' method must be implemented.")

    def get_session(self) -> Session | ClientSession:
        return self.__session

    def set_session(self, session: Session | ClientSession | None = None):
        self.__session = session

    def get_request_params(self, **kwargs) -> Dict | List[Tuple] | bytes:
        raise NotImplementedError("The 'get_request_params' method is not implemented.")

    def set_request_params(self, **kwargs):
        raise NotImplementedError("The 'set_request_params' method is not implemented.")

    def get_request_body(self, **kwargs) -> Dict | List[Tuple] | bytes | IO | JsonSerialize:
        raise NotImplementedError("The 'get_request_body' method is not implemented.")

    def set_request_body(self, **kwargs):
        raise NotImplementedError("The 'set_request_body' method is not implemented.")

    def get_request_headers(self, **kwargs) -> Dict[str,str]:
        return dict(self.__headers, **kwargs) if kwargs else self.__headers

    def set_request_headers(
            self,
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
            **kwargs
        ):
        from linkmerce.utils.headers import make_headers
        self.__headers = make_headers(
            authority, accept, encoding, language, connection, contents, cookies, host, origin, priority,
            referer, client, mobile, platform, metadata, https, user_agent, ajax, **kwargs)

    def cookies_required(func):
        @functools.wraps(func)
        def wrapper(self: Collector, *args, **kwargs):
            if "cookies" not in kwargs:
                import warnings
                warnings.warn("Cookies will be required for upcoming requests.")
            return func(self, *args, **kwargs)
        return wrapper


class RequestSessionClient(BaseSessionClient):
    def request(
            self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | RequestsCookieJar = None,
            **kwargs
        ) -> Response:
        return self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs)

    def request_status(
            self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | RequestsCookieJar = None,
            **kwargs
        ) -> int:
        with self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.status_code

    def request_content(
            self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | RequestsCookieJar = None,
            **kwargs
        ) -> bytes:
        with self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.content

    def request_text(
            self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | RequestsCookieJar = None,
            **kwargs
        ) -> str:
        with self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.text

    def request_json(
            self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | RequestsCookieJar = None,
            **kwargs
        ) -> JsonObject:
        with self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.json()

    def request_headers(
            self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | RequestsCookieJar = None,
            **kwargs
        ) -> Dict[str,str]:
        with self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.headers

    def request_html(
            self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | RequestsCookieJar = None,
            features: str | Sequence[str] | None = "html.parser",
            **kwargs
        ) -> BeautifulSoup:
        from bs4 import BeautifulSoup
        response = self.request_text(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs)
        return BeautifulSoup(response, features)

    def request_table(
            self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | RequestsCookieJar = None,
            content_type: Literal["excel", "csv", "html", "xml"] | Sequence = "xlsx",
            table_options: Dict = dict(),
            **kwargs
        ) -> DataFrame:
        from linkmerce.src.linkmerce.extensions.pandas import read_table
        response = self.request_content(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs)
        return read_table(response, table_format=content_type, **table_options)

    def with_session(func):
        @functools.wraps(func)
        def wrapper(self: Collector, *args, init_session=False, **kwargs):
            if init_session and (self.get_session() is None):
                return self._run_with_session(func, *args, **kwargs)
            else:
                return func(self, *args, **kwargs)
        return wrapper

    def _run_with_session(self, func: Callable, *args, **kwargs) -> Any:
        import requests
        try:
            with requests.Session() as session:
                self.set_session(session)
                return func(self, *args, **kwargs)
        finally:
            self.set_session(None)


class AiohttpSessionClient(BaseSessionClient):
    def request(self, *args, **kwargs):
        raise NotImplementedError("This feature does not support synchronous requests. Please use the request_async method instead.")

    async def request_async(
            self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | LooseCookies = None,
            **kwargs
        ) -> ClientResponse:
        return await self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs)

    async def request_async_status(
            self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | LooseCookies = None,
            **kwargs
        ) -> int:
        async with self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.status

    async def request_async_content(
            self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | LooseCookies = None,
            **kwargs
        ) -> bytes:
        async with self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.content

    async def request_async_text(
            self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | LooseCookies = None,
            **kwargs
        ) -> str:
        async with self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return await response.text()

    async def request_async_json(
            self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | LooseCookies = None,
            **kwargs
        ) -> JsonObject:
        async with self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return await response.json()

    async def request_async_headers(
            self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | LooseCookies = None,
            **kwargs
        ) -> Dict[str,str]:
        async with self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.headers

    async def request_async_html(
            self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | LooseCookies = None,
            features: str | Sequence[str] | None = "html.parser",
            **kwargs
        ) -> BeautifulSoup:
        from bs4 import BeautifulSoup
        response = await self.request_async_text(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs)
        return BeautifulSoup(response, features)

    async def request_async_table(
            self,
            method: str,
            url: str,
            params: Dict | List[Tuple] | bytes | None = None,
            data: Dict | List[Tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: Dict[str,str] = None,
            cookies: Dict | LooseCookies = None,
            content_type: Literal["excel", "csv", "html", "xml"] | Sequence = "xlsx",
            table_options: Dict = dict(),
            **kwargs
        ) -> DataFrame:
        from linkmerce.src.linkmerce.extensions.pandas import read_table
        response = await self.request_async_content(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs)
        return read_table(response, table_format=content_type, **table_options)

    def with_client_session(func):
        @functools.wraps(func)
        async def wrapper(self: Collector, *args, init_session=False, **kwargs):
            if init_session and (self.get_session() is None):
                return await self._run_with_client_session(func, *args, **kwargs)
            else:
                return await func(self, *args, **kwargs)
        return wrapper

    async def _run_with_client_session(self, func: Callable, *args, **kwargs) -> Any:
        import aiohttp
        try:
            async with aiohttp.ClientSession() as session:
                self.set_session(session)
                return await func(self, *args, **kwargs)
        finally:
            self.set_session(None)


class Collector(RequestSessionClient, AiohttpSessionClient, metaclass=ABCMeta):
    @abstractmethod
    def collect(self, *args, **kwargs) -> Any:
        raise NotImplementedError("This feature does not support synchronous requests. Please use the collect_async method instead.")

    async def collect_async(self, *args, **kwargs):
        raise NotImplementedError("This feature does not support asynchronous requests. Please use the collect method instead.")

    def build_request(self, *args, **kwargs) -> Dict:
        return dict(method=self.method, url=self.url)

    def parse(self, response: Any, parser: Callable | None = None, *args, **kwargs) -> Any:
        if parser is None:
            return response
        elif isinstance(parser, Callable):
            return parser(response, *args, **kwargs)
        else:
            raise ValueError("Unable to recognize the parser.")

    def import_parser(self, name: str, module_name: Literal["parse"] | str = "parse") -> Callable:
        from importlib import import_module
        if module_name == "parse":
            from inspect import getmodule
            module_name = getmodule(self.collect).__name__.replace("collect", "parse", 1)
        module = import_module(module_name, __name__)
        return getattr(module, name)
