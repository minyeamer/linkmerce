from __future__ import annotations

from abc import ABCMeta, abstractmethod
import functools

from typing import Callable, Dict, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Hashable, IO, List, Literal, Tuple, TypeVar
    from linkmerce.types import JsonObject, JsonSerialize
    _KT = TypeVar("_KT", Hashable)
    _VT = TypeVar("_VT", Any)
    _SKIPPED = TypeVar("_SKIPPED", None)

    from requests import Session, Response
    from requests.cookies import RequestsCookieJar
    from aiohttp.client import ClientSession, ClientResponse
    from aiohttp.typedefs import LooseCookies

    from bs4 import BeautifulSoup
    # from pandas import DataFrame
    DATAFRAME = TypeVar("DATAFRAME")


class BaseSessionClient(metaclass=ABCMeta):
    def __init__(
            self,
            session: Session | ClientSession | None = None,
            account: Dict[_KT,_VT] = dict(),
            params: Dict | List[Tuple] | bytes | None = None,
            body: Dict | Dict | List[Tuple] | bytes | IO | JsonSerialize | None = None,
            headers: Dict[_KT,_VT] = dict(),
        ):
        self.set_session(session)
        self.set_account(account)
        self.set_request_params(params)
        self.set_request_body(body)
        self.set_request_headers(**headers)

    @abstractmethod
    def request(self, **kwargs):
        raise NotImplementedError("The 'request' method must be implemented.")

    def get_session(self) -> Session | ClientSession:
        return self.__session

    def set_session(self, session: Session | ClientSession | None = None):
        self.__session = session

    def get_account(self, key: _KT | None = None, **kwargs) -> _VT | Dict:
        return self.__account[key] if key is not None else self.__account

    def set_account(self, account: Dict[_KT,_VT] = dict(), **kwargs):
        self.__account = account

    def get_request_params(self, **kwargs) -> Dict | List[Tuple] | bytes:
        return self.__params

    def set_request_params(self, params: Dict | List[Tuple] | bytes | None = None, **kwargs):
        self.__params = params

    def get_request_body(self, **kwargs) -> Dict | List[Tuple] | bytes | IO | JsonSerialize:
        return self.__body

    def set_request_body(self, body: Dict | Dict | List[Tuple] | bytes | IO | JsonSerialize | None = None, **kwargs):
        self.__body = body

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
            headers: Dict | None = None,
            **kwargs
        ):
        if headers is None:
            from linkmerce.utils.headers import make_headers
            self.__headers = make_headers(
                authority, accept, encoding, language, connection, contents, cookies, host, origin, priority,
                referer, client, mobile, platform, metadata, https, user_agent, ajax, **kwargs)
        else:
            self.__headers = headers

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
        ) -> DATAFRAME:
        from linkmerce.src.linkmerce.extensions.pandas import read_table
        response = self.request_content(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs)
        return read_table(response, table_format=content_type, **table_options)

    def with_session(func):
        @functools.wraps(func)
        def wrapper(self: RequestSessionClient, *args, init_session: bool = False, **kwargs):
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
        ) -> DATAFRAME:
        from linkmerce.src.linkmerce.extensions.pandas import read_table
        response = await self.request_async_content(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs)
        return read_table(response, table_format=content_type, **table_options)

    def async_with_session(func):
        @functools.wraps(func)
        async def wrapper(self: AiohttpSessionClient, *args, init_session: bool = False, **kwargs):
            if init_session and (self.get_session() is None):
                return await self._run_async_with_session(func, *args, **kwargs)
            else:
                return await func(self, *args, **kwargs)
        return wrapper

    async def _run_async_with_session(self, func: Callable, *args, **kwargs) -> Any:
        import aiohttp
        try:
            async with aiohttp.ClientSession() as session:
                self.set_session(session)
                return await func(self, *args, **kwargs)
        finally:
            self.set_session(None)

    def async_with_semaphore(func):
        @functools.wraps(func)
        async def wrapper(self: AiohttpSessionClient, *args, max_tasks: int | None = None, **kwargs):
            if isinstance(max_tasks, int):
                import asyncio
                async with asyncio.Semaphore(max_tasks) as semaphore:
                    return await func(self, *args, **kwargs)
            else:
                return await func(self, *args, **kwargs)
        return wrapper


class SessionClient(RequestSessionClient, AiohttpSessionClient):
    ...


class ParserClient(metaclass=ABCMeta):
    def __init__(self, parser: Literal["default"] | Callable | None = "default"):
        self.set_parser(parser)

    def parse(self, response: Any, parser: Literal["self"] | Callable | None = "self", *args, **kwargs) -> Any:
        return parser(response, *args, **kwargs) if (parser := self.get_parser(parser)) is not None else response

    def get_parser(self, parser: Literal["self"] | Callable | None = "self") -> Callable:
        if isinstance(parser, str):
            parser = self.__parser if parser == "self" else self.import_parser(parser)
        return self._type_check_parser(parser)

    def set_parser(self, parser: Literal["default"] | Callable | None = "default"):
        if isinstance(parser, str):
            parser = self.import_parser(self.__class__.__name__ if parser == "default" else parser)
        self.__parser = self._type_check_parser(parser)

    def _type_check_parser(self, parser: Callable | None = None) -> Callable:
        if isinstance(parser, Callable) or (parser is None):
            return parser
        else:
            raise ValueError("Unable to recognize the parser.")

    def import_parser(self, name: str, module_name: Literal["parse"] | str = "parse") -> Callable:
        from importlib import import_module
        if module_name == "parse":
            from inspect import getmodule
            module_name = getmodule(getattr(self, "collect")).__name__.replace("collect", "parse", 1)
        module = import_module(module_name, __name__)
        return getattr(module, name)


class Collector(SessionClient, ParserClient, metaclass=ABCMeta):
    method: str
    url: str

    def __init__(
            self,
            session: Session | ClientSession | None = None,
            account: Dict[_KT,_VT] = dict(),
            params: Dict | List[Tuple] | bytes | None = None,
            body: Dict | Dict | List[Tuple] | bytes | IO | JsonSerialize | None = None,
            headers: Dict[_KT,_VT] = dict(),
            parser: Literal["default"] | Callable | None = "default",
        ):
        SessionClient.__init__(self, session, account, params, body, headers)
        ParserClient.__init__(self, parser)
        self.__post_init__()

    def __post_init__(self):
        ...

    @abstractmethod
    def collect(self, **kwargs) -> Any:
        raise NotImplementedError("This feature does not support synchronous requests. Please use the collect_async method instead.")

    async def collect_async(self, **kwargs):
        raise NotImplementedError("This feature does not support asynchronous requests. Please use the collect method instead.")

    def build_request(
            self,
            params: Dict | None = None,
            data: Dict | None = None,
            json: Dict | None = None,
            headers: Dict | None = dict(),
            **kwargs
        ) -> Dict:
        message = dict(method=self.method, url=self.url)
        keys = ["params", "data", "json", "headers"]
        attrs = ["params", "body", "body", "headers"]
        for key, attr, param in zip(keys, attrs, [params,data,json,headers]):
            if isinstance(param, Dict):
                message[key] = getattr(self, f"get_request_{attr}")(**param)
        return message


class PaginationMixin:
    max_page_size: int
    page_start: int = 1
    total_count: int | None = None

    def count_total(self, response: Any) -> int:
        raise NotImplementedError("To use the 'PaginationMixin's methods, the 'count_total' method must be implemented.")

    def parse(self, *args, **kwargs) -> Any:
        raise NotImplementedError("To use the 'PaginationMixin's methods, the 'parse' method must be implemented.")

    def collect(self, **kwargs) -> Any:
        raise NotImplementedError("To use the 'collect_all' method, the 'collect' method must be implemented.")

    async def collect_async(self, **kwargs):
        raise NotImplementedError("To use the 'collect_async_all' method, the 'collect_async' method must be implemented.")

    @Collector.with_session
    def collect_all(
            self,
            concat_response: Literal["auto"] | bool = "auto",
            tqdm_optinos: Dict = dict(disable=True),
            *,
            page: _SKIPPED = None,
            page_size: _SKIPPED = None,
            **kwargs
        ) -> Any:
        response = self.collect(page=self.page_start, page_size=self.max_page_size, **kwargs)
        concat = isinstance(response, Sequence) if concat_response == "auto" else concat_response
        total = self.total_count

        data = response if concat else [response]
        if isinstance(total, int) and (total > self.max_page_size):
            from math import ceil
            from tqdm.auto import tqdm
            for page in tqdm(range(self.page_start + 1, ceil(total / self.max_page_size)), **tqdm_optinos):
                if concat:
                    data += self.collect(page=page, page_size=self.max_page_size, **kwargs)
                else:
                    data.append(self.collect(page=page, page_size=self.max_page_size, **kwargs))
        return data

    @Collector.async_with_session
    @Collector.async_with_semaphore
    async def collect_async_all(
            self,
            concat_response: Literal["auto"] | bool = "auto",
            tqdm_optinos: Dict = dict(disable=True),
            *,
            page: _SKIPPED = None,
            page_size: _SKIPPED = None,
            **kwargs
        ) -> Any:
        response = await self.collect_async(page=self.page_start, page_size=self.max_page_size, **kwargs)
        concat = isinstance(response, Sequence) if concat_response == "auto" else concat_response
        total = self.total_count

        data = response if concat else [response]
        if isinstance(total, int) and (total > self.max_page_size):
            from math import ceil
            from tqdm.auto import tqdm
            results = await tqdm.gather(*[
                self.collect_async(page=page, **kwargs)
                    for page in range(self.page_start + 1, ceil(total / self.max_page_size))], **tqdm_optinos)
            if concat:
                from itertools import chain
                return data + list(chain.from_iterable(results))
            else:
                return data + results
        else:
            return data

    def count_and_parse(self, response: Any, parser: Literal["self"] | Callable | None = "self", *args, **kwargs) -> Any:
        self.total_count = self.count_total(response)
        return self.parse(response, parser, *args, **kwargs)
