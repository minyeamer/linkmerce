from __future__ import annotations

from abc import ABCMeta, abstractmethod
import functools

from typing import Callable, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Coroutine, Hashable, IO, Literal, TypeVar
    from linkmerce.collect.tasks import RequestLoop, RequestEach
    from linkmerce.collect.tasks import PaginateAll, RequestEachPages
    from linkmerce.types import JsonObject, JsonSerialize, ResponseComponent
    from linkmerce.types import TaskOption, TaskOptions
    _KT = TypeVar("_KT", Hashable)
    _VT = TypeVar("_VT", Any)

    from requests import Session, Response
    from requests.cookies import RequestsCookieJar
    from aiohttp.client import ClientSession, ClientResponse
    from aiohttp.typedefs import LooseCookies

    from bs4 import BeautifulSoup
    import datetime as dt


###################################################################
########################## Session Client #########################
###################################################################

class BaseSessionClient(metaclass=ABCMeta):
    def __init__(
            self,
            session: Session | ClientSession | None = None,
            params: dict | list[tuple] | bytes | None = None,
            body: dict | dict | list[tuple] | bytes | IO | JsonSerialize | None = None,
            headers: dict[_KT,_VT] = dict(),
        ):
        self.set_session(session)
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

    def get_request_params(self, **kwargs) -> dict | list[tuple] | bytes:
        if kwargs:
            if isinstance(self.__params, dict):
                return dict(self.__params, **kwargs)
            elif self.__params is None:
                return kwargs
        return self.__params

    def set_request_params(self, params: dict | list[tuple] | bytes | None = None):
        self.__params = params

    def get_request_body(self, **kwargs) -> dict | list[tuple] | bytes | IO | JsonSerialize:
        if kwargs:
            if isinstance(self.__body, dict):
                return dict(self.__body, **kwargs)
            elif self.__body is None:
                return kwargs
        return self.__body

    def set_request_body(self, body: dict | dict | list[tuple] | bytes | IO | JsonSerialize | None = None):
        self.__body = body

    def get_request_headers(self, **kwargs) -> dict[str,str]:
        return dict(self.__headers, **kwargs) if kwargs else self.__headers

    def set_request_headers(
            self,
            authority: str = str(),
            accept: str = "*/*",
            encoding: str = "gzip, deflate, br",
            language: Literal["ko","en"] | str = "ko",
            connection: str = "keep-alive",
            contents: Literal["form", "javascript", "json", "text", "multipart"] | str | dict = str(),
            cookies: str = str(),
            host: str = str(),
            origin: str = str(),
            priority: str = "u=0, i",
            referer: str = str(),
            client: str = str(),
            mobile: bool = False,
            platform: str = str(),
            metadata: Literal["cors", "navigate"] | dict[str,str] = "navigate",
            https: bool = False,
            user_agent: str = str(),
            ajax: bool = False,
            headers: dict | None = None,
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
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: dict[str,str] = None,
            cookies: dict | RequestsCookieJar = None,
            parse: Literal["status","content","text","json","headers","html","table"] | None = None,
            **kwargs
        ) -> Response | ResponseComponent:
        kwargs = dict(method=method, url=url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs)
        if parse is None:
            return self.get_session().request(**kwargs)
        else:
            return getattr(self, f"request_{parse}")(**kwargs)

    def request_status(
            self,
            method: str,
            url: str,
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: dict[str,str] = None,
            cookies: dict | RequestsCookieJar = None,
            **kwargs
        ) -> int:
        with self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.status_code

    def request_content(
            self,
            method: str,
            url: str,
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: dict[str,str] = None,
            cookies: dict | RequestsCookieJar = None,
            **kwargs
        ) -> bytes:
        with self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.content

    def request_text(
            self,
            method: str,
            url: str,
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: dict[str,str] = None,
            cookies: dict | RequestsCookieJar = None,
            **kwargs
        ) -> str:
        with self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.text

    def request_json(
            self,
            method: str,
            url: str,
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: dict[str,str] = None,
            cookies: dict | RequestsCookieJar = None,
            **kwargs
        ) -> JsonObject:
        with self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.json()

    def request_headers(
            self,
            method: str,
            url: str,
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: dict[str,str] = None,
            cookies: dict | RequestsCookieJar = None,
            **kwargs
        ) -> dict[str,str]:
        with self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.headers

    def request_html(
            self,
            method: str,
            url: str,
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: dict[str,str] = None,
            cookies: dict | RequestsCookieJar = None,
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
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: dict[str,str] = None,
            cookies: dict | RequestsCookieJar = None,
            content_type: Literal["excel", "csv", "html", "xml"] | Sequence = "xlsx",
            table_options: dict = dict(),
            **kwargs
        ):
        from linkmerce.extensions.pandas import read_table
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
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: dict[str,str] = None,
            cookies: dict | LooseCookies = None,
            parse: Literal["status","content","text","json","headers","html","table"] | None = None,
            **kwargs
        ) -> ClientResponse | ResponseComponent:
        kwargs = dict(method=method, url=url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs)
        if parse is None:
            return await self.get_session().request(**kwargs)
        else:
            return await getattr(self, f"request_async_{parse}")(**kwargs)

    async def request_async_status(
            self,
            method: str,
            url: str,
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: dict[str,str] = None,
            cookies: dict | LooseCookies = None,
            **kwargs
        ) -> int:
        async with self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.status

    async def request_async_content(
            self,
            method: str,
            url: str,
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: dict[str,str] = None,
            cookies: dict | LooseCookies = None,
            **kwargs
        ) -> bytes:
        async with self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.content

    async def request_async_text(
            self,
            method: str,
            url: str,
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: dict[str,str] = None,
            cookies: dict | LooseCookies = None,
            **kwargs
        ) -> str:
        async with self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return await response.text()

    async def request_async_json(
            self,
            method: str,
            url: str,
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: dict[str,str] = None,
            cookies: dict | LooseCookies = None,
            **kwargs
        ) -> JsonObject:
        async with self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return await response.json()

    async def request_async_headers(
            self,
            method: str,
            url: str,
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: dict[str,str] = None,
            cookies: dict | LooseCookies = None,
            **kwargs
        ) -> dict[str,str]:
        async with self.get_session().request(method, url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs) as response:
            return response.headers

    async def request_async_html(
            self,
            method: str,
            url: str,
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: dict[str,str] = None,
            cookies: dict | LooseCookies = None,
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
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: dict[str,str] = None,
            cookies: dict | LooseCookies = None,
            content_type: Literal["excel", "csv", "html", "xml"] | Sequence = "xlsx",
            table_options: dict = dict(),
            **kwargs
        ):
        from linkmerce.extensions.pandas import read_table
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


class SessionClient(RequestSessionClient, AiohttpSessionClient):
    ...


###################################################################
########################### Extra Client ##########################
###################################################################

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

    def update_parser(self, parser: Literal["self"] | Callable | None = "self", **kwargs) -> dict:
        return dict(parser=self.get_parser(parser), **kwargs)

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


class TaskClient():
    def __init__(self, options: TaskOptions = dict()):
        self.set_options(options)

    def get_options(self, name: _KT) -> TaskOption:
        return self.__options.get(name, dict())

    def set_options(self, options: TaskOptions = dict()):
        self.__options = options

    def build_options(self, name: _KT, **kwargs) -> TaskOption:
        options = {key: value for key, value in kwargs.items() if value is not None}
        return options or self.get_options(name)

    def request_loop(
            self,
            func: Callable | Coroutine,
            condition: Callable[...,bool],
            count: int | None = None,
            delay: Literal["incremental"] | float | int | Sequence[int,int] | None = None,
            loop_error: type | None = None,
        ) -> RequestLoop:
        from linkmerce.collect.tasks import RequestLoop
        options = self.build_options("RequestLoop", count=count, delay=delay, loop_error=loop_error)
        return RequestLoop(func, condition, **options)

    def request_each(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT,...] | dict[_KT,_VT]] = list(),
            delay: float | int | tuple[int,int] | None = None,
            limit: int | None = None,
            loop_options: dict = dict(),
            tqdm_options: dict | None = None,
        ) -> RequestEach:
        from linkmerce.collect.tasks import RequestEach
        options = self.build_options("RequestEach", delay=delay, limit=limit, tqdm_options=tqdm_options)
        if "loop_options" not in options:
            options["loop_options"] = self.build_options("RequestLoop", **loop_options)
        return RequestEach(func, context, **options)

    def paginate_all(
            self,
            func: Callable | Coroutine,
            counter: Callable[...,int],
            max_page_size: int,
            page_start: int | None = None,
            delay: float | int | tuple[int,int] | None = None,
            limit: int | None = None,
            tqdm_options: dict | None = None,
            count_error: type | None = None,
        ) -> PaginateAll:
        from linkmerce.collect.tasks import PaginateAll
        options = self.build_options("PaginateAll",
            page_start=page_start, delay=delay, limit=limit, tqdm_options=tqdm_options, count_error=count_error)
        return PaginateAll(func, counter, max_page_size, **options)

    def request_each_pages(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT,...] | dict[_KT,_VT]] | dict[_KT,_VT] = list(),
            delay: float | int | tuple[int,int] | None = None,
            limit: int | None = None,
            loop_options: dict = dict(),
            tqdm_options: dict | None = None,
        ) -> RequestEachPages:
        from linkmerce.collect.tasks import RequestEachPages
        options = self.build_options("RequestEachPages", delay=delay, limit=limit, tqdm_options=tqdm_options)
        if "loop_options" not in options:
            options["loop_options"] = self.build_options("PaginateAll", **loop_options)
        return RequestEachPages(func, context, **options)


###################################################################
############################ Collector ############################
###################################################################

class Collector(SessionClient, ParserClient, TaskClient, metaclass=ABCMeta):
    method: str
    url: str
    args: list[str] = list()

    def __init__(
            self,
            session: Session | ClientSession | None = None,
            params: dict | list[tuple] | bytes | None = None,
            body: dict | dict | list[tuple] | bytes | IO | JsonSerialize | None = None,
            headers: dict[_KT,_VT] = dict(),
            parser: Literal["default"] | Callable | None = "default",
            options: TaskOptions = dict(),
        ):
        SessionClient.__init__(self, session, params, body, headers)
        ParserClient.__init__(self, parser)
        TaskClient.__init__(self, options)

    @abstractmethod
    def collect(self, **kwargs) -> Any:
        raise NotImplementedError("This feature does not support synchronous requests. Please use the collect_async method instead.")

    async def collect_async(self, **kwargs):
        raise NotImplementedError("This feature does not support asynchronous requests. Please use the collect method instead.")

    def build_request(
            self,
            params: dict | None = None,
            data: dict | None = None,
            json: dict | None = None,
            headers: dict | None = dict(),
        ) -> dict:
        message = dict(method=self.method, url=self.url)
        keys = ["params", "data", "json", "headers"]
        attrs = ["params", "body", "body", "headers"]
        for key, attr, kwargs in zip(keys, attrs, [params,data,json,headers]):
            if isinstance(kwargs, dict):
                message[key] = getattr(self, f"get_request_{attr}")(**kwargs)
        return message

    def generate_date_pairs(
            self,
            start: dt.date | str,
            end: dt.date | str,
            freq: Literal["D","W","M"] = "D",
            format: str = "%Y-%m-%d",
        ) -> tuple[dt.date,dt.date] | list[tuple[dt.date,dt.date]]:
        from linkmerce.utils.datetime import date_pairs
        pairs = date_pairs(start, end, freq, format)
        return pairs[0] if len(pairs) == 1 else pairs
