from __future__ import annotations

from abc import ABCMeta, abstractmethod
import functools

from typing import Any, Callable, Hashable, IO, TypeVar, Union, overload, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Coroutine, Literal, Sequence

    from linkmerce.common.tasks import TaskOption, TaskOptions
    from linkmerce.common.tasks import RequestLoop, RequestEach, RequestEachLoop
    from linkmerce.common.tasks import PaginateAll, RequestEachPages
    from linkmerce.common.tasks import CursorAll, RequestEachCursor

    from requests import Session, Response
    from requests.cookies import RequestsCookieJar
    from aiohttp.client import ClientSession, ClientResponse
    from aiohttp.typedefs import LooseCookies
    from bs4 import BeautifulSoup
    import datetime as dt

_KT = TypeVar("_KT", bound=Hashable)
_VT = TypeVar("_VT", bound=Any)
Headers = dict[_KT,_VT]
Variables = dict[_KT,_VT]

JsonObject = Union[dict, list]
JsonSerialize = Union[dict, list, bytes, IO]


class Client:
    ...


###################################################################
########################## Session Client #########################
###################################################################

class BaseSessionClient(Client, metaclass=ABCMeta):
    method: str | None = None
    url: str | None = None

    def __init__(
            self,
            session: Literal["per_request"] | Session | ClientSession = "per_request",
            headers: Headers = dict(),
        ):
        self.set_session(session)
        self.set_request_params()
        self.set_request_body()
        self.set_request_headers(**headers)

    @abstractmethod
    def request(self, **kwargs):
        raise NotImplementedError("The 'request' method must be implemented.")

    def build_request_message(self, **kwargs) -> dict:
        return dict(filter(lambda x: x[1] is not None, [
                ("method", kwargs["method"] if "method" in kwargs else self.method),
                ("url", kwargs["url"] if "url" in kwargs else self.url),
                ("params", self.build_request_params(**kwargs)),
                ("data", self.build_request_data(**kwargs)),
                ("json", self.build_request_json(**kwargs)),
                ("headers", self.build_request_headers(**kwargs)),
            ]))

    ######################### Request Session #########################

    def get_session(self) -> Literal["per_request"] | Session | ClientSession:
        return self.__session

    def set_session(self, session: Literal["per_request"] | Session | ClientSession = "per_request"):
        self.__session = session

    def set_cookies(self, cookies: str):
        cookies = dict([kv.split('=', maxsplit=1) for kv in cookies.split("; ")])
        self.get_session().cookies.update(cookies)

    def get_cookies(self) -> str:
        cookies = self.get_session().cookies
        return "; ".join([f"{key}={value}" for key, value in cookies.items()])

    ########################## Request Params #########################

    def build_request_params(self, **kwargs) -> dict | list[tuple] | bytes | None:
        return self.get_request_params()

    def get_request_params(self) -> dict | list[tuple] | bytes | None:
        return self.__params

    def set_request_params(self, params: dict | list[tuple] | bytes | None = None):
        self.__params = params

    ########################### Request Body ##########################

    def build_request_data(self, **kwargs) -> dict | list[tuple] | bytes | None:
        return None

    def build_request_json(self, **kwargs) -> dict | list[tuple] | bytes | None:
        return None

    def get_request_body(self) -> dict | list[tuple] | bytes | IO | JsonSerialize | None:
        return self.__body

    def set_request_body(self, body: dict | list[tuple] | bytes | IO | JsonSerialize | None = None):
        self.__body = body

    ######################### Request Headers #########################

    def build_request_headers(self, **kwargs: str) -> dict[str,str]:
        return self.get_request_headers()

    def get_request_headers(self) -> dict[str,str]:
        return self.__headers

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
            metadata: Literal["cors", "navigate"] | dict[str,str] = "cors",
            https: bool = False,
            user_agent: str = str(),
            ajax: bool = False,
            headers: dict | None = None,
            **kwargs: str
        ):
        if headers is None:
            from linkmerce.utils.headers import build_headers
            headers = build_headers(
                authority, accept, encoding, language, connection, contents, cookies, host, origin, priority,
                referer, client, mobile, platform, metadata, https, user_agent, ajax, **kwargs)
        self.__headers = headers

    def cookies_required(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            if "cookies" not in kwargs:
                import warnings
                warnings.warn("Cookies will be required for upcoming requests.")
            return func(self, *args, **kwargs)
        return wrapper


class RequestSessionClient(BaseSessionClient):
    method: str | None = None
    url: str | None = None

    def request(
            self,
            method: str,
            url: str,
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: dict[str,str] = None,
            cookies: dict | RequestsCookieJar = None,
            **kwargs
        ) -> Response:
        message = dict(method=method, url=url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs)
        return self.get_session().request(**message)

    def request_status(self, **kwargs) -> int:
        message = self.build_request_message(**kwargs)
        with self.get_session().request(**message) as response:
            return response.status_code

    def request_content(self, **kwargs) -> bytes:
        message = self.build_request_message(**kwargs)
        with self.get_session().request(**message) as response:
            return response.content

    def request_text(self, **kwargs) -> str:
        message = self.build_request_message(**kwargs)
        with self.get_session().request(**message) as response:
            return response.text

    def request_json(self, **kwargs) -> JsonObject:
        response = self.request_text(**kwargs)
        import json
        return json.loads(response)

    def request_json_safe(self, **kwargs) -> JsonObject:
        response = self.request_text(**kwargs)
        import json
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            return None

    def request_headers(self, **kwargs) -> dict[str,str]:
        message = self.build_request_message(**kwargs)
        with self.get_session().request(**message) as response:
            return response.headers

    def request_html(self, features: str | Sequence[str] | None = "html.parser", **kwargs) -> BeautifulSoup:
        response = self.request_text(**kwargs)
        from bs4 import BeautifulSoup
        return BeautifulSoup(response, features)

    def request_excel(self, sheet_name: str | None = None, header: int = 1, warnings: bool = False, **kwargs) -> JsonObject:
        response = self.request_content(**kwargs)
        from linkmerce.utils.excel import excel2json
        return excel2json(response, sheet_name, header, warnings)

    def with_session(func):
        @functools.wraps(func)
        def wrapper(self: RequestSessionClient, *args, **kwargs):
            if self.get_session() == "per_request":
                try:
                    return self._run_with_session(func, *args, **kwargs)
                finally:
                    self.set_session("per_request")
            else:
                return func(self, *args, **kwargs)
        return wrapper

    def _run_with_session(self, func: Callable, *args, **kwargs) -> Any:
        import requests
        with requests.Session() as session:
            self.set_session(session)
            return func(self, *args, **kwargs)


class AiohttpSessionClient(BaseSessionClient):
    method: str | None = None
    url: str | None = None

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
            **kwargs
        ) -> ClientResponse:
        message = dict(method=method, url=url, params=params, data=data, json=json, headers=headers, cookies=cookies, **kwargs)
        return await self.get_session().request(**message)

    async def request_async_status(self, **kwargs) -> int:
        message = self.build_request_message(**kwargs)
        async with self.get_session().request(**message) as response:
            return response.status

    async def request_async_content(self, **kwargs) -> bytes:
        message = self.build_request_message(**kwargs)
        async with self.get_session().request(**message) as response:
            return response.content

    async def request_async_text(self, **kwargs) -> str:
        message = self.build_request_message(**kwargs)
        async with self.get_session().request(**message) as response:
            return await response.text()

    async def request_async_json(self, **kwargs) -> JsonObject:
        response = await self.request_async_text(**kwargs)
        import json
        return json.loads(response)

    async def request_async_json_safe(self, **kwargs) -> JsonObject:
        response = await self.request_async_text(**kwargs)
        import json
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            return None

    async def request_async_headers(self, **kwargs) -> dict[str,str]:
        message = self.build_request_message(**kwargs)
        async with self.get_session().request(**message) as response:
            return response.headers

    async def request_async_html(self, features: str | Sequence[str] | None = "html.parser", **kwargs) -> BeautifulSoup:
        response = await self.request_async_text(**kwargs)
        from bs4 import BeautifulSoup
        return BeautifulSoup(response, features)

    async def request_async_excel(self, sheet_name: str | None = None, header: int = 1, warnings: bool = False, **kwargs) -> JsonObject:
        response = await self.request_async_content(**kwargs)
        from linkmerce.utils.excel import excel2json
        return excel2json(response, sheet_name, header, warnings)

    def async_with_session(func):
        @functools.wraps(func)
        async def wrapper(self: AiohttpSessionClient, *args, **kwargs):
            if self.get_session() == "per_request":
                try:
                    return await self._run_async_with_session(func, *args, **kwargs)
                finally:
                    self.set_session("per_request")
            else:
                return await func(self, *args, **kwargs)
        return wrapper

    async def _run_async_with_session(self, func: Callable, *args, **kwargs) -> Any:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            self.set_session(session)
            return await func(self, *args, **kwargs)


class SessionClient(RequestSessionClient, AiohttpSessionClient):
    method: str | None = None
    url: str | None = None


###################################################################
########################### Task Client ###########################
###################################################################

class TaskClient(Client):
    def __init__(self, options: TaskOptions = dict(), parser: Callable | None = None):
        self.set_options(options or self.default_options)
        self.set_parser(parser)

    ########################### Task Options ##########################

    def get_options(self, name: _KT) -> TaskOption:
        return self.__options.get(name, dict())

    def set_options(self, options: TaskOptions = dict()):
        self.__options = options

    def build_options(self, name: _KT, **kwargs) -> TaskOption:
        options = {key: value for key, value in kwargs.items() if value is not None}
        return options or self.get_options(name)

    @property
    def default_options(self) -> TaskOptions:
        return dict()

    ############################## Parser #############################

    def get_parser(self) -> Callable:
        return self.__parser

    def set_parser(self, parser: Callable | None = None):
        self.__parser = parser

    def parse(self, response: Any, *args, **kwargs) -> Any:
        return parser(response, *args, **kwargs) if (parser := self.get_parser()) is not None else response

    ########################### Import Task ###########################

    def request_loop(
            self,
            func: Callable | Coroutine,
            condition: Callable[...,bool],
            max_retries: int | None = None,
            request_delay: Literal["incremental"] | float | int | Sequence[int,int] | None = None,
            raise_errors: type | Sequence[type] | None = None,
            ignored_errors: type | Sequence[type] | None = None,
        ) -> RequestLoop:
        from linkmerce.common.tasks import RequestLoop
        input_options = dict(max_retries=max_retries, request_delay=request_delay, raise_errors=raise_errors, ignored_errors=ignored_errors)
        options = self.build_options("RequestLoop", **input_options)
        return RequestLoop(func, condition, parser=self.parse, **options)

    def request_each(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT,...] | dict[_KT,_VT]] = list(),
            request_delay: float | int | tuple[int,int] | None = None,
            max_concurrent: int | None = None,
            tqdm_options: dict | None = None,
        ) -> RequestEach:
        from linkmerce.common.tasks import RequestEach
        input_options = dict(request_delay=request_delay, max_concurrent=max_concurrent, tqdm_options=tqdm_options)
        options = self.build_options("RequestEach", **input_options)
        return RequestEach(func, context, parser=self.parse, **options)

    def request_each_loop(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT,...] | dict[_KT,_VT]] = list(),
            request_delay: float | int | tuple[int,int] | None = None,
            max_concurrent: int | None = None,
            tqdm_options: dict | None = None,
            loop_options: dict = dict(),
        ) -> RequestEachLoop:
        from linkmerce.common.tasks import RequestEachLoop
        input_options = dict(request_delay=request_delay, max_concurrent=max_concurrent, tqdm_options=tqdm_options)
        options = self.build_options("RequestEachLoop", **input_options)
        if "loop_options" not in options:
            options["loop_options"] = self.build_options("RequestLoop", **loop_options)
        return RequestEachLoop(func, context, parser=self.parse, **options)

    def paginate_all(
            self,
            func: Callable | Coroutine,
            counter: Callable[...,int],
            max_page_size: int,
            page_start: int = 1,
            request_delay: float | int | tuple[int,int] | None = None,
            max_concurrent: int | None = None,
            tqdm_options: dict | None = None,
        ) -> PaginateAll:
        from linkmerce.common.tasks import PaginateAll
        input_options = dict(request_delay=request_delay, max_concurrent=max_concurrent, tqdm_options=tqdm_options)
        options = self.build_options("PaginateAll", **input_options)
        return PaginateAll(func, counter, max_page_size, page_start, parser=self.parse, **options)

    def request_each_pages(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT,...] | dict[_KT,_VT]] | dict[_KT,_VT] = list(),
            request_delay: float | int | tuple[int,int] | None = None,
            max_concurrent: int | None = None,
            tqdm_options: dict | None = None,
            page_options: dict = dict(),
        ) -> RequestEachPages:
        from linkmerce.common.tasks import RequestEachPages
        input_options = dict(request_delay=request_delay, max_concurrent=max_concurrent, tqdm_options=tqdm_options)
        options = self.build_options("RequestEachPages", **input_options)
        if "page_options" not in options:
            options["page_options"] = self.build_options("PaginateAll", **page_options)
        return RequestEachPages(func, context, parser=self.parse, **options)

    def cursor_all(
            self,
            func: Callable,
            get_next_cursor: Callable[...,Any],
            next_cursor: Any | None = None,
            request_delay: float | int | tuple[int,int] | None = None,
        ) -> CursorAll:
        from linkmerce.common.tasks import CursorAll
        input_options = dict(next_cursor=next_cursor, request_delay=request_delay)
        options = self.build_options("CursorAll", **input_options)
        return CursorAll(func, get_next_cursor, parser=self.parse, **options)

    def request_each_cursor(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT,...] | dict[_KT,_VT]] | dict[_KT,_VT] = list(),
            request_delay: float | int | tuple[int,int] | None = None,
            tqdm_options: dict | None = None,
            cursor_options: dict = dict(),
        ) -> RequestEachCursor:
        from linkmerce.common.tasks import RequestEachCursor
        input_options = dict(request_delay=request_delay, tqdm_options=tqdm_options)
        options = self.build_options("RequestEachCursor", **input_options)
        if "cursor_options" not in options:
            options["cursor_options"] = self.build_options("CursorAll", **cursor_options)
        return RequestEachCursor(func, context, parser=self.parse, **options)


###################################################################
############################ Extractor ############################
###################################################################

class Extractor(SessionClient, TaskClient, metaclass=ABCMeta):
    method: str | None = None
    url: str | None = None

    def __init__(
            self,
            session: Literal["per_request"] | Session | ClientSession = "per_request",
            headers: Headers = dict(),
            options: TaskOptions = dict(),
            variables: Variables = dict(),
            parser: Callable | None = None,
        ):
        self.set_variables(variables)
        SessionClient.__init__(self, session, headers)
        TaskClient.__init__(self, options, parser)

    @abstractmethod
    def extract(self, *args, **kwargs) -> Any:
        raise NotImplementedError("This feature does not support synchronous requests. Please use the extract_async method instead.")

    async def extract_async(self, *args, **kwargs):
        raise NotImplementedError("This feature does not support asynchronous requests. Please use the extract method instead.")

    @overload
    def get_variable(self, key: _KT) -> _VT:
        ...

    @overload
    def get_variable(self, key: _KT, default: Any | None = None) -> _VT:
        ...

    def get_variable(self, key: _KT, default: Any | None = None) -> _VT:
        return self.get_variables().get(key, default)

    def get_variables(self) -> dict[_KT,_VT]:
        return self.__variables

    def set_variables(self, variables: Variables = dict()):
        self.__variables = variables

    def concat_path(self, url: str, *args: str) -> str:
        for path in args:
            if isinstance(path, str) and path:
                url = (url[:-1] if url.endswith('/') else url) + '/' + (path[1:] if path.startswith('/') else path)
        return url

    def generate_date_range(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            freq: Literal["D","W","M"] = "D",
            format: str = "%Y-%m-%d",
        ) -> list[dt.date] | dt.date:
        from linkmerce.utils.date import date_range
        ranges = date_range(start_date, (start_date if end_date == ":start_date:" else end_date), freq, format)
        return ranges[0] if len(ranges) == 1 else ranges

    def generate_date_context(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            freq: Literal["D","W","M"] = "D",
            format: str = "%Y-%m-%d",
        ) -> list[dict[str,dt.date]] | dict[str,dt.date]:
        from linkmerce.utils.date import date_pairs
        pairs = date_pairs(start_date, (start_date if end_date == ":start_date:" else end_date), freq, format)
        context = list(map(lambda values: dict(zip(["start_date","end_date"], values)), pairs))
        return context[0] if len(context) == 1 else context

    def split_date_range(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            delta: int | dict[Literal["days","seconds","microseconds","milliseconds","minutes","hours","weeks"],float] = 1,
            format: str = "%Y-%m-%d",
        ) -> list[tuple[dt.date,dt.date]] | tuple[dt.date,dt.date]:
        from linkmerce.utils.date import date_split
        pairs = date_split(start_date, (start_date if end_date == ":start_date:" else end_date), delta, format)
        return pairs[0] if len(pairs) == 1 else pairs

    def split_date_context(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            delta: int | dict[Literal["days","seconds","microseconds","milliseconds","minutes","hours","weeks"],float] = 1,
            format: str = "%Y-%m-%d",
        ) -> list[dict[str,dt.date]] | dict[str,dt.date]:
        from linkmerce.utils.date import date_split
        pairs = date_split(start_date, (start_date if end_date == ":start_date:" else end_date), delta, format)
        context = list(map(lambda values: dict(zip(["start_date","end_date"], values)), pairs))
        return context[0] if len(context) == 1 else context


###################################################################
########################### LoginHandler ##########################
###################################################################

class LoginHandler(Extractor):
    cookies: dict = dict()

    @abstractmethod
    def login(self, **kwargs):
        raise NotImplementedError("The 'login' method must be implemented.")

    def extract(self, *args, **kwargs) -> Any:
        raise NotImplementedError("Direct calls to extract method are not supported. Please use login method instead.")

    def with_session(func):
        @functools.wraps(func)
        def wrapper(self: LoginHandler, *args, **kwargs):
            if self.get_session() == "per_request":
                try:
                    return self._run_with_session(func, *args, **kwargs)
                finally:
                    cookies = self.get_cookies()
                    self.set_session("per_request")
                    self.set_cookies(cookies)
            else:
                return func(self, *args, **kwargs)
        return wrapper

    def set_cookies(self, cookies: str):
        if self.get_session() == "per_request":
            cookies = dict([kv.split('=', maxsplit=1) for kv in cookies.split("; ")])
            self.cookies.update(cookies)
        else:
            super().set_cookies(cookies)

    def get_cookies(self) -> str:
        if self.get_session() == "per_request":
            cookies = self.cookies
            return "; ".join([f"{key}={value}" for key, value in cookies.items()])
        else:
            return super().get_cookies()
