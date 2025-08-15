from __future__ import annotations

from abc import ABCMeta, abstractmethod
import functools

from typing import Any, Callable, Hashable, IO, TypeVar, Union, overload, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Coroutine, Literal, Sequence

    from linkmerce.common.tasks import TaskOption, TaskOptions
    from linkmerce.common.tasks import RequestLoop, RequestEach, RequestEachLoop
    from linkmerce.common.tasks import PaginateAll, RequestEachPages

    from requests import Session, Response
    from aiohttp.client import ClientSession, ClientResponse
    from bs4 import BeautifulSoup
    import datetime as dt

_KT = TypeVar("_KT", bound=Hashable)
_VT = TypeVar("_VT", bound=Any)
Headers = dict[_KT,_VT]
Variables = dict[_KT,_VT]

JsonObject = Union[dict, list]
JsonSerialize = Union[dict, list, bytes, IO]
ResponseComponent = Union[int, bytes, str, JsonObject, dict[str,str]]


class Client:
    ...


###################################################################
########################## Session Client #########################
###################################################################

class BaseSessionClient(Client, metaclass=ABCMeta):
    method: str
    url: str

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
                ("method", self.method),
                ("url", self.url),
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
            metadata: Literal["cors", "navigate"] | dict[str,str] = "navigate",
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
    method: str
    url: str

    def request(
            self,
            parse: Literal["status","content","text","json","headers","html"] | None = None,
            **kwargs
        ) -> Response | ResponseComponent:
        if parse is not None:
            return getattr(self, f"request_{parse}")(**kwargs)
        message = self.build_request_message(**kwargs)
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
        message = self.build_request_message(**kwargs)
        with self.get_session().request(**message) as response:
            return response.json()

    def request_json_safe(self, **kwargs) -> JsonObject:
        message = self.build_request_message(**kwargs)
        with self.get_session().request(**message) as response:
            from json import JSONDecodeError
            try:
                return response.json()
            except JSONDecodeError:
                return None

    def request_headers(self, **kwargs) -> dict[str,str]:
        message = self.build_request_message(**kwargs)
        with self.get_session().request(**message) as response:
            return response.headers

    def request_html(self, features: str | Sequence[str] | None = "html.parser", **kwargs) -> BeautifulSoup:
        response = self.request_text(**kwargs)
        from bs4 import BeautifulSoup
        return BeautifulSoup(response, features)

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
    method: str
    url: str

    def request(self, *args, **kwargs):
        raise NotImplementedError("This feature does not support synchronous requests. Please use the request_async method instead.")

    async def request_async(
            self,
            parse: Literal["status","content","text","json","headers","html"] | None = None,
            **kwargs
        ) -> ClientResponse | ResponseComponent:
        if parse is not None:
            return await getattr(self, f"request_async_{parse}")(**kwargs)
        message = self.build_request_message(**kwargs)
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
        message = self.build_request_message(**kwargs)
        async with self.get_session().request(**message) as response:
            return await response.json()

    async def request_async_json_safe(self, **kwargs) -> JsonObject:
        message = self.build_request_message(**kwargs)
        async with self.get_session().request(**message) as response:
            from json import JSONDecodeError
            try:
                return await response.json()
            except JSONDecodeError:
                return None

    async def request_async_headers(self, **kwargs) -> dict[str,str]:
        message = self.build_request_message(**kwargs)
        async with self.get_session().request(**message) as response:
            return response.headers

    async def request_async_html(self, features: str | Sequence[str] | None = "html.parser", **kwargs) -> BeautifulSoup:
        response = await self.request_async_text(**kwargs)
        from bs4 import BeautifulSoup
        return BeautifulSoup(response, features)

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
    method: str
    url: str


###################################################################
########################### Task Client ###########################
###################################################################

class TaskClient(Client):
    def __init__(self, options: TaskOptions = dict(), parser: Callable | None = None):
        self.set_options(options)
        self.set_parser(parser)

    ########################### Task Options ##########################

    def get_options(self, name: _KT) -> TaskOption:
        return self.__options.get(name, dict())

    def set_options(self, options: TaskOptions = dict()):
        self.__options = options

    def build_options(self, name: _KT, **kwargs) -> TaskOption:
        options = {key: value for key, value in kwargs.items() if value is not None}
        return options or self.get_options(name)

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
            count: int | None = None,
            delay: Literal["incremental"] | float | int | Sequence[int,int] | None = None,
            loop_error: type | None = None,
        ) -> RequestLoop:
        from linkmerce.common.tasks import RequestLoop
        options = self.build_options("RequestLoop", count=count, delay=delay, loop_error=loop_error)
        return RequestLoop(func, condition, parser=self.parse, **options)

    def request_each(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT,...] | dict[_KT,_VT]] = list(),
            delay: float | int | tuple[int,int] | None = None,
            limit: int | None = None,
            tqdm_options: dict | None = None,
        ) -> RequestEach:
        from linkmerce.common.tasks import RequestEach
        options = self.build_options("RequestEach", delay=delay, limit=limit, tqdm_options=tqdm_options)
        return RequestEach(func, context, parser=self.parse, **options)

    def request_each_loop(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT,...] | dict[_KT,_VT]] = list(),
            delay: float | int | tuple[int,int] | None = None,
            limit: int | None = None,
            tqdm_options: dict | None = None,
            loop_options: dict = dict(),
        ) -> RequestEachLoop:
        from linkmerce.common.tasks import RequestEachLoop
        options = self.build_options("RequestEachLoop", delay=delay, limit=limit, tqdm_options=tqdm_options)
        if "loop_options" not in options:
            options["loop_options"] = self.build_options("RequestLoop", **loop_options)
        return RequestEachLoop(func, context, parser=self.parse, **options)

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
        from linkmerce.common.tasks import PaginateAll
        options = self.build_options("PaginateAll",
            page_start=page_start, delay=delay, limit=limit, tqdm_options=tqdm_options, count_error=count_error)
        return PaginateAll(func, counter, max_page_size, parser=self.parse, **options)

    def request_each_pages(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT,...] | dict[_KT,_VT]] | dict[_KT,_VT] = list(),
            delay: float | int | tuple[int,int] | None = None,
            limit: int | None = None,
            tqdm_options: dict | None = None,
            page_options: dict = dict(),
        ) -> RequestEachPages:
        from linkmerce.common.tasks import RequestEachPages
        options = self.build_options("RequestEachPages", delay=delay, limit=limit, tqdm_options=tqdm_options)
        if "page_options" not in options:
            options["page_options"] = self.build_options("PaginateAll", **page_options)
        return RequestEachPages(func, context, parser=self.parse, **options)


###################################################################
############################ Extractor ############################
###################################################################

class Extractor(SessionClient, TaskClient, metaclass=ABCMeta):
    method: str
    url: str

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

    def generate_date_context(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str,
            freq: Literal["D","W","M"] = "D",
            format: str = "%Y-%m-%d",
        ) -> list[dict[str,dt.date]] | dict[str,dt.date]:
        from linkmerce.utils.date import date_pairs
        pairs = date_pairs(start_date, end_date, freq, format)
        context = list(map(lambda values: dict(zip(["start_date","end_date"], values)), pairs))
        return context[0] if len(context) == 1 else context
