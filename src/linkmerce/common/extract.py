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
Headers = dict[_KT, _VT]
Configs = dict[_KT, _VT]

JsonObject = Union[dict, list]
JsonSerialize = Union[dict, list, bytes, IO]


class Client:
    """모든 클라이언트의 최상위 클래스."""
    ...


###################################################################
########################## Session Client #########################
###################################################################

class BaseSessionClient(Client, metaclass=ABCMeta):
    """HTTP 세션 기반 요청을 처리하는 추상 클라이언트 클래스."""

    method: str | None = None
    url: str | None = None
    cookies: str | None = None

    def __init__(
            self,
            session: Literal["per_request"] | Session | ClientSession = "per_request",
            cookies: str | dict | None = None,
            headers: Headers | None = None,
        ):
        """HTTP 세션 객체 및 요청 헤더를 초기화한다.

        Parameters
        ----------
        session: Literal["per_request"] | Session | ClientSession
            - `"per_request"`: HTTP 요청마다 세션 객체를 생성한다. (기본값)
            - `requests.Session()`: 동기 세션 객체
            - `aiohttp.ClientSession()`: 비동기 세션 객체
        cookies: str | dict | None
            세션에 저장할 쿠키를 `"키=값"` 형태의 문자열 또는 `{키: 값}` 형태의 딕셔너리로 입력할 수 있다.
        headers: Headers | None
            HTTP 요청 헤더를 입력할 수 있다.
        """
        self.set_session(session)
        self.set_cookies(cookies)
        self.set_request_params()
        self.set_request_body()
        self.set_request_headers(**(headers or dict()))

    @abstractmethod
    def request(self, **kwargs):
        """HTTP 요청 동작을 구현해야 한다."""
        raise NotImplementedError("The 'request' method must be implemented.")

    def build_request_message(self, **kwargs) -> dict:
        """HTTP 요청에 필요한 `method`, `url`, `params`, `data`, `json`, `headers`를 하나의 딕셔너리로 조합한다."""
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
        """HTTP 세션 객체를 반환한다."""
        return self.__session

    def set_session(self, session: Literal["per_request"] | Session | ClientSession = "per_request"):
        """HTTP 세션 객체를 설정한다."""
        self.__session = session

    def get_session_type(self) -> Literal["per_request", "requests.Session", "aiohttp.ClientSession"]:
        """현재 설정된 세션의 타입을 반환한다."""
        session = self.get_session()
        if session == "per_request":
            return "per_request"
        elif hasattr(session, "request"):
            if hasattr(session, "cookies"):
                return "requests.Session"
            elif hasattr(session, "cookie_jar"):
                return "aiohttp.ClientSession"
        raise TypeError(f"Unsupported session type: {type(session).__name__}")

    ######################### Session Cookies #########################

    def set_cookies(self, cookies: str | dict | None):
        """쿠키 문자열 또는 딕셔너리를 파싱하여 현재 세션에 설정한다."""
        if not cookies:
            return
        self.cookies = self.convert_cookies(cookies, to="str")

        session_type = self.get_session_type()
        if session_type == "per_request":
            return
        elif session_type == "requests.Session":
            cookies_map = self.convert_cookies(cookies, to="dict")
            self.get_session().cookies.update(cookies_map)
        elif session_type == "aiohttp.ClientSession":
            cookies_map = self.convert_cookies(cookies, to="dict")
            self.get_session().cookie_jar.update_cookies(cookies_map)
        # raise TypeError(f"Unsupported session type: {type(session).__name__}")

    def get_cookies(self, to: Literal["str", "dict"] = "str") -> str | dict | None:
        """현재 세션의 쿠키를 문자열로 반환한다. 세션 객체가 아니라면 None을 반환한다."""
        session_type = self.get_session_type()
        if session_type == "per_request":
            return self.convert_cookies(self.cookies, to=to)
        elif session_type == "requests.Session":
            cookies = self.get_session().cookies.get_dict()
            return self.convert_cookies(cookies, to=to)
        elif session_type == "aiohttp.ClientSession":
            cookies = {cookie.key: cookie.value for cookie in self.get_session().cookie_jar}
            return self.convert_cookies(cookies, to=to)
        # raise TypeError(f"Unsupported session type: {type(session).__name__}")

    def convert_cookies(self, cookies: str | dict, to: Literal["str", "dict"] = "str") -> str | dict:
        """쿠키를 지정된 타입(`str` 또는 `dict`)으로 변환하여 반환한다."""
        type_ = type(cookies).__name__
        if type_ == to:
            return cookies
        elif (to == "str") and isinstance(cookies, dict):
            return "; ".join([f"{key}={value}" for key, value in cookies.items()])
        elif (to == "dict") and isinstance(cookies, str):
            return dict([kv.split('=', maxsplit=1) for kv in cookies.split("; ") if '=' in kv])
        raise TypeError(f"Unsupported conversion from {type_} to {to}")

    ########################## Request Params #########################

    def build_request_params(self, **kwargs) -> dict | list[tuple] | bytes | None:
        """HTTP 요청 파라미터 속성에 키워드 인자를 추가해 반환한다."""
        return self.get_request_params()

    def get_request_params(self) -> dict | list[tuple] | bytes | None:
        """HTTP 요청 파라미터 속성을 반환한다."""
        return self.__params

    def set_request_params(self, params: dict | list[tuple] | bytes | None = None):
        """HTTP 요청 파라미터 속성을 설정한다."""
        self.__params = params

    ########################### Request Body ##########################

    def build_request_data(self, **kwargs) -> dict | list[tuple] | bytes | None:
        """HTTP 요청 본문 속성에 키워드 인자를 추가해 반환한다."""
        return None

    def build_request_json(self, **kwargs) -> dict | list[tuple] | bytes | None:
        """HTTP 요청 본문 속성에 키워드 인자를 추가해 JSON 형식으로 반환한다."""
        return None

    def get_request_body(self) -> dict | list[tuple] | bytes | IO | JsonSerialize | None:
        """HTTP 요청 본문 속성을 반환한다."""
        return self.__body

    def set_request_body(self, body: dict | list[tuple] | bytes | IO | JsonSerialize | None = None):
        """HTTP 요청 본문 속성을 설정한다."""
        self.__body = body

    ######################### Request Headers #########################

    def build_request_headers(self, **kwargs: str) -> dict[str, str]:
        """HTTP 요청 헤더 속성에 키워드 인자를 추가해 반환한다."""
        return self.get_request_headers()

    def get_request_headers(self) -> dict[str, str]:
        """HTTP 요청 헤더 속성을 반환한다."""
        return self.__headers

    def set_request_headers(
            self,
            authority: str | None = None,
            accept: str = "*/*",
            encoding: str = "gzip, deflate, br",
            language: Literal["ko", "en"] | str = "ko",
            connection: str = "keep-alive",
            contents: Literal["form", "javascript", "json", "text", "multipart"] | dict | None = None,
            cookies: str | None = None,
            host: str | None = None,
            origin: str | None = None,
            priority: str = "u=0, i",
            referer: str | None = None,
            client: str | None = None,
            mobile: bool = False,
            platform: str | None = None,
            metadata: Literal["cors", "navigate"] | dict[str, str] = "cors",
            https: bool = False,
            user_agent: str | None = None,
            ajax: bool = False,
            headers: dict | None = None,
            from_cookies: dict[str, str] | None = None,
            **kwargs: str
        ):
        """HTTP 요청 헤더 속성을 설정한다. 헤더에 쿠키가 있다면 `cookies` 속성 및 세션 객체에 업데이트한다.

        쿠키의 값을 꺼내 헤더에 넣어야 할 경우 `from_cookies`를 `{cookie_key: header_key}` 형태로 전달한다."""
        if headers is None:
            from linkmerce.utils.headers import build_headers
            headers = build_headers(
                authority, accept, encoding, language, connection, contents, cookies, host, origin, priority,
                referer, client, mobile, platform, metadata, https, user_agent, ajax, **kwargs)
        if "cookie" in headers:
            self.set_cookies(headers["cookie"])
        if from_cookies:
            cookies_map = self.get_cookies(to="dict")
            for cookie_key, header_key in from_cookies.items():
                try:
                    headers[header_key] = cookies_map[cookie_key]
                except KeyError:
                    raise KeyError(f"Missing '{cookie_key}' in cookies.")
        self.__headers = headers

    def require_cookies(self, key: str | None = None):
        """1. 세션 객체에 쿠키가 없으면 경고 메시지를 발생시킨다.
        2. `key`가 주어진 경우, 쿠키에 해당 키가 없다면 `ValueError`를 발생시킨다."""
        cookies = self.get_cookies(to="dict")
        if key and (key not in cookies):
            raise KeyError(f"Missing '{key}' in cookies.")
        elif not cookies:
            import warnings
            warnings.warn("Cookies will be required for upcoming requests.")


class RequestSessionClient(BaseSessionClient):
    """`requests` 라이브러리 기반의 동기 HTTP 요청을 수행하는 클라이언트 클래스."""

    method: str | None = None
    url: str | None = None
    cookies: str | None = None

    def request(
            self,
            method: str,
            url: str,
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: dict[str, str] = None,
            cookies: dict | RequestsCookieJar = None,
            **kwargs
        ) -> Response:
        """HTTP 요청을 수행하고 `Response` 객체를 반환한다."""
        optional = {"params": params, "data": data, "json": json, "headers": headers, "cookies": cookies}
        return self.get_session().request(method, url, **optional, **kwargs)

    def request_status(self, **kwargs) -> int:
        """HTTP 요청을 수행하고 응답 코드를 반환한다."""
        message = self.build_request_message(**kwargs)
        with self.get_session().request(**message) as response:
            return response.status_code

    def request_content(self, **kwargs) -> bytes:
        """HTTP 요청을 수행하고 응답 본문을 바이트로 반환한다."""
        message = self.build_request_message(**kwargs)
        with self.get_session().request(**message) as response:
            return response.content

    def request_text(self, **kwargs) -> str:
        """HTTP 요청을 수행하고 응답 본문을 유니코드 텍스트로 반환한다."""
        message = self.build_request_message(**kwargs)
        with self.get_session().request(**message) as response:
            return response.text

    def request_json(self, **kwargs) -> JsonObject:
        """HTTP 요청을 수행하고 응답 본문을 JSON 형식의 객체로 반환한다."""
        response = self.request_text(**kwargs)
        import json
        return json.loads(response)

    def request_json_safe(self, **kwargs) -> JsonObject | None:
        """HTTP 요청을 수행하고 응답 본문을 JSON 형식의 객체로 반환한다. JSON 파싱 실패 오류가 발생하면 무시한다."""
        response = self.request_text(**kwargs)
        import json
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            return None

    def request_headers(self, **kwargs) -> dict[str, str]:
        """HTTP 요청을 수행하고 응답 헤더를 반환한다."""
        message = self.build_request_message(**kwargs)
        with self.get_session().request(**message) as response:
            return response.headers

    def request_html(self, features: str | Sequence[str] | None = "html.parser", **kwargs) -> BeautifulSoup:
        """HTTP 요청을 수행하고 응답 본문을 `BeautifulSoup` 객체로 반환한다."""
        response = self.request_text(**kwargs)
        from bs4 import BeautifulSoup
        return BeautifulSoup(response, features)

    def request_excel(self, sheet_name: str | None = None, header: int = 1, warnings: bool = False, **kwargs) -> JsonObject:
        """HTTP 요청을 수행하고, 엑셀 형식의 응답 본문을 파싱하여 JSON 형식의 객체로 반환한다."""
        response = self.request_content(**kwargs)
        from linkmerce.utils.excel import excel2json
        return excel2json(response, sheet_name, header, warnings)

    def with_session(func):
        """HTTP 세션을 `per_request`로 설정했을 경우, 요청을 수행할 때마다 세션을 생성하고 종료하는 데코레이터."""
        @functools.wraps(func)
        def wrapper(self: RequestSessionClient, *args, **kwargs):
            if self.get_session() == "per_request":
                try:
                    import requests
                    with requests.Session() as session:
                        self.set_session(session)
                        self.set_cookies(self.cookies)
                        return func(self, *args, **kwargs)
                finally:
                    self.set_session("per_request")
            else:
                return func(self, *args, **kwargs)
        return wrapper


class AiohttpSessionClient(BaseSessionClient):
    """`aiohttp` 라이브러리 기반의 비동기 HTTP 요청을 수행하는 클라이언트 클래스."""

    method: str | None = None
    url: str | None = None
    cookies: str | None = None

    def request(self, *args, **kwargs):
        """비동기 HTTP 요청 시에는 `request` 메서드를 사용하지 않는다."""
        raise NotImplementedError("This feature does not support synchronous requests. Please use the request_async method instead.")

    async def request_async(
            self,
            method: str,
            url: str,
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | IO | None = None,
            json: JsonSerialize | None = None,
            headers: dict[str, str] = None,
            cookies: dict | LooseCookies = None,
            **kwargs
        ) -> ClientResponse:
        """비동기 HTTP 요청을 수행하고 `ClientResponse` 객체를 반환한다."""
        optional = {"params": params, "data": data, "json": json, "headers": headers, "cookies": cookies}
        return await self.get_session().request(method, url, **optional, **kwargs)

    async def request_async_status(self, **kwargs) -> int:
        """비동기 HTTP 요청을 수행하고 응답 코드를 반환한다."""
        message = self.build_request_message(**kwargs)
        async with self.get_session().request(**message) as response:
            return response.status

    async def request_async_content(self, **kwargs) -> bytes:
        """비동기 HTTP 요청을 수행하고 응답 본문을 바이트로 반환한다."""
        message = self.build_request_message(**kwargs)
        async with self.get_session().request(**message) as response:
            return response.content

    async def request_async_text(self, **kwargs) -> str:
        """비동기 HTTP 요청을 수행하고 응답 본문을 유니코드 텍스트로 반환한다."""
        message = self.build_request_message(**kwargs)
        async with self.get_session().request(**message) as response:
            return await response.text()

    async def request_async_json(self, **kwargs) -> JsonObject:
        """비동기 HTTP 요청을 수행하고 응답 본문을 JSON 형식의 객체로 반환한다."""
        response = await self.request_async_text(**kwargs)
        import json
        return json.loads(response)

    async def request_async_json_safe(self, **kwargs) -> JsonObject:
        """비동기 HTTP 요청을 수행하고 응답 본문을 JSON 형식의 객체로 반환한다. JSON 파싱 실패 오류가 발생하면 무시한다."""
        response = await self.request_async_text(**kwargs)
        import json
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            return None

    async def request_async_headers(self, **kwargs) -> dict[str, str]:
        """비동기 HTTP 요청을 수행하고 응답 헤더를 반환한다."""
        message = self.build_request_message(**kwargs)
        async with self.get_session().request(**message) as response:
            return response.headers

    async def request_async_html(self, features: str | Sequence[str] | None = "html.parser", **kwargs) -> BeautifulSoup:
        """비동기 HTTP 요청을 수행하고 응답 본문을 `BeautifulSoup` 객체로 반환한다."""
        response = await self.request_async_text(**kwargs)
        from bs4 import BeautifulSoup
        return BeautifulSoup(response, features)

    async def request_async_excel(self, sheet_name: str | None = None, header: int = 1, warnings: bool = False, **kwargs) -> JsonObject:
        """비동기 HTTP 요청을 수행하고, 엑셀 형식의 응답 본문을 파싱하여 JSON 형식의 객체로 반환한다."""
        response = await self.request_async_content(**kwargs)
        from linkmerce.utils.excel import excel2json
        return excel2json(response, sheet_name, header, warnings)

    def async_with_session(func):
        """HTTP 세션을 `per_request`로 설정했을 경우, 요청을 수행할 때마다 비동기 세션을 생성하고 종료하는 데코레이터."""
        @functools.wraps(func)
        async def wrapper(self: AiohttpSessionClient, *args, **kwargs):
            if self.get_session() == "per_request":
                try:
                    import aiohttp
                    async with aiohttp.ClientSession() as session:
                        self.set_session(session)
                        self.set_cookies(self.cookies)
                        return await func(self, *args, **kwargs)
                finally:
                    self.set_session("per_request")
            else:
                return await func(self, *args, **kwargs)
        return wrapper


class SessionClient(RequestSessionClient, AiohttpSessionClient):
    """동기 및 비동기 HTTP 요청을 모두 지원하는 통합 클라이언트 클래스."""

    method: str | None = None
    url: str | None = None
    cookies: str | None = None


###################################################################
########################### Task Client ###########################
###################################################################

class TaskClient(Client):
    """Task 기반의 요청 실행을 관리하는 클라이언트 클래스."""

    default_options: TaskOptions | None = None

    def __init__(self, options: TaskOptions | None = None, parser: Callable | None = None):
        """Task 옵션과 파서 함수를 초기화한다.

        Parameters
        ----------
        options: TaskOptions | None
            각각의 Task에 대한 옵션을 `{Task: 옵션}` 형태로 입력할 수 있다.   
            옵션 생략 시 `default_options` 속성으로 대체한다.
        parser: Callable | None
            Task에서 호출할 파서 함수를 입력할 수 있다.
        """
        self.set_options(options or self.default_options or dict())
        self.set_parser(parser)

    ########################### Task Options ##########################

    def get_options(self, name: str) -> TaskOption:
        """Task 옵션에서 특정 명칭의 Task에 대한 옵션을 반환한다."""
        return self.__options.get(name, dict())

    def set_options(self, options: TaskOptions):
        """전체 Task 옵션을 설정한다."""
        self.__options = options

    def build_options(self, name: str, **kwargs) -> TaskOption:
        """키워드 인자를 Task 옵션으로 반환한다. 키워드 인자가 없을 경우 기본값으로 특정 명칭의 Task에 대한 옵션을 반환한다."""
        options = {key: value for key, value in kwargs.items() if value is not None}
        return options or self.get_options(name)

    ############################## Parser #############################

    def get_parser(self) -> Callable:
        """Task에서 사용하는 파서 함수를 반환한다."""
        return self.__parser

    def set_parser(self, parser: Callable | None = None):
        """Task에서 사용할 파서 함수를 설정한다."""
        self.__parser = parser

    def parse(self, response: Any, *args, **kwargs) -> Any:
        """HTTP 응답 데이터를 전달받아 파서 함수에 직접적으로 전달한다. 파서 함수가 없다면 데이터를 그대로 반환한다."""
        return parser(response, *args, **kwargs) if (parser := self.get_parser()) is not None else response

    ########################### Import Task ###########################

    def request_loop(
            self,
            func: Callable | Coroutine,
            condition: Callable[..., bool],
            max_retries: int | None = None,
            request_delay: Literal["incremental"] | float | int | Sequence[int, int] | None = None,
            raise_errors: type | Sequence[type] | None = None,
            ignored_errors: type | Sequence[type] | None = None,
        ) -> RequestLoop:
        """조건(`condition`)을 만족할 때까지 함수를 반복 실행하는 `RequestLoop` Task를 생성한다.

        Parameters
        ----------
        func: Callable | Coroutine
            반복 실행할 함수 또는 코루틴
        condition: Callable[..., bool]
            실행 종료 또는 재시도 조건을 판단하는 함수. `True`로 인식되는 값이 반환되면 반복 실행을 종료한다.
        max_retries: int | None
            최대 반복 실행 횟수. `None`이면 조건을 만족할 때까지 무한 반복한다. 기본값은 1이다.
        request_delay: Literal["incremental"] | float | int | tuple[int, int]
            재시도 요청 간 대기 시간(초). `"incremental"`이면 재시도 요청 간 대기 시간(초)이 1초씩 점진적으로 증가한다.
        raise_errors: type | Sequence[type] | None
            즉시 예외를 발생시킬 에러 타입
        ignored_errors: type | Sequence[type] | None
            무시할 에러 타입

        Returns
        -------
        RequestLoop
        """
        from linkmerce.common.tasks import RequestLoop
        options = self.build_options("RequestLoop", **{
            "max_retries": max_retries,
            "request_delay": request_delay,
            "raise_errors": raise_errors,
            "ignored_errors": ignored_errors,
        })
        return RequestLoop(func, condition, parser=self.parse, **options)

    def request_each(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT, ...] | dict[_KT, _VT]] = list(),
            request_delay: float | int | tuple[int, int] | None = None,
            max_concurrent: int | None = None,
            tqdm_options: dict | None = None,
        ) -> RequestEach:
        """매개변수 목록(`context`)에 대해 순차적으로 함수를 실행하는 `RequestEach` Task를 생성한다.

        Parameters
        ----------
        func: Callable | Coroutine
            순차 실행할 함수 또는 병렬로 실행할 코루틴
        context: Sequence[tuple[_VT, ...] | dict[_KT, _VT]]
            함수를 순차 또는 병렬로 실행할 때 전달할 매개변수 목록
        request_delay: float | int | tuple[int, int]
            매개변수별 요청 간 대기 시간(초)
        max_concurrent: int | None
            비동기 요청 시 최대 동시 실행 횟수
        tqdm_options: dict | None
            반복 요청 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수

        Returns
        -------
        RequestEach
        """
        from linkmerce.common.tasks import RequestEach
        options = self.build_options("RequestEach", **{
            "request_delay": request_delay,
            "max_concurrent": max_concurrent,
            "tqdm_options": tqdm_options,
        })
        return RequestEach(func, context, parser=self.parse, **options)

    def request_each_loop(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT, ...] | dict[_KT, _VT]] = list(),
            request_delay: float | int | tuple[int, int] | None = None,
            max_concurrent: int | None = None,
            tqdm_options: dict | None = None,
            loop_options: dict | None = None,
        ) -> RequestEachLoop:
        """매개변수 목록(`context`)에 대해 순차적으로 요청하면서 조건을 만족할 때까지 재시도하는 `RequestEachLoop` Task를 생성한다.

        Parameters
        ----------
        func: Callable | Coroutine
            순차 실행할 함수 또는 병렬로 실행할 코루틴
        context: Sequence[tuple[_VT, ...] | dict[_KT, _VT]]
            함수를 순차 또는 병렬로 실행할 때 전달할 매개변수 목록
        request_delay: float | int | tuple[int, int]
            매개변수별 요청 간 대기 시간(초)
        max_concurrent: int | None
            비동기 요청 시 최대 동시 실행 횟수
        tqdm_options: dict | None
            반복 요청 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
        loop_options: dict
            `RequestLoop` Task에 전달할 속성

        Returns
        -------
        RequestEachLoop
        """
        from linkmerce.common.tasks import RequestEachLoop
        options = self.build_options("RequestEachLoop", **{
            "request_delay": request_delay,
            "max_concurrent": max_concurrent,
            "tqdm_options": tqdm_options,
        })
        if "loop_options" not in options:
            options["loop_options"] = self.build_options("RequestLoop", **(loop_options or dict()))
        return RequestEachLoop(func, context, parser=self.parse, **options)

    def paginate_all(
            self,
            func: Callable | Coroutine,
            counter: Callable[..., int],
            max_page_size: int,
            page_start: int = 1,
            request_delay: float | int | tuple[int, int] | None = None,
            max_concurrent: int | None = None,
            tqdm_options: dict | None = None,
        ) -> PaginateAll:
        """전체 페이지를 순회하며 데이터를 수집하는 `PaginateAll` Task를 생성한다.

        Parameters
        ----------
        func: Callable | Coroutine
            순차 실행할 함수 또는 병렬로 실행할 코루틴
        counter: Callable[..., int]
            전체 데이터 항목 수를 반환하는 함수. 반드시 정수를 반환해야 한다.
        max_page_size: int
            페이지 내 최대 데이터 항목 수
        page_start: int
            시작 페이지 번호
        request_delay: float | int | tuple[int, int]
            페이지 요청 간 대기 시간(초)
        max_concurrent: int | None
            비동기 요청 시 최대 동시 실행 횟수
        tqdm_options: dict | None
            페이지 순회 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수

        Returns
        -------
        PaginateAll
        """
        from linkmerce.common.tasks import PaginateAll
        options = self.build_options("PaginateAll", **{
            "request_delay": request_delay,
            "max_concurrent": max_concurrent,
            "tqdm_options": tqdm_options,
        })
        return PaginateAll(func, counter, max_page_size, page_start, parser=self.parse, **options)

    def request_each_pages(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT, ...] | dict[_KT, _VT]] | dict[_KT, _VT] = list(),
            request_delay: float | int | tuple[int, int] | None = None,
            max_concurrent: int | None = None,
            tqdm_options: dict | None = None,
            page_options: dict | None = None,
        ) -> RequestEachPages:
        """매개변수 목록(`context`)에 대해 순차적으로 요청하면서,
        각 인자별 전체 페이지를 수집하는 `RequestEachPages` Task를 생성한다.

        Parameters
        ----------
        func: Callable | Coroutine
            순차 실행할 함수 또는 병렬로 실행할 코루틴
        context: Sequence[tuple[_VT, ...] | dict[_KT, _VT]] | dict[_KT, _VT]
            함수를 순차 또는 병렬로 실행할 때 전달할 매개변수 목록
        request_delay: float | int | tuple[int, int]
            매개변수별 요청 간 대기 시간(초)
        max_concurrent: int | None
            비동기 요청 시 최대 동시 실행 횟수
        tqdm_options: dict | None
            반복 요청 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
        page_options: dict
            `PaginateAll` Task에 전달할 속성. 기본값은 `tqdm` 진행도 출력을 비활성화하는 옵션이다.

        Returns
        -------
        RequestEachPages
        """
        from linkmerce.common.tasks import RequestEachPages
        options = self.build_options("RequestEachPages", **{
            "request_delay": request_delay,
            "max_concurrent": max_concurrent,
            "tqdm_options": tqdm_options,
        })
        if "page_options" not in options:
            options["page_options"] = self.build_options("PaginateAll", **(page_options or dict()))
        return RequestEachPages(func, context, parser=self.parse, **options)

    def cursor_all(
            self,
            func: Callable,
            get_next_cursor: Callable[..., Any],
            next_cursor: Any | None = None,
            request_delay: float | int | tuple[int, int] | None = None,
        ) -> CursorAll:
        """커서 기반으로 데이터를 수집하는 `CursorAll` Task를 생성한다.

        Parameters
        ----------
        func: Callable
            순차 실행할 함수
        get_next_cursor: Callable[..., Any]
            다음 커서를 반환하는 함수
        next_cursor: Any | None
            시작 커서
        request_delay: float | int | tuple[int, int]
            커서 요청 간 대기 시간(초)

        Returns
        -------
        CursorAll
        """
        from linkmerce.common.tasks import CursorAll
        options = self.build_options("CursorAll", **{
            "next_cursor": next_cursor,
            "request_delay": request_delay,
        })
        return CursorAll(func, get_next_cursor, parser=self.parse, **options)

    def request_each_cursor(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT, ...] | dict[_KT, _VT]] | dict[_KT, _VT] = list(),
            request_delay: float | int | tuple[int, int] | None = None,
            tqdm_options: dict | None = None,
            cursor_options: dict | None = None,
        ) -> RequestEachCursor:
        """매개변수 목록(`context`)에 대해 순차적으로 요청하면서,
        각 인자별 다음 커서가 없을 때까지 모든 데이터를 수집하는 `RequestEachCursor` Task를 생성한다.

        Parameters
        ----------
        func: Callable | Coroutine
            순차 실행할 함수 또는 병렬로 실행할 코루틴
        context: Sequence[tuple[_VT, ...] | dict[_KT, _VT]] | dict[_KT, _VT]
            함수를 순차 또는 병렬로 실행할 때 전달할 매개변수 목록
        request_delay: float | int | tuple[int, int]
            매개변수별 요청 간 대기 시간(초)
        tqdm_options: dict | None
            반복 요청 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
        cursor_options: dict
            `CursorAll` Task에 전달할 속성. 기본값은 `tqdm` 진행도 출력을 비활성화하는 옵션이다.

        Returns
        -------
        RequestEachCursor
        """
        from linkmerce.common.tasks import RequestEachCursor
        options = self.build_options("RequestEachCursor", **{
            "request_delay": request_delay,
            "tqdm_options": tqdm_options,
        })
        if "cursor_options" not in options:
            options["cursor_options"] = self.build_options("CursorAll", **(cursor_options or dict()))
        return RequestEachCursor(func, context, parser=self.parse, **options)


###################################################################
############################ Extractor ############################
###################################################################

class Extractor(SessionClient, TaskClient, metaclass=ABCMeta):
    """ETL 파이프라인의 추출(Extract) 단계를 담당하는 추상 클래스.

    `SessionClient`의 HTTP 요청 기능과 `TaskClient`의 Task 생성 기능을 통합하며,
    `extract` 메서드를 구현하여 데이터를 추출한다."""

    method: str | None = None
    url: str | None = None
    cookies: str | None = None
    config_fields: dict | list | None = None
    default_options: TaskOptions | None = None

    def __init__(
            self,
            session: Literal["per_request"] | Session | ClientSession = "per_request",
            cookies: str | dict | None = None,
            headers: Headers | None = None,
            options: TaskOptions | None = None,
            configs: Configs | None = None,
            parser: Callable | None = None,
            **kwargs
        ):
        """HTTP 요청 및 응답 데이터 파싱을 위한 속성을 초기화한다.

        **NOTE** HTTP 요청을 위해 `configs`, `cookies`, `options`에   
        필수 또는 선택적으로 입력할 수 있는 항목은 각각의 클래스 docstring을 참고한다.

        Parameters
        ----------
        session: Literal["per_request"] | Session | ClientSession
            - `"per_request"`: HTTP 요청마다 세션 객체를 생성한다. (기본값)
            - `requests.Session()`: 동기 세션 객체
            - `aiohttp.ClientSession()`: 비동기 세션 객체
        cookies: str | dict | None
            세션에 저장할 쿠키를 `"키=값"` 형태의 문자열 또는 `{키: 값}` 형태의 딕셔너리로 입력할 수 있다.
        headers: Headers | None
            HTTP 요청 헤더를 입력할 수 있다.
        options: TaskOptions | None
            각각의 Task에 대한 옵션을 `{Task: 옵션}` 형태로 입력할 수 있다.   
            옵션 생략 시 `default_options` 속성으로 대체한다.
        configs: Configs | None
            사용자 정의 설정을 딕셔너리 형태로 입력할 수 있다.   
            `config_fields` 속성에 필수 설정 항목을 지정할 수 있고, 해당 설정이 없으면 `KeyError`가 발생한다.
        parser: Callable | None
            Task에서 호출할 파서 함수를 입력할 수 있다.
        """
        self.pre_init(**kwargs)
        self.set_configs(configs)
        SessionClient.__init__(self, session, cookies, headers)
        TaskClient.__init__(self, options, parser)
        self.post_init(**kwargs)

    def pre_init(self, **kwargs):
        """초기화 전에 호출되는 후크 메서드."""
        ...

    def post_init(self, **kwargs):
        """초기화가 완료된 후에 호출되는 후크 메서드."""
        ...

    @abstractmethod
    def extract(self, *args, **kwargs) -> Any:
        """데이터 추출(Extract) 동작을 구현해야 한다."""
        raise NotImplementedError("This feature does not support synchronous requests. Please use the extract_async method instead.")

    async def extract_async(self, *args, **kwargs) -> Any:
        """비동기 데이터 추출(Extract) 동작을 구현할 수 있다."""
        raise NotImplementedError("This feature does not support asynchronous requests. Please use the extract method instead.")

    def run(self, *args, how_to_run: Literal["sync", "async", "async_loop"] = "sync", **kwargs) -> Any:
        """`how_to_run` 키워드 인자로 `extract` 또는 `extract_async` 메서드를 실행한다.
        - `"sync"`: 동기 실행. `extract` 메서드를 실행한다. (기본값)
        - `"async"`: 비동기 실행. `asyncio.run` 함수로 감싸서 `extract_async` 코루틴을 실행한다.
        - `"async_loop"`: 비동기 루프 내 실행. 주피터 노트북 등 이미 비동기 이벤트 루프 내에 있을 경우에 비동기 실행을 처리한다.
        """
        if how_to_run == "sync":
            return self.extract(*args, **kwargs)
        elif how_to_run == "async":
            import asyncio
            return asyncio.run(self.extract_async(*args, **kwargs))
        elif how_to_run == "async_loop":
            import asyncio
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(asyncio.run, self.extract_async(*args, **kwargs))
                return future.result()
        raise ValueError("Invalid value for `how_to_run`. Supported values are: sync, async, async_loop.")

    @overload
    def get_config(self, key: _KT) -> _VT:
        ...

    @overload
    def get_config(self, key: _KT, default: Any | None = None) -> _VT:
        ...

    def get_config(self, key: _KT, default: Any | None = None) -> _VT:
        """HTTP 요청 중 사용할 설정 값을 조회한다."""
        return self.get_configs().get(key, default)

    def get_configs(self) -> dict[_KT, _VT]:
        """HTTP 요청 중 사용할 설정을 반환한다."""
        return self.__configs

    def set_configs(self, configs: Configs | None = None):
        """HTTP 요청 중 사용할 설정을 적용한다.

        `config_fields` 속성이 있을 경우 `configs`에서 지정된 경로의 값만 추출하고,
        이때 경로가 없다면 `KeyError`를 발생시킨다."""
        if self.config_fields:
            from linkmerce.utils.nested import select_values
            configs = select_values(configs, self.config_fields, on_missing="raise")
        self.__configs = configs or dict()

    def concat_path(self, url: str, *args: str) -> str:
        """URL 경로 세그먼트를 순서대로 연결하여 완성된 URL을 반환한다."""
        for path in args:
            if isinstance(path, str) and path:
                url = (url[:-1] if url.endswith('/') else url) + '/' + (path[1:] if path.startswith('/') else path)
        return url

    def generate_date_range(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            freq: Literal["D", "W", "M"] = "D",
            format: str = "%Y-%m-%d",
        ) -> list[dt.date] | dt.date:
        """시작일부터 종료일까지의 날짜 범위 리스트를 생성한다.

        일/주/월 빈도에 따라 다음과 같은 날짜 범위가 만들어진다.
        - `D`: 일 단위 `date` 객체를 반환한다.
        - `W`: 월요일 기준 주 단위로 `date` 객체를 반환한다.
        - `M`: 매월 1일 기준 월 단위로 `date` 객체를 반환한다."""
        from linkmerce.utils.date import date_range
        ranges = date_range(start_date, (start_date if end_date == ":start_date:" else end_date), freq, format)
        return ranges[0] if len(ranges) == 1 else ranges

    def generate_date_context(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            freq: Literal["D", "W", "M"] = "D",
            format: str = "%Y-%m-%d",
        ) -> list[dict[str, dt.date]] | dict[str, dt.date]:
        """시작일부터 종료일까지의 기간을 `{"start_date": 시작일, "end_date": 종료일}` 딕셔너리 리스트로 분할한다.

        일/주/월 빈도에 따라 다음과 같은 날짜 범위가 만들어진다.
        - `D`: 일 단위로 기간을 분할한다. 분할된 각각의 쌍은 시작일과 종료일이 동일하다.
        - `W`: 월요일 기준 주 단위로 기간을 분할한다. 중간 기간은 (월요일, 일요일) 쌍으로 생성된다.
        - `M`: 매월 1일 기준 월 단위로 기간을 분할한다. 중간 기간은 (1일, 말일) 쌍으로 생성된다."""
        from linkmerce.utils.date import date_pairs
        pairs = date_pairs(start_date, (start_date if end_date == ":start_date:" else end_date), freq, format)
        context = list(map(lambda values: dict(zip(["start_date", "end_date"], values)), pairs))
        return context[0] if len(context) == 1 else context

    def split_date_range(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            delta: int | dict[Literal["days", "seconds", "microseconds", "milliseconds", "minutes", "hours", "weeks"], float] = 1,
            format: str = "%Y-%m-%d",
        ) -> list[tuple[dt.date, dt.date]] | tuple[dt.date, dt.date]:
        """시작일부터 종료일까지의 기간을 지정된 간격으로 (시작일, 종료일) 쌍의 리스트로 분할한다.

        `delta`가 정수형인 경우 `days`로 인식하며, `timedelta`에 전달할 파라미터를 딕셔너리로 지정할 수도 있다."""
        from linkmerce.utils.date import date_split
        pairs = date_split(start_date, (start_date if end_date == ":start_date:" else end_date), delta, format)
        return pairs[0] if len(pairs) == 1 else pairs

    def split_date_context(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            delta: int | dict[Literal["days", "seconds", "microseconds", "milliseconds", "minutes", "hours", "weeks"], float] = 1,
            format: str = "%Y-%m-%d",
        ) -> list[dict[str, dt.date]] | dict[str, dt.date]:
        """시작일부터 종료일까지의 기간을 지정된 간격으로 `{"start_date": 시작일, "end_date": 종료일}` 딕셔너리 리스트로 분할한다.

        `delta`가 정수형인 경우 `days`로 인식하며, `timedelta`에 전달할 파라미터를 딕셔너리로 지정할 수도 있다."""
        from linkmerce.utils.date import date_split
        pairs = date_split(start_date, (start_date if end_date == ":start_date:" else end_date), delta, format)
        context = list(map(lambda values: dict(zip(["start_date", "end_date"], values)), pairs))
        return context[0] if len(context) == 1 else context


###################################################################
########################### LoginHandler ##########################
###################################################################

class LoginHandler(Extractor):
    """외부 서비스 로그인을 처리하는 추상 클래스.

    `login` 메서드를 구현하여 인증을 수행하고, 세션 쿠키를 보존한다."""

    cookies: str | None = None

    @abstractmethod
    def login(self, **kwargs):
        """로그인 동작을 구현해야 한다."""
        raise NotImplementedError("The 'login' method must be implemented.")

    def extract(self, *args, **kwargs) -> Any:
        """`Extractor` 부모 클래스의 추상 메서드는 구현하지 않는다. `login` 메서드가 역할을 대신한다."""
        raise NotImplementedError("Direct calls to extract method are not supported. Please use login method instead.")

    def build_headers(
            self,
            authority: str | None = None,
            accept: str = "*/*",
            encoding: str = "gzip, deflate, br",
            language: Literal["ko", "en"] | str = "ko",
            connection: str = "keep-alive",
            contents: Literal["form", "javascript", "json", "text", "multipart"] | dict | None = None,
            cookies: str | None = None,
            host: str | None = None,
            origin: str | None = None,
            priority: str = "u=0, i",
            referer: str | None = None,
            client: str | None = None,
            mobile: bool = False,
            platform: str | None = None,
            metadata: Literal["cors", "navigate"] | dict[str, str] = "cors",
            https: bool = False,
            user_agent: str | None = None,
            ajax: bool = False,
            **kwargs
        ) -> dict:
        """`build_headers` 함수를 호출하여 HTTP 요청 헤더를 생성한다."""
        from linkmerce.utils.headers import build_headers
        return build_headers(
                authority, accept, encoding, language, connection, contents, cookies, host, origin, priority,
                referer, client, mobile, platform, metadata, https, user_agent, ajax, **kwargs)

    def with_session(func):
        """HTTP 세션을 `per_request`로 설정했을 경우, 요청을 수행할 때마다 세션을 생성하고 종료하는 데코레이터."""
        @functools.wraps(func)
        def wrapper(self: LoginHandler, *args, **kwargs):
            if self.get_session() == "per_request":
                try:
                    import requests
                    with requests.Session() as session:
                        self.set_session(session)
                        self.set_cookies(self.cookies)
                        try:
                            return func(self, *args, **kwargs)
                        finally:
                            self.set_cookies(self.get_cookies())
                finally:
                    self.set_session("per_request")
            else:
                try:
                    return func(self, *args, **kwargs)
                finally:
                    self.set_cookies(self.get_cookies())
        return wrapper
