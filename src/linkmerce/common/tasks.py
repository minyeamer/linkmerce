from __future__ import annotations

from abc import ABCMeta, abstractmethod

from typing import Any, Hashable, Iterable, Sequence, TypeVar, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Callable, Coroutine, Literal
    _SKIPPED = TypeVar("_SKIPPED", bound=None)

_KT = TypeVar("_KT", bound=Hashable)
_VT = TypeVar("_VT", bound=Any)
TaskOption = dict[_KT, _VT]
TaskOptions = dict[_KT, TaskOption]


class Task(metaclass=ABCMeta):
    """모든 Task의 최상위 추상 기반 클래스."""

    @abstractmethod
    def run(self):
        """Task 동작을 정의해야 한다."""
        raise NotImplementedError("This task does not support synchronous execution. Please use the run_async method instead.")

    async def run_async(self):
        """비동기 Task 동작을 정의할 수 있다."""
        raise NotImplementedError("This task does not support asynchronous execution. Please use the run method instead.")

    def setattr(self, name: str, value: _VT, *args: str | _VT) -> Task:
        """하나 이상의 속성을 설정하고 자기 자신을 반환하여 메서드 체이닝을 지원한다."""
        self.__setattr__(name, value)
        if args:
            if (len(args) % 2) == 0:
                for i in range(len(args)//2):
                    self.__setattr__(args[i*2], args[i*2+1])
            else:
                raise ValueError("All positional arguments must be provided as name-value pairs.")
        return self


###################################################################
############################# Request #############################
###################################################################

class Request(Task):
    """단일 요청을 수행하고 파서 함수를 적용하는 Task."""

    def __init__(self, func: Callable | Coroutine, parser: Callable | None = None):
        """요청을 수행하는 함수와 파서 함수를 초기화한다."""
        self.func = func
        self.parser = parser

    def run(self, *args, **kwargs) -> Any:
        """요청을 수행하고 파서 함수를 적용한다."""
        result = self.func(*args, **kwargs)
        return self._parse(result, args, kwargs)

    async def run_async(self, *args, **kwargs) -> Any:
        """비동기 요청을 수행하고 파서 함수를 적용한다."""
        result = await self.func(*args, **kwargs)
        return self._parse(result, args, kwargs)

    def parse(self, parser: Callable | None = None) -> Request:
        """파서 함수를 설정한다."""
        return self.setattr("parser", parser)

    def _parse(self, result: Any, args: tuple = tuple(), kwargs: dict = dict()) -> dict:
        """요청 결과를 파서 함수에 전달한다."""
        return self.parser(result, *args, **kwargs) if self.parser is not None else result


###################################################################
############################# Run Loop ############################
###################################################################

class RunLoop(Task):
    """조건을 만족할 때까지 함수를 반복 실행하는 Task."""

    def __init__(
            self,
            func: Callable | Coroutine,
            condition: Callable[..., bool],
            max_retries: int | None = 1,
            delay: Literal["incremental"] | float | int | Sequence[int, int] = "incremental",
            raise_errors: type | Sequence[type] = tuple(),
            ignored_errors: type | Sequence[type] = tuple(),
        ):
        """`RunLoop` Task 속성을 초기화한다.

        Args:
            `func`: 반복 실행할 함수 또는 코루틴.
            `condition`: 재시도 조건을 판단하는 함수. `True`로 판단되는 값이 반환되면 반복 실행을 종료한다.
            `max_retries`: 최대 반복 실행 횟수. `None`이면 조건을 만족할 때까지 무한 반복한다.
            `delay`: 재시도 간 대기 시간. `"incremental"`이면 점진적으로 증가한다.
            `raise_errors`: 즉시 예외를 발생시킬 에러 타입.
            `ignored_errors`: 무시할 에러 타입."""
        self.func = func
        self.condition = condition
        self.max_retries = max_retries
        self.delay = delay
        self.raise_errors = raise_errors
        self.ignored_errors = ignored_errors

    def run(self, *args, **kwargs) -> Any:
        """`max_retries`에 도달할 때까지 함수를 반복 실행하고, 조건에 만족하면 결과를 반환한다."""
        if not isinstance(self.max_retries, int):
            return self._infinite_run(args, kwargs)
        for retry_count in range(1, self.max_retries+1):
            try:
                result = self.func(*args, **kwargs)
                if self.condition(result):
                    return result
            except self.raise_errors as error:
                raise error
            except self.ignored_errors:
                if retry_count == self.max_retries:
                    return
            self._sleep(retry_count)
        self._raise_loop_error()

    async def run_async(self, *args, **kwargs) -> Any:
        """`max_retries`에 도달할 때까지 비동기 코루틴을 반복 실행하고, 조건에 만족하면 결과를 반환한다."""
        if not isinstance(self.max_retries, int):
            raise RuntimeError("Invalid max_retries value provided.")
        for retry_count in range(1, self.max_retries+1):
            try:
                result = await self.func(*args, **kwargs)
                if self.condition(result):
                    return result
            except self.raise_errors as error:
                raise error
            except self.ignored_errors:
                if retry_count == self.max_retries:
                    return
            await self._sleep_async(retry_count)
        self._raise_loop_error()

    def _infinite_run(self, args: tuple = tuple(), kwargs: dict = dict()) -> Any:
        """`max_retries`가 없을 경우 조건을 만족할 때까지 함수를 무한히 반복 실행한다."""
        retry_count = 1
        while True:
            result = self.func(*args, **kwargs)
            if self.condition(result):
                return result
            else:
                self._sleep(retry_count)
                retry_count += 1

    def _sleep(self, retry_count: int):
        """`retry_count`에 따라 점진적으로 증가된 시간 또는 고정된 시간만큼 대기한다."""
        import time
        if self.delay == "incremental":
            time.sleep(retry_count)
        else:
            from linkmerce.utils.progress import _get_seconds
            time.sleep(_get_seconds(self.delay))

    async def _sleep_async(self, retry_count: int):
        """`retry_count`에 따라 점진적으로 증가된 시간 또는 고정된 시간만큼 대기한다."""
        import asyncio
        if self.delay == "incremental":
            await asyncio.sleep(retry_count)
        else:
            from linkmerce.utils.progress import _get_seconds
            await asyncio.sleep(_get_seconds(self.delay))

    def _raise_loop_error(self):
        """`max_retries`에 도달하면 `RuntimeError`를 발생시킨다."""
        raise RuntimeError("Exceeded maximum retry attempts without success.")


class RequestLoop(RunLoop, Request):
    """`Request`와 `RunLoop`를 결합하여 조건을 만족할 때까지 반복 요청하는 Task."""

    def __init__(
            self,
            func: Callable | Coroutine,
            condition: Callable[..., bool],
            parser: Callable | None = None,
            max_retries: int | None = 1,
            request_delay: Literal["incremental"] | float | int | Sequence[int, int] = "incremental",
            raise_errors: type | Sequence[type] = tuple(),
            ignored_errors: type | Sequence[type] = tuple(),
        ):
        """`RunLoop` Task 속성과 파서 함수를 초기화한다.

        Args:
            `func`: 반복 실행할 함수 또는 코루틴.
            `condition`: 재시도 조건을 판단하는 함수. `True`로 판단되는 값이 반환되면 반복 실행을 종료한다.
            `parser`: 조건을 만족했을 때 결과에 적용할 파서 함수.
            `max_retries`: 최대 반복 실행 횟수. `None`이면 조건을 만족할 때까지 무한 반복한다.
            `delay`: 재시도 간 대기 시간. `"incremental"`이면 점진적으로 증가한다.
            `raise_errors`: 즉시 예외를 발생시킬 에러 타입.
            `ignored_errors`: 무시할 에러 타입."""
        RunLoop.__init__(self, func, condition, max_retries, request_delay, raise_errors, ignored_errors)
        self.parser = parser

    def run(self, *args, **kwargs) -> Any:
        """조건을 만족할 때까지 함수를 반복 실행하고, 그 결과에 파서 함수를 적용한다."""
        result = super().run(*args, **kwargs)
        if result is not None:
            return self._parse(result, args, kwargs)

    async def run_async(self, *args, **kwargs) -> Any:
        """조건을 만족할 때까지 비동기 코루틴을 반복 실행하고, 그 결과에 파서 함수를 적용한다."""
        result = await super().run_async(*args, **kwargs)
        if result is not None:
            return self._parse(result, args, kwargs)

    def parse(self, parser: Callable | None = None) -> RequestLoop:
        """파서 함수를 설정한다."""
        return self.setattr("parser", parser)


###################################################################
############################# For Each ############################
###################################################################

class ForEach(Task):
    """리스트의 각 항목에 대해 함수를 순차 또는 병렬로 실행하는 Task."""

    def __init__(
            self,
            func: Callable | Coroutine,
            array: Sequence[tuple[_VT, ...] | dict[_KT, _VT]] = list(),
            delay: float | int | tuple[int, int] = 0.,
            max_concurrent: int | None = None,
            tqdm_options: dict = dict(),
        ):
        """`ForEach` Task 속성을 초기화한다.

        Args:
            `func`: 순차 실행할 함수 또는 병렬로 실행할 코루틴.
            `array`: 함수를 순차 또는 병렬로 실행할 때 전달할 인자 목록.
            `parser`: 조건을 만족했을 때 결과에 적용할 파서 함수.
            `delay`: 함수 실행 간 대기 시간.
            `max_concurrent`: 비동기 실행 시 최대 동시 실행 횟수.
            `tqdm_options`: 진행도를 표시하는 `tqdm`에 전달할 매개변수."""
        self.func = func
        self.array = array
        self.delay = delay
        self.max_concurrent = max_concurrent
        self.tqdm_options = tqdm_options
        self.kwargs = dict()
        self.concat_how = "auto"

    def run(self) -> list:
        """함수를 여러 인자 목록에 대해 순차적으로 실행한다. 실행 중 `tqdm` 모듈로 진행도를 표시한다."""
        from linkmerce.utils.progress import gather
        results = gather(self.func, self.array, self.kwargs, self.delay, self.tqdm_options)
        return self._concat_results(results)

    async def run_async(self) -> list:
        """코루틴을 여러 인자 목록에 대해 비동기로 병렬 실행한다. 실행 중 `tqdm` 모듈로 진행도를 표시한다."""
        from linkmerce.utils.progress import gather_async
        results = await gather_async(self.func, self.array, self.kwargs, self.delay, self.max_concurrent, self.tqdm_options)
        return self._concat_results(results)

    def expand(self, **map_kwargs: Iterable[_VT]) -> ForEach:
        """여러 인자 목록들의 카테시안 곱을 전개하여 하나의 인자 목록을 설정한다."""
        from linkmerce.utils.progress import _expand_kwargs
        array = _expand_kwargs(**map_kwargs)
        return self.setattr("array", array)

    def partial(self, **kwargs: _VT) -> ForEach:
        """모든 실행에 공통으로 전달할 키워드 인자를 설정한다."""
        return self.setattr("kwargs", kwargs)

    def concat(self, how: Literal["always", "never", "auto"] = "auto") -> ForEach:
        """결과 리스트의 병합 방식을 설정한다.
        - `always`: 모든 실행 결과가 리스트 타입일 것으로 인식하고 2차원 리스트를 1차원으로 전개한다.
        - `never`: 실행 결과 리스트를 병합하지 않는다.
        - `auto`: 모든 실행 결과가 리스트(`Sequence`) 타입인지 확인하여 선택적으로 병합한다."""
        return self.setattr("concat_how", how)

    def _concat_results(self, results: list) -> list:
        """결과 리스트를 `concat_how` 방식에 따라 병합한다."""
        def chain_list(iterable: list):
            from itertools import chain
            return list(chain.from_iterable(iterable))
        if self.concat_how == "always":
            return chain_list(results)
        elif self.concat_how == "auto":
            iterable = [result for result in results if isinstance(result, Sequence)]
            return chain_list(iterable) if iterable else results
        else:
            return results


class RequestEach(ForEach, Request):
    """`Request`와 `ForEach`를 결합하여 여러 인자 목록에 대해 순차적으로 요청하는 Task."""

    def __init__(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT, ...] | dict[_KT, _VT]] | dict[_KT, _VT] = list(),
            parser: Callable | None = None,
            request_delay: float | int | tuple[int, int] = 0.,
            max_concurrent: int | None = None,
            tqdm_options: dict = dict(),
        ):
        """`RequestEach` Task 속성을 초기화한다.

        Args:
            `func`: 순차 실행할 함수 또는 병렬로 실행할 코루틴.
            `context`: 함수를 순차 또는 병렬로 실행할 때 전달할 인자 목록.
            `parser`: 조건을 만족했을 때 결과에 적용할 파서 함수.
            `request_delay`: 요청 간 대기 시간.
            `max_concurrent`: 비동기 요청 시 최대 동시 실행 횟수.
            `tqdm_options`: 진행도를 표시하는 `tqdm`에 전달할 매개변수."""
        self.func = func
        self.context = context
        self.parser = parser
        self.delay = request_delay
        self.max_concurrent = max_concurrent
        self.tqdm_options = tqdm_options
        self.kwargs = dict()
        self.concat_how = "auto"

    @property
    def callable(self) -> Callable:
        return Request(self.func, self.parser).run

    @property
    def coroutine(self) -> Coroutine:
        return Request(self.func, self.parser).run_async

    def run(self) -> list | Any:
        """함수를 여러 인자 목록에 대해 순차적으로 실행한다. 실행 중 `tqdm` 모듈로 진행도를 표시한다."""
        if isinstance(self.context, Sequence):
            from linkmerce.utils.progress import gather
            results = gather(self.callable, self.context, self.kwargs, self.delay, self.tqdm_options)
            return self._concat_results(results)
        elif isinstance(self.context, dict):
            return self.callable(**self.context, **self.kwargs)
        else:
            self._raise_context_error()

    async def run_async(self) -> list | Any:
        """코루틴을 여러 인자 목록에 대해 비동기로 병렬 실행한다. 실행 중 `tqdm` 모듈로 진행도를 표시한다."""
        if isinstance(self.context, Sequence):
            from linkmerce.utils.progress import gather_async
            results = await gather_async(self.coroutine, self.context, self.kwargs, self.delay, self.max_concurrent, self.tqdm_options)
            return self._concat_results(results)
        elif isinstance(self.context, dict):
            return await self.coroutine(**self.context, **self.kwargs)
        else:
            self._raise_context_error()

    def parse(self, parser: Callable | None = None) -> RequestEach:
        """파서 함수를 설정한다."""
        return self.setattr("parser", parser)

    def partial(self, **kwargs: _VT) -> RequestEach:
        """모든 실행에 공통으로 전달할 키워드 인자를 설정한다."""
        return self.setattr("kwargs", kwargs)

    def expand(self, **map_kwargs: _VT) -> RequestEach:
        """여러 인자 목록들의 카테시안 곱을 전개하여 하나의 인자 목록을 설정한다."""
        mapping, partial = self._split_map_kwargs(map_kwargs)
        context = self._expand_context(mapping) if mapping else self.context
        return self.setattr("context", (context or dict()), "kwargs", dict(self.kwargs, **partial))

    def concat(self, how: Literal["always", "never", "auto"] = "auto") -> RequestEach:
        """결과 리스트의 병합 방식을 설정한다."""
        return self.setattr("concat_how", how)

    def _split_map_kwargs(self, map_kwargs: dict[_KT, _VT]) -> tuple[dict[_KT, _VT], dict[_KT, _VT]]:
        """여러 인자 목록에서 `Sequence` 타입만 인자 목록으로 분리한다."""
        sequential, non_sequential = dict(), self.kwargs.copy()
        for key, value in map_kwargs.items():
            if (not isinstance(value, str)) and isinstance(value, Sequence):
                sequential[key] = value
            else:
                non_sequential[key] = value
        return sequential, non_sequential

    def _expand_context(self, mapping: dict[_KT, Sequence]) -> Sequence[dict[_KT, _VT]]:
        """여러 인자 목록들의 카테시안 곱을 딕셔너리 리스트로 전개한다."""
        from linkmerce.utils.progress import _expand_kwargs
        context = self._get_sequential_context()
        if context:
            context = _expand_kwargs(context_=context, **mapping)
            unpack = lambda context_, **kwargs: dict(context_, **kwargs)
            return [unpack(**kwargs) for kwargs in context]
        else:
            return _expand_kwargs(**mapping)

    def _get_sequential_context(self) -> list[dict[_KT, _VT]]:
        """인자 목록이 항상 리스트로 반환됨을 보장한다."""
        if self.context:
            if isinstance(self.context, Sequence) and all(map(lambda x: isinstance(x, dict), self.context)):
                return self.context
            elif isinstance(self.context, dict):
                return [self.context]
        return list()

    def _raise_context_error(self):
        """인자 목록이 올바른 형태가 아니면 `ValueError`를 발생시킨다."""
        raise ValueError("Invalid type for context. Context must be a sequence or a dict.")


class RequestEachLoop(RequestEach):
    """`Request`, `RunLoop`, `ForEach`를 결합하여,
    여러 인자 목록에 대해 순차적으로 요청하면서 조건을 만족할 때까지 재시도하는 Task."""

    def __init__(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT, ...] | dict[_KT, _VT]] | dict[_KT, _VT] = list(),
            parser: Callable | None = None,
            request_delay: float | int | tuple[int, int] = 0.,
            max_concurrent: int | None = None,
            tqdm_options: dict = dict(),
            loop_options: dict = dict(),
        ):
        """`RequestEach` Task 속성과 `RequestLoop` Task 속성을 초기화한다.

        Args:
            `func`: 순차 실행할 함수 또는 병렬로 실행할 코루틴.
            `context`: 함수를 순차 또는 병렬로 실행할 때 전달할 인자 목록.
            `parser`: 조건을 만족했을 때 결과에 적용할 파서 함수.
            `request_delay`: 요청 간 대기 시간.
            `max_concurrent`: 비동기 요청 시 최대 동시 실행 횟수.
            `tqdm_options`: 진행도를 표시하는 `tqdm`에 전달할 매개변수.
            `loop_options`: `RequestLoop` Task에 전달할 속성."""
        super().__init__(func, context, parser, request_delay, max_concurrent, tqdm_options)
        self.loop_options = loop_options

    @property
    def callable(self) -> Callable:
        if isinstance(self.func, RequestLoop):
            return self.func.run
        else:
            return Request(self.func, self.parser).run

    @property
    def coroutine(self) -> Coroutine:
        if isinstance(self.func, RequestLoop):
            return self.func.run_async
        else:
            return Request(self.func, self.parser).run_async

    def parse(self, parser: Callable | None = None) -> RequestEachLoop:
        """파서 함수를 설정한다."""
        return self.setattr("parser", parser)

    def partial(self, **kwargs: _VT) -> RequestEachLoop:
        """모든 실행에 공통으로 전달할 키워드 인자를 설정한다."""
        return self.setattr("kwargs", kwargs)

    def expand(self, **map_kwargs: _VT) -> RequestEachLoop:
        """여러 인자 목록들의 카테시안 곱을 전개하여 하나의 인자 목록을 설정한다."""
        return super().expand(**map_kwargs)

    def loop(self, condition: Callable[..., bool], **kwargs) -> RequestEachLoop:
        """실행 함수와 파서 함수를 `RequestLoop`로 감싸 재시도 로직을 적용한다."""
        loop = RequestLoop(self.func, condition, self.parser, **(kwargs or self.loop_options))
        return self.setattr("func", loop)

    def concat(self, how: Literal["always", "never", "auto"] = "auto") -> RequestEachLoop:
        """결과 리스트의 병합 방식을 설정한다."""
        return self.setattr("concat_how", how)


###################################################################
############################# Paginate ############################
###################################################################

class PaginateAll(ForEach, Request):
    """전체 페이지를 순회하며 데이터를 수집하는 Task."""

    def __init__(
            self,
            func: Callable | Coroutine,
            counter: Callable[..., int],
            max_page_size: int,
            page_start: int = 1,
            parser: Callable | None = None,
            request_delay: float | int | tuple[int, int] = 0.,
            max_concurrent: int | None = None,
            tqdm_options: dict = dict(),
        ):
        """`PaginateAll` Task 속성을 초기화한다.

        Args:
            `func`: 순차 실행할 함수 또는 병렬로 실행할 코루틴.
            `counter`: 전체 데이터 항목 수를 가져오는 함수. 반드시 정수를 반환해야 한다.
            `max_page_size`: 페이지 내 데이터 항목 수.
            `page_start`: 시작 페이지 번호.
            `parser`: 페이지 별 요청 결과에 적용할 파서 함수.
            `request_delay`: 요청 간 대기 시간.
            `max_concurrent`: 비동기 요청 시 최대 동시 실행 횟수.
            `tqdm_options`: 진행도를 표시하는 `tqdm`에 전달할 매개변수."""
        self.func = func
        self.max_retrieser = counter
        self.max_page_size = max_page_size
        self.page_start = page_start
        self.parser = parser
        self.delay = request_delay
        self.max_concurrent = max_concurrent
        self.tqdm_options = tqdm_options
        self.concat_how = "never"

    def run(self, page: _SKIPPED = None, page_size: _SKIPPED = None, **kwargs) -> list:
        """첫 페이지 요청으로 전체 데이터 항목 수를 파악한 뒤 나머지 페이지를 순차적으로 요청한다."""
        kwargs["page_size"] = self.max_page_size
        results, total_count = self._run_with_count(page=self.page_start, **kwargs)
        if isinstance(total_count, int) and (total_count > self.max_page_size):
            from linkmerce.utils.progress import gather
            func = self._run_without_count
            pages = map(lambda page: {"page": page}, self._generate_next_pages(total_count))
            results = [results] + gather(func, pages, kwargs, self.delay, self.tqdm_options)
            return self._concat_results(results)
        else:
            return [results]

    async def run_async(self, page: _SKIPPED = None, page_size: _SKIPPED = None, **kwargs) -> list:
        """첫 페이지 요청으로 전체 데이터 항목 수를 파악한 뒤 나머지 페이지를 비동기로 병렬 요청한다."""
        kwargs["page_size"] = self.max_page_size
        results, total_count = await self._run_async_with_count(page=self.page_start, **kwargs)
        if isinstance(total_count, int) and (total_count > self.max_page_size):
            from linkmerce.utils.progress import gather_async
            func = self._run_async_without_count
            pages = map(lambda page: {"page": page}, self._generate_next_pages(total_count))
            results = [results] + (await gather_async(func, pages, kwargs, self.delay, self.max_concurrent, self.tqdm_options))
            return self._concat_results(results)
        else:
            return [results]

    def parse(self, parser: Callable | None = None) -> PaginateAll:
        """파서 함수를 설정한다."""
        return self.setattr("parser", parser)

    def concat(self, how: Literal["always", "never", "auto"] = "auto") -> PaginateAll:
        """결과 리스트의 병합 방식을 설정한다."""
        return self.setattr("concat_how", how)

    def _generate_next_pages(self, total_count: int) -> Iterable[int]:
        """전체 데이터 항목 수(`total_count`) / 페이지 내 데이터 항목 수(`max_page_size`) 계산하여 페이지 목록을 반환한다."""
        from math import ceil
        return range(self.page_start + 1, ceil(total_count / self.max_page_size) + self.page_start)

    def _run_with_count(self, **kwargs) -> tuple[Any, int]:
        """첫 번째 페이지를 요청하면서 전체 데이터 항목 수를 카운트해 결과와 같이 반환한다."""
        results = self.func(**kwargs)
        return self._parse(results, kwargs=kwargs), self.max_retrieser(results, **kwargs)

    def _run_without_count(self, **kwargs) -> Any:
        """나머지 페이지를 요청하고 결과를 반환한다."""
        results = self.func(**kwargs)
        return self._parse(results, kwargs=kwargs)

    async def _run_async_with_count(self, **kwargs) -> tuple[Any, int]:
        """첫 번째 페이지를 비동기 요청하면서 전체 데이터 항목 수를 카운트해 결과와 같이 반환한다."""
        results = await self.func(**kwargs)
        return self._parse(results, kwargs=kwargs), self.max_retrieser(results, **kwargs)

    async def _run_async_without_count(self, **kwargs) -> Any:
        """나머지 페이지를 비동기 요청하고 결과를 반환한다."""
        results = await self.func(**kwargs)
        return self._parse(results, kwargs=kwargs)


class RequestEachPages(RequestEach):
    """`Request`, `ForEach`, `PaginateAll`을 결합하여,
    여러 인자 목록에 대해 순차적으로 요청하면서 각 인자별 전체 페이지를 수집하는 Task."""

    def __init__(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT, ...] | dict[_KT, _VT]] | dict[_KT, _VT] = list(),
            parser: Callable | None = None,
            request_delay: float | int | tuple[int, int] = 0.,
            max_concurrent: int | None = None,
            tqdm_options: dict = dict(),
            page_options: dict = {"tqdm_options": {"disable": True}},
        ):
        """`RequestEach` Task 속성과 `PaginateAll` Task 속성을 초기화한다.

        Args:
            `func`: 순차 실행할 함수 또는 병렬로 실행할 코루틴.
            `context`: 함수를 순차 또는 병렬로 실행할 때 전달할 인자 목록.
            `parser`: 페이지 별 요청 결과에 적용할 파서 함수.
            `request_delay`: 요청 간 대기 시간.
            `max_concurrent`: 비동기 요청 시 최대 동시 실행 횟수.
            `tqdm_options`: 진행도를 표시하는 `tqdm`에 전달할 매개변수.
            `page_options`: `PaginateAll` Task에 전달할 속성."""
        super().__init__(func, context, parser, request_delay, max_concurrent, tqdm_options)
        self.page_options = page_options

    @property
    def callable(self) -> Callable:
        if isinstance(self.func, PaginateAll):
            return self.func.run
        else:
            return Request(self.func, self.parser).run

    @property
    def coroutine(self) -> Coroutine:
        if isinstance(self.func, PaginateAll):
            return self.func.run_async
        else:
            return Request(self.func, self.parser).run_async

    def parse(self, parser: Callable | None = None) -> RequestEachPages:
        """파서 함수를 설정한다."""
        return self.setattr("parser", parser)

    def expand(self, **map_kwargs: _VT) -> RequestEachPages:
        """여러 인자 목록들의 카테시안 곱을 전개하여 하나의 인자 목록을 설정한다."""
        return super().expand(**map_kwargs)

    def partial(self, **kwargs: _VT) -> RequestEachPages:
        """모든 실행에 공통으로 전달할 키워드 인자를 설정한다."""
        return self.setattr("kwargs", kwargs)

    def all_pages(self, counter: Callable[..., int], max_page_size: int, page_start: int = 1, page: int | None = None, **kwargs) -> RequestEachPages:
        """페이지네이션을 적용하여 모든 페이지를 순회하도록 설정한다."""
        if page is None:
            paginate_all = PaginateAll(self.func, counter, max_page_size, page_start, self.parser, **(kwargs or self.page_options))
            return self.setattr("func", paginate_all)
        else:
            return self

    def concat(self, how: Literal["always", "never", "auto"] = "auto") -> RequestEachPages:
        """결과 리스트의 병합 방식을 설정한다."""
        if isinstance(self.func, PaginateAll):
            return self.setattr("func", self.func.concat(how), "concat_how", how)
        else:
            return self.setattr("concat_how", how)


###################################################################
############################## Cursor #############################
###################################################################

class CursorAll(RunLoop, ForEach, Request):
    """커서 기반으로 데이터를 수집하는 Task."""

    def __init__(
            self,
            func: Callable,
            get_next_cursor: Callable[..., Any],
            next_cursor: Any | None = None,
            parser: Callable | None = None,
            request_delay: float | int | tuple[int, int] = 0.,
        ):
        """`CursorAll` Task 속성을 초기화한다.

        Args:
            `func`: 순차 실행할 함수 또는 병렬로 실행할 코루틴.
            `get_next_cursor`: 다음 커서를 가져오는 함수.
            `next_cursor`: 시작 커서.
            `parser`: 페이지 별 요청 결과에 적용할 파서 함수.
            `request_delay`: 요청 간 대기 시간."""
        self.func = func
        self.get_next_cursor = get_next_cursor
        self.next_cursor = next_cursor
        self.parser = parser
        self.delay = request_delay
        self.concat_how = "never"

    def run(self, next_cursor: _SKIPPED = None, **kwargs) -> list:
        """다음 커서가 없을 때까지 무한 반복 요청한다."""
        results, next_cursor = list(), self.next_cursor
        while (next_cursor is not None) or (not results):
            result, next_cursor = self._run_with_cursor(next_cursor=next_cursor, **kwargs)
            results.append(result)
            self._sleep(self.delay)
        return self._concat_results(results)

    async def run_async(self):
        """커서 기반 Task는 비동기 요청을 지원하지 않는다."""
        raise NotImplementedError("This task does not support asynchronous execution. Please use the run method instead.")

    def parse(self, parser: Callable | None = None) -> CursorAll:
        """파서 함수를 설정한다."""
        return self.setattr("parser", parser)

    def concat(self, how: Literal["always", "never", "auto"] = "auto") -> CursorAll:
        """결과 리스트의 병합 방식을 설정한다."""
        return self.setattr("concat_how", how)

    def _run_with_cursor(self, **kwargs) -> tuple[Any, Any]:
        """요청하면서 다음 커서를 가져와 결과와 같이 반환한다."""
        results = self.func(**kwargs)
        return self._parse(results, kwargs=kwargs), self.get_next_cursor(results, **kwargs)


class RequestEachCursor(RequestEach):
    """`Request`, `ForEach`, `CursorAll`을 결합하여,
    여러 인자 목록에 대해 순차적으로 요청하면서 각 인자별 다음 커서가 없을 때까지 모든 데이터를 수집하는 Task."""

    def __init__(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT, ...] | dict[_KT, _VT]] | dict[_KT, _VT] = list(),
            parser: Callable | None = None,
            request_delay: float | int | tuple[int, int] = 0.,
            tqdm_options: dict = dict(),
            cursor_options: dict = {"tqdm_options": {"disable": True}},
        ):
        """`RequestEach` Task 속성과 `CursorAll` Task 속성을 초기화한다.

        Args:
            `func`: 순차 실행할 함수 또는 병렬로 실행할 코루틴.
            `context`: 함수를 순차 또는 병렬로 실행할 때 전달할 인자 목록.
            `parser`: 페이지 별 요청 결과에 적용할 파서 함수.
            `request_delay`: 요청 간 대기 시간.
            `tqdm_options`: 진행도를 표시하는 `tqdm`에 전달할 매개변수.
            `cursor_options`: `CursorAll` Task에 전달할 속성."""
        super().__init__(func, context, parser, request_delay, None, tqdm_options)
        self.cursor_options = cursor_options

    @property
    def callable(self) -> Callable:
        if isinstance(self.func, CursorAll):
            return self.func.run
        else:
            return Request(self.func, self.parser).run

    async def run_async(self):
        """커서 기반 Task는 비동기 요청을 지원하지 않는다."""
        raise NotImplementedError("This task does not support asynchronous execution. Please use the run method instead.")

    def parse(self, parser: Callable | None = None) -> RequestEachCursor:
        """파서 함수를 설정한다."""
        return self.setattr("parser", parser)

    def expand(self, **map_kwargs: _VT) -> RequestEachCursor:
        """여러 인자 목록들의 카테시안 곱을 전개하여 하나의 인자 목록을 설정한다."""
        return super().expand(**map_kwargs)

    def partial(self, **kwargs: _VT) -> RequestEachCursor:
        """모든 실행에 공통으로 전달할 키워드 인자를 설정한다."""
        return self.setattr("kwargs", kwargs)

    def all_cursor(self, get_next_cursor: Callable[..., Any], next_cursor: Any | None = None, **kwargs) -> RequestEachCursor:
        """커서 기반 순회를 적용하여 모든 데이터를 수집하도록 설정한다."""
        cursor_all = CursorAll(self.func, get_next_cursor, next_cursor, self.parser, **(kwargs or self.cursor_options))
        return self.setattr("func", cursor_all)

    def concat(self, how: Literal["always", "never", "auto"] = "auto") -> RequestEachCursor:
        """결과 리스트의 병합 방식을 설정한다."""
        if isinstance(self.func, PaginateAll):
            return self.setattr("func", self.func.concat(how), "concat_how", how)
        else:
            return self.setattr("concat_how", how)
