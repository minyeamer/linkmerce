from __future__ import annotations

from abc import ABCMeta, abstractmethod

from typing import Any, Hashable, Iterable, Sequence, TypeVar, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Callable, Coroutine, Literal
    _SKIPPED = TypeVar("_SKIPPED", bound=None)

_KT = TypeVar("_KT", bound=Hashable)
_VT = TypeVar("_VT", bound=Any)
TaskOption = dict[_KT,_VT]
TaskOptions = dict[_KT,TaskOption]


class Task(metaclass=ABCMeta):
    @abstractmethod
    def run(self):
        raise NotImplementedError("This task does not support synchronous execution. Please use the run_async method instead.")

    async def run_async(self):
        raise NotImplementedError("This task does not support asynchronous execution. Please use the run method instead.")

    def setattr(self, name: str, value: _VT, *args: str | _VT) -> Task:
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
    def __init__(self, func: Callable | Coroutine, parser: Callable | None = None):
        self.func = func
        self.parser = parser

    def run(self, *args, **kwargs) -> Any:
        result = self.func(*args, **kwargs)
        return self._parse(result, args, kwargs)

    async def run_async(self, *args, **kwargs) -> Any:
        result = await self.func(*args, **kwargs)
        return self._parse(result, args, kwargs)

    def parse(self, parser: Callable | None = None) -> Request:
        return self.setattr("parser", parser)

    def _parse(self, result: Any, args: tuple = tuple(), kwargs: dict = dict()) -> dict:
        return self.parser(result, *args, **kwargs) if self.parser is not None else result


###################################################################
############################# Run Loop ############################
###################################################################

class RunLoop(Task):
    def __init__(
            self,
            func: Callable | Coroutine,
            condition: Callable[...,bool],
            max_retries: int | None = 1,
            delay: Literal["incremental"] | float | int | Sequence[int,int] = "incremental",
            raise_errors: type | Sequence[type] = tuple(),
            ignored_errors: type | Sequence[type] = tuple(),
        ):
        self.func = func
        self.condition = condition
        self.max_retries = max_retries
        self.delay = delay
        self.raise_errors = raise_errors
        self.ignored_errors = ignored_errors

    def run(self, *args, **kwargs) -> Any:
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
        retry_count = 1
        while True:
            result = self.func(*args, **kwargs)
            if self.condition(result):
                return result
            else:
                self._sleep(retry_count)
                retry_count += 1

    def _sleep(self, retry_count: int):
        import time
        if self.delay == "incremental":
            time.sleep(retry_count)
        else:
            from linkmerce.utils.progress import _get_seconds
            time.sleep(_get_seconds(self.delay))

    async def _sleep_async(self, retry_count: int):
        import asyncio
        if self.delay == "incremental":
            await asyncio.sleep(retry_count)
        else:
            from linkmerce.utils.progress import _get_seconds
            await asyncio.sleep(_get_seconds(self.delay))

    def _raise_loop_error(self):
        raise RuntimeError("Exceeded maximum retry attempts without success.")


class RequestLoop(RunLoop, Request):
    def __init__(
            self,
            func: Callable | Coroutine,
            condition: Callable[...,bool],
            parser: Callable | None = None,
            max_retries: int | None = 1,
            request_delay: Literal["incremental"] | float | int | Sequence[int,int] = "incremental",
            raise_errors: type | Sequence[type] = tuple(),
            ignored_errors: type | Sequence[type] = tuple(),
        ):
        RunLoop.__init__(self, func, condition, max_retries, request_delay, raise_errors, ignored_errors)
        self.parser = parser

    def run(self, *args, **kwargs) -> Any:
        result = super().run(*args, **kwargs)
        if result is not None:
            return self._parse(result, args, kwargs)

    async def run_async(self, *args, **kwargs) -> Any:
        result = await super().run_async(*args, **kwargs)
        if result is not None:
            return self._parse(result, args, kwargs)

    def parse(self, parser: Callable | None = None) -> RequestLoop:
        return self.setattr("parser", parser)


###################################################################
############################# For Each ############################
###################################################################

class ForEach(Task):
    def __init__(
            self,
            func: Callable | Coroutine,
            array: Sequence[tuple[_VT,...] | dict[_KT,_VT]] = list(),
            delay: float | int | tuple[int,int] = 0.,
            max_concurrent: int | None = None,
            tqdm_options: dict = dict(),
        ):
        self.func = func
        self.array = array
        self.delay = delay
        self.max_concurrent = max_concurrent
        self.tqdm_options = tqdm_options
        self.kwargs = dict()
        self.concat_how = "auto"

    def run(self) -> list:
        from linkmerce.utils.progress import gather
        results = gather(self.func, self.array, self.kwargs, self.delay, self.tqdm_options)
        return self._concat_results(results)

    async def run_async(self) -> list:
        from linkmerce.utils.progress import gather_async
        results = await gather_async(self.func, self.array, self.kwargs, self.delay, self.max_concurrent, self.tqdm_options)
        return self._concat_results(results)

    def expand(self, **map_kwargs: Iterable[_VT]) -> ForEach:
        from linkmerce.utils.progress import _expand_kwargs
        array = _expand_kwargs(**map_kwargs)
        return self.setattr("array", array)

    def partial(self, **kwargs: _VT) -> ForEach:
        return self.setattr("kwargs", kwargs)

    def concat(self, how: Literal["always","never","auto"] = "auto") -> ForEach:
        return self.setattr("concat_how", how)

    def _concat_results(self, results: list) -> list:
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
    def __init__(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT,...] | dict[_KT,_VT]] | dict[_KT,_VT] = list(),
            parser: Callable | None = None,
            request_delay: float | int | tuple[int,int] = 0.,
            max_concurrent: int | None = None,
            tqdm_options: dict = dict(),
        ):
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
        if isinstance(self.context, Sequence):
            from linkmerce.utils.progress import gather
            results = gather(self.callable, self.context, self.kwargs, self.delay, self.tqdm_options)
            return self._concat_results(results)
        elif isinstance(self.context, dict):
            return self.callable(**self.context, **self.kwargs)
        else:
            self._raise_context_error()

    async def run_async(self) -> list | Any:
        if isinstance(self.context, Sequence):
            from linkmerce.utils.progress import gather_async
            results = await gather_async(self.coroutine, self.context, self.kwargs, self.delay, self.max_concurrent, self.tqdm_options)
            return self._concat_results(results)
        elif isinstance(self.context, dict):
            return await self.coroutine(**self.context, **self.kwargs)
        else:
            self._raise_context_error()

    def parse(self, parser: Callable | None = None) -> RequestEach:
        return self.setattr("parser", parser)

    def partial(self, **kwargs: _VT) -> RequestEach:
        return self.setattr("kwargs", kwargs)

    def expand(self, **map_kwargs: _VT) -> RequestEach:
        mapping, partial = self._split_map_kwargs(map_kwargs)
        context = self._expand_context(mapping) if mapping else self.context
        return self.setattr("context", (context or dict()), "kwargs", dict(self.kwargs, **partial))

    def concat(self, how: Literal["always","never","auto"] = "auto") -> RequestEach:
        return self.setattr("concat_how", how)

    def _split_map_kwargs(self, map_kwargs: dict[_KT,_VT]) -> tuple[dict[_KT,_VT], dict[_KT,_VT]]:
        sequential, non_sequential = dict(), self.kwargs.copy()
        for key, value in map_kwargs.items():
            if (not isinstance(value, str)) and isinstance(value, Sequence):
                sequential[key] = value
            else:
                non_sequential[key] = value
        return sequential, non_sequential

    def _expand_context(self, mapping: dict[_KT,Sequence]) -> Sequence[dict[_KT,_VT]]:
        from linkmerce.utils.progress import _expand_kwargs
        context = self._get_sequential_context()
        if context:
            context = _expand_kwargs(context_=context, **mapping)
            unpack = lambda context_, **kwargs: dict(context_, **kwargs)
            return [unpack(**kwargs) for kwargs in context]
        else:
            return _expand_kwargs(**mapping)

    def _get_sequential_context(self) -> list[dict[_KT,_VT]]:
        if self.context:
            if isinstance(self.context, Sequence) and all(map(lambda x: isinstance(x, dict), self.context)):
                return self.context
            elif isinstance(self.context, dict):
                return [self.context]
        return list()

    def _raise_context_error(self):
        raise ValueError("Invalid type for context. Context must be a sequence or a dict.")


class RequestEachLoop(RequestEach):
    def __init__(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT,...] | dict[_KT,_VT]] | dict[_KT,_VT] = list(),
            parser: Callable | None = None,
            request_delay: float | int | tuple[int,int] = 0.,
            max_concurrent: int | None = None,
            tqdm_options: dict = dict(),
            loop_options: dict = dict(),
        ):
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
        return self.setattr("parser", parser)

    def partial(self, **kwargs: _VT) -> RequestEachLoop:
        return self.setattr("kwargs", kwargs)

    def expand(self, **map_kwargs: _VT) -> RequestEachLoop:
        return super().expand(**map_kwargs)

    def loop(self, condition: Callable[...,bool], **kwargs) -> RequestEachLoop:
        loop = RequestLoop(self.func, condition, self.parser, **(kwargs or self.loop_options))
        return self.setattr("func", loop)

    def concat(self, how: Literal["always","never","auto"] = "auto") -> RequestEachLoop:
        return self.setattr("concat_how", how)


###################################################################
############################# Paginate ############################
###################################################################

class PaginateAll(ForEach, Request):
    def __init__(
            self,
            func: Callable | Coroutine,
            counter: Callable[...,int],
            max_page_size: int,
            page_start: int = 1,
            parser: Callable | None = None,
            request_delay: float | int | tuple[int,int] = 0.,
            max_concurrent: int | None = None,
            tqdm_options: dict = dict(),
        ):
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
        kwargs["page_size"] = self.max_page_size
        results, total_count = self._run_with_count(page=self.page_start, **kwargs)
        print("[TOTAL]", total_count)
        if isinstance(total_count, int) and (total_count > self.max_page_size):
            from linkmerce.utils.progress import gather
            func = self._run_without_count
            pages = map(lambda page: dict(page=page), self._generate_next_pages(total_count))
            results = [results] + gather(func, pages, kwargs, self.delay, self.tqdm_options)
            return self._concat_results(results)
        else:
            return [results]

    async def run_async(self, page: _SKIPPED = None, page_size: _SKIPPED = None, **kwargs) -> list:
        kwargs["page_size"] = self.max_page_size
        results, total_count = await self._run_async_with_count(page=self.page_start, **kwargs)
        if isinstance(total_count, int) and (total_count > self.max_page_size):
            from linkmerce.utils.progress import gather_async
            func = self._run_async_without_count
            pages = map(lambda page: dict(page=page), self._generate_next_pages(total_count))
            results = [results] + (await gather_async(func, pages, kwargs, self.delay, self.max_concurrent, self.tqdm_options))
            return self._concat_results(results)
        else:
            return [results]

    def parse(self, parser: Callable | None = None) -> PaginateAll:
        return self.setattr("parser", parser)

    def concat(self, how: Literal["always","never","auto"] = "auto") -> PaginateAll:
        return self.setattr("concat_how", how)

    def _generate_next_pages(self, total_count: int) -> Iterable[int]:
        from math import ceil
        return range(self.page_start + 1, ceil(total_count / self.max_page_size) + self.page_start)

    def _run_with_count(self, **kwargs) -> tuple[Any,int]:
        results = self.func(**kwargs)
        return self._parse(results, kwargs=kwargs), self.max_retrieser(results, **kwargs)

    def _run_without_count(self, **kwargs) -> Any:
        results = self.func(**kwargs)
        return self._parse(results, kwargs=kwargs)

    async def _run_async_with_count(self, **kwargs) -> tuple[Any,int]:
        results = await self.func(**kwargs)
        return self._parse(results, kwargs=kwargs), self.max_retrieser(results, **kwargs)

    async def _run_async_without_count(self, **kwargs) -> Any:
        results = await self.func(**kwargs)
        return self._parse(results, kwargs=kwargs)


class RequestEachPages(RequestEach):
    def __init__(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT,...] | dict[_KT,_VT]] | dict[_KT,_VT] = list(),
            parser: Callable | None = None,
            request_delay: float | int | tuple[int,int] = 0.,
            max_concurrent: int | None = None,
            tqdm_options: dict = dict(),
            page_options: dict = dict(tqdm_options=dict(disable=True)),
        ):
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
        return self.setattr("parser", parser)

    def expand(self, **map_kwargs: _VT) -> RequestEachPages:
        return super().expand(**map_kwargs)

    def partial(self, **kwargs: _VT) -> RequestEachPages:
        return self.setattr("kwargs", kwargs)

    def all_pages(self, counter: Callable[...,int], max_page_size: int, page_start: int = 1, page: int | None = None, **kwargs) -> RequestEachPages:
        if page is None:
            paginate_all = PaginateAll(self.func, counter, max_page_size, page_start, self.parser, **(kwargs or self.page_options))
            return self.setattr("func", paginate_all)
        else:
            return self

    def concat(self, how: Literal["always","never","auto"] = "auto") -> RequestEachPages:
        if isinstance(self.func, PaginateAll):
            return self.setattr("func", self.func.concat(how), "concat_how", how)
        else:
            return self.setattr("concat_how", how)


###################################################################
############################## Cursor #############################
###################################################################

class CursorAll(RunLoop, ForEach, Request):
    def __init__(
            self,
            func: Callable,
            get_next_cursor: Callable[...,Any],
            next_cursor: Any | None = None,
            parser: Callable | None = None,
            request_delay: float | int | tuple[int,int] = 0.,
        ):
        self.func = func
        self.get_next_cursor = get_next_cursor
        self.next_cursor = next_cursor
        self.parser = parser
        self.delay = request_delay
        self.concat_how = "never"

    def run(self, next_cursor: _SKIPPED = None, **kwargs) -> list:
        results, next_cursor = list(), self.next_cursor
        while (next_cursor is not None) or (not results):
            result, next_cursor = self._run_with_cursor(next_cursor=next_cursor, **kwargs)
            results.append(result)
            self._sleep(self.delay)
        return self._concat_results(results)

    async def run_async(self):
        raise NotImplementedError("This task does not support asynchronous execution. Please use the run method instead.")

    def parse(self, parser: Callable | None = None) -> CursorAll:
        return self.setattr("parser", parser)

    def concat(self, how: Literal["always","never","auto"] = "auto") -> CursorAll:
        return self.setattr("concat_how", how)

    def _run_with_cursor(self, **kwargs) -> tuple[Any,Any]:
        results = self.func(**kwargs)
        return self._parse(results, kwargs=kwargs), self.get_next_cursor(results, **kwargs)


class RequestEachCursor(RequestEach):
    def __init__(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT,...] | dict[_KT,_VT]] | dict[_KT,_VT] = list(),
            parser: Callable | None = None,
            request_delay: float | int | tuple[int,int] = 0.,
            tqdm_options: dict = dict(),
            cursor_options: dict = dict(tqdm_options=dict(disable=True)),
        ):
        super().__init__(func, context, parser, request_delay, None, tqdm_options)
        self.cursor_options = cursor_options

    @property
    def callable(self) -> Callable:
        if isinstance(self.func, CursorAll):
            return self.func.run
        else:
            return Request(self.func, self.parser).run

    async def run_async(self):
        raise NotImplementedError("This task does not support asynchronous execution. Please use the run method instead.")

    def parse(self, parser: Callable | None = None) -> RequestEachCursor:
        return self.setattr("parser", parser)

    def expand(self, **map_kwargs: _VT) -> RequestEachCursor:
        return super().expand(**map_kwargs)

    def partial(self, **kwargs: _VT) -> RequestEachCursor:
        return self.setattr("kwargs", kwargs)

    def all_cursor(self, get_next_cursor: Callable[...,Any], next_cursor: Any | None = None, **kwargs) -> RequestEachCursor:
        cursor_all = CursorAll(self.func, get_next_cursor, next_cursor, self.parser, **(kwargs or self.cursor_options))
        return self.setattr("func", cursor_all)

    def concat(self, how: Literal["always","never","auto"] = "auto") -> RequestEachCursor:
        if isinstance(self.func, PaginateAll):
            return self.setattr("func", self.func.concat(how), "concat_how", how)
        else:
            return self.setattr("concat_how", how)
