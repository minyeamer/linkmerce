from __future__ import annotations

from abc import ABCMeta, abstractmethod

from typing import Callable, Iterable, Protocol, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Coroutine, Hashable, Literal, TypeVar
    _KT = TypeVar("_KT", Hashable)
    _VT = TypeVar("_VT", Any)
    _SKIPPED = TypeVar("_SKIPPED", None)


def _is_non_string_sequence(object_: Any) -> bool:
    return (not isinstance(object_, str)) and isinstance(object_, Sequence)


class Task(Protocol, metaclass=ABCMeta):
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
    def __init__(self, func: Callable | Coroutine):
        self.func = func
        self.parser = None
        self.parse_kwargs = dict()

    def run(self, *args, **kwargs) -> Any:
        result = self.func(*args, **kwargs)
        return self._parse_result(result, *args, **kwargs, **self.parse_kwargs)

    async def run_async(self, *args, **kwargs) -> Any:
        result = await self.func(*args, **kwargs)
        return self._parse_result(result, *args, **kwargs, **self.parse_kwargs)

    def parse(self, parser: Callable | None = None, **kwargs) -> Request:
        return self.setattr("parser", parser, "parse_kwargs", kwargs)

    def _parse_result(self, result: Any, *args, **kwargs) -> Any:
        if self.parser is not None:
            return self.parser(result, *args, **kwargs, **self.parse_kwargs)
        else:
            return result


###################################################################
############################# Run Loop ############################
###################################################################

class RunLoop(Task):
    def __init__(
            self,
            func: Callable | Coroutine,
            condition: Callable[...,bool],
            count: int | None = 1,
            delay: Literal["incremental"] | float | int | Sequence[int,int] = "incremental",
            loop_error: type = RuntimeError,
        ):
        self.func = func
        self.condition = condition
        self.count = count
        self.delay = delay
        self.loop_error = loop_error

    def run(self, *args, **kwargs) -> Any:
        if not isinstance(self.count, int):
            return self._infinite_run(*args, **kwargs)
        for count in range(1, self.count+1):
            result = self.func(*args, **kwargs)
            if self.condition(result):
                return result
            else:
                self.sleep(count)
        raise self.loop_error("Exceeded maximum retry attempts without success.")

    async def run_async(self, *args, **kwargs) -> Any:
        if not isinstance(self.count, int):
            raise self.loop_error("Invalid loop count provided.")
        for count in range(1, self.count+1):
            result = await self.func(*args, **kwargs)
            if self.condition(result):
                return result
            else:
                await self.sleep_async(count)
        raise self.loop_error("Exceeded maximum retry attempts without success.")

    def _infinite_run(self, *args, **kwargs) -> Any:
        count = 1
        while True:
            result = self.func(*args, **kwargs)
            if self.condition(result):
                return result
            else:
                self.sleep(count)
                count += 1

    def sleep(self, count: int):
        import time
        if self.delay == "incremental":
            time.sleep(count)
        else:
            from linkmerce.utils.tqdm import _get_seconds
            time.sleep(_get_seconds(self.delay))

    async def sleep_async(self, count: int):
        import asyncio
        if self.delay == "incremental":
            await asyncio.sleep(count)
        else:
            from linkmerce.utils.tqdm import _get_seconds
            await asyncio.sleep(_get_seconds(self.delay))


class RequestLoop(RunLoop, Request):
    def __init__(
            self,
            func: Callable | Coroutine,
            condition: Callable[...,bool],
            count: int | None = 1,
            delay: Literal["incremental"] | float | int | Sequence[int,int] = "incremental",
            loop_error: type = RuntimeError,
        ):
        RunLoop.__init__(self, func, condition, count, delay, loop_error)
        self.parser = None
        self.parse_kwargs = dict()

    def run(self, *args, **kwargs) -> Any:
        if not isinstance(self.count, int):
            return self._infinite_run(*args, **kwargs)
        for count in range(1, self.count+1):
            result = self.func(*args, **kwargs)
            if self.condition(result):
                return self._parse_result(result, *args, **kwargs)
            else:
                print(count)
                self.sleep(count)
        raise self.loop_error("Exceeded maximum retry attempts without success.")

    async def run_async(self, *args, **kwargs) -> Any:
        if not isinstance(self.count, int):
            raise self.loop_error("Invalid loop count provided.")
        for count in range(1, self.count+1):
            result = await self.func(*args, **kwargs)
            if self.condition(result):
                return self._parse_result(result, *args, **kwargs)
            else:
                await self.sleep_async(count)
        raise self.loop_error("Exceeded maximum retry attempts without success.")

    def parse(self, parser: Callable | None = None, **kwargs) -> RequestLoop:
        return self.setattr("parser", parser, "parse_kwargs", kwargs)


###################################################################
############################# For Each ############################
###################################################################

class ForEach(Task):
    def __init__(
            self,
            func: Callable | Coroutine,
            array: Sequence[tuple[_VT,...] | dict[_KT,_VT]] = list(),
            delay: float | int | tuple[int,int] = 0.,
            limit: int | None = None,
            tqdm_options: dict = dict(),
        ):
        self.func = func
        self.array = array
        self.delay = delay
        self.limit = limit
        self.tqdm_options = tqdm_options
        self.kwargs = dict()
        self.concat_how = "never"

    def run(self) -> list:
        from linkmerce.utils.tqdm import gather
        results = gather(self.func, self.array, self.kwargs, self.delay, self.tqdm_options)
        return self._concat_results(results)

    async def run_async(self) -> list:
        from linkmerce.utils.tqdm import gather_async
        results = await gather_async(self.func, self.array, self.kwargs, self.delay, self.limit, self.tqdm_options)
        return self._concat_results(results)

    def expand(self, **map_kwargs: Iterable[_VT]) -> ForEach:
        array = self._expand_kwargs(**map_kwargs)
        return self.setattr("array", array)

    def partial(self, **kwargs: _VT) -> ForEach:
        return self.setattr("kwargs", kwargs)

    def concat(self, how: Literal["always","never","auto"] = "auto") -> ForEach:
        return self.setattr("concat_how", how)

    def _concat_results(self, results: list) -> list:
        if (self.concat_how == "always") or ((self.concat_how == "auto") and all(map(lambda x: isinstance(x, Sequence), results))):
            from itertools import chain
            return list(chain.from_iterable(results))
        else:
            return results

    def _expand_kwargs(self, **map_kwargs: Iterable[_VT]) -> list[dict[_KT,_VT]]:
        from itertools import product
        keys = map_kwargs.keys()
        return [dict(zip(keys, values)) for values in product(*map_kwargs.values())]


class RequestEach(ForEach, Request):
    def __init__(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT,...] | dict[_KT,_VT]] | dict[_KT,_VT] = list(),
            delay: float | int | tuple[int,int] = 0.,
            limit: int | None = None,
            loop_options: dict = dict(),
            tqdm_options: dict = dict(),
        ):
        self.func = func
        self.context = context
        self.delay = delay
        self.limit = limit
        self.loop_options = loop_options
        self.tqdm_options = tqdm_options
        self.kwargs = dict()
        self.parser = None
        self.parse_kwargs = dict()
        self.concat_how = "never"

    def run(self) -> list | Any:
        if isinstance(self.context, Sequence):
            from linkmerce.utils.tqdm import gather
            results = gather(self._get_func(), self.context, self.kwargs, self.delay, self.tqdm_options)
            return self._concat_results(results)
        elif isinstance(self.context, dict):
            return self._get_func()(**self.context, **self.kwargs)
        else:
            raise ValueError("Invalid type for context. Context must be a sequence or a dict.")

    async def run_async(self) -> list | Any:
        if isinstance(self.context, Sequence):
            from linkmerce.utils.tqdm import gather_async
            results = await gather_async(self._get_async_func(), self.context, self.kwargs, self.delay, self.limit, self.tqdm_options)
            return self._concat_results(results)
        elif isinstance(self.context, dict):
            return await self._get_async_func()(**self.context, **self.kwargs)
        else:
            raise ValueError("Invalid type for context. Context must be a sequence or a dict.")

    def expand(self, **map_kwargs: _VT) -> RequestEach:
        mapping, partial = self._split_map_kwargs(map_kwargs)
        context = self._expand_context(mapping) if mapping else self.context
        return self.setattr("context", (context or dict()), "kwargs", dict(self.kwargs, **partial))

    def partial(self, **kwargs: _VT) -> RequestEach:
        return self.setattr("kwargs", kwargs)

    def parse(self, parser: Callable | None = None, **kwargs) -> RequestEach:
        return self.setattr("parser", parser, "parse_kwargs", kwargs)

    def loop(self, condition: Callable[...,bool], **kwargs) -> RequestEach:
        loop = RequestLoop(self.func, condition=condition, **(kwargs or self.loop_options))
        return self.setattr("func", loop.parse(self.parser, **self.parse_kwargs))

    def concat(self, how: Literal["always","never","auto"] = "auto") -> RequestEach:
        return self.setattr("concat_how", how)

    def _get_func(self) -> Callable:
        if isinstance(self.func, RequestLoop):
            return self.func.run
        else:
            return Request(self.func).parse(self.parser, **self.parse_kwargs).run

    def _get_async_func(self) -> Coroutine:
        if isinstance(self.func, RequestLoop):
            return self.func.run_async
        else:
            return Request(self.func).parse(self.parser, **self.parse_kwargs).run_async

    def _split_map_kwargs(self, map_kwargs: dict[_KT,_VT]) -> tuple[dict[_KT,_VT], dict[_KT,_VT]]:
        sequential, non_sequential = dict(), self.kwargs.copy()
        for key, value in map_kwargs.items():
            if _is_non_string_sequence(value):
                sequential[key] = value
            else:
                non_sequential[key] = value
        return sequential, non_sequential

    def _expand_context(self, mapping: dict[_KT,Sequence]) -> Sequence[dict[_KT,_VT]]:
        context = self._get_context_to_expand()
        if context:
            context = self._expand_kwargs(context_=context, **mapping)
            unpack = lambda context_, **kwargs: dict(context_, **kwargs)
            return [unpack(**kwargs) for kwargs in context]
        else:
            return self._expand_kwargs(**mapping)

    def _get_context_to_expand(self) -> list[dict[_KT,_VT]]:
        if self.context:
            if isinstance(self.context, Sequence) and all(map(lambda x: isinstance(x, dict), self.context)):
                return self.context
            elif isinstance(self.context, dict):
                return [self.context]
        return list()


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
            delay: float | int | tuple[int,int] = 0.,
            limit: int | None = None,
            tqdm_options: dict = dict(),
            count_error: type = ValueError,
        ):
        self.func = func
        self.counter = counter
        self.max_page_size = max_page_size
        self.page_start = page_start
        self.delay = delay
        self.limit = limit
        self.tqdm_options = tqdm_options
        self.count_error = count_error
        self.parser = None
        self.parse_kwargs = dict()
        self.concat_how = "never"

    def run(self, page: _SKIPPED = None, page_size: _SKIPPED = None, **kwargs) -> list:
        kwargs["page_size"] = self.max_page_size
        results, total_count = self._run_with_count(page=self.page_start, **kwargs)
        if isinstance(total_count, int) and (total_count > self.max_page_size):
            from linkmerce.utils.tqdm import gather
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
            from linkmerce.utils.tqdm import gather_async
            func = self._run_async_without_count
            pages = map(lambda page: dict(page=page), self._generate_next_pages(total_count))
            results = [results] + (await gather_async(func, pages, kwargs, self.delay, self.limit, self.tqdm_options))
            return self._concat_results(results)
        else:
            return [results]

    def parse(self, parser: Callable | None = None, **kwargs) -> PaginateAll:
        return self.setattr("parser", parser, "parse_kwargs", kwargs)

    def concat(self, how: Literal["always","never","auto"] = "auto") -> PaginateAll:
        return self.setattr("concat_how", how)

    def _generate_next_pages(self, total_count: int) -> Iterable[int]:
        from math import ceil
        return range(self.page_start + 1, ceil(total_count / self.max_page_size))

    def _run_with_count(self, *args, **kwargs) -> tuple[Any,int]:
        results = self.func(*args, **kwargs)
        return self._parse_result(results, *args, **kwargs, **self.parse_kwargs), self.counter(results)

    def _run_without_count(self, *args, **kwargs) -> Any:
        results = self.func(*args, **kwargs)
        return self._parse_result(results, *args, **kwargs, **self.parse_kwargs)

    async def _run_async_with_count(self, *args, **kwargs) -> tuple[Any,int]:
        results = await self.func(*args, **kwargs)
        return self._parse_result(results, *args, **kwargs, **self.parse_kwargs), self.counter(results)

    async def _run_async_without_count(self, *args, **kwargs) -> Any:
        results = await self.func(*args, **kwargs)
        return self._parse_result(results, *args, **kwargs, **self.parse_kwargs)

    def _parse_result(self, results: Any, *args, **kwargs) -> Any:
        if self.parser is not None:
            return self.parser(results, *args, **kwargs, **self.parse_kwargs)
        else:
            return results


class RequestEachPages(RequestEach):
    def __init__(
            self,
            func: Callable | Coroutine,
            context: Sequence[tuple[_VT,...] | dict[_KT,_VT]] | dict[_KT,_VT] = list(),
            delay: float | int | tuple[int,int] = 0.,
            limit: int | None = None,
            loop_options: dict = dict(tqdm_options=dict(disable=True)),
            tqdm_options: dict = dict(),
        ):
        super().__init__(func, context, delay, limit, loop_options, tqdm_options)

    def expand(self, **map_kwargs: _VT) -> RequestEachPages:
        return super().expand(**map_kwargs)

    def partial(self, **kwargs: _VT) -> RequestEachPages:
        return self.setattr("kwargs", kwargs)

    def parse(self, parser: Callable | None = None, **kwargs) -> RequestEachPages:
        return self.setattr("parser", parser, "parse_kwargs", kwargs)

    def loop(self, counter: Callable[...,int], max_page_size: int, page_start: int = 1, **kwargs) -> RequestEachPages:
        loop = PaginateAll(self.func, counter, max_page_size, page_start, **(kwargs or self.loop_options))
        return self.setattr("func", loop.parse(self.parser, **self.parse_kwargs))

    def concat(self, how: Literal["always","never","auto"] = "auto") -> RequestEachPages:
        if isinstance(self.func, PaginateAll):
            return self.setattr("func", self.func.concat(how), "concat_how", how)
        else:
            return self.setattr("concat_how", how)

    def _get_func(self) -> Callable:
        if isinstance(self.func, PaginateAll):
            return self.func.run
        else:
            return Request(self.func).parse(self.parser, **self.parse_kwargs).run

    def _get_async_func(self) -> Coroutine:
        if isinstance(self.func, PaginateAll):
            return self.func.run_async
        else:
            return Request(self.func).parse(self.parser, **self.parse_kwargs).run_async
