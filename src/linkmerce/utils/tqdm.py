from __future__ import annotations

from typing import Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Callable, Coroutine, Hashable, Iterable, TypeVar
    from types import ModuleType
    from numbers import Real
    _KT = TypeVar("_KT", Hashable)
    _VT = TypeVar("_VT", Any)


def import_tqdm() -> ModuleType:
    try:
        from tqdm import tqdm
        return tqdm
    except:
        return lambda x, **kwargs: x


def import_tqdm_asyncio() -> ModuleType:
    try:
        from tqdm.asyncio import tqdm_asyncio
        return tqdm_asyncio
    except:
        import asyncio
        return asyncio


###################################################################
############################## Gather #############################
###################################################################

def gather(
        func: Callable[...,Any],
        arr_args: Iterable[tuple[_VT,...] | dict[_KT,_VT]],
        partial: dict[_KT,_VT] = dict(),
        delay: Real | Sequence[Real,Real] = 0.,
        tqdm_options: dict = dict(),
    ) -> list:
    import time
    try:
        from tqdm import tqdm
    except:
        tqdm = lambda x: x
        tqdm_options = dict()

    def run_with_delay(args: tuple[_VT,...] | dict[_KT,_VT]) -> Any:
        try:
            if isinstance(args, dict):
                return func(**args, **partial)
            else:
                return func(*args, **partial)
        finally: time.sleep(_get_seconds(delay))

    return [run_with_delay(args) for args in tqdm(arr_args, **tqdm_options)]


async def gather_async(
        func: Coroutine,
        arr_args: Iterable[tuple[_VT,...] | dict[_KT,_VT]],
        partial: dict[_KT,_VT] = dict(),
        delay: Real | Sequence[Real,Real] = 0.,
        limit: int | None = None,
        tqdm_options: dict = dict(),
    ) -> list:
    import asyncio
    try:
        from tqdm.asyncio import tqdm_asyncio
    except:
        tqdm_asyncio = asyncio
        tqdm_options = dict()

    async def run_with_delay(args: tuple[_VT,...] | dict[_KT,_VT]) -> Any:
        try:
            if isinstance(args, dict):
                return await func(**args, **partial)
            else:
                return await func(*args, **partial)
        finally: await asyncio.sleep(_get_seconds(delay))

    async def run_with_delay_and_limit(semaphore, args: tuple[_VT,...] | dict[_KT,_VT]) -> Any:
        if semaphore is not None:
            async with semaphore:
                return await run_with_delay(args)
        else:
            return await run_with_delay(args)

    semaphore = asyncio.Semaphore(limit) if isinstance(limit, int) else None
    return await tqdm_asyncio.gather(*[run_with_delay_and_limit(semaphore, args) for args in arr_args], **tqdm_options)


def _get_seconds(value: Real | Sequence[Real,Real]) -> Real:
    if isinstance(value, (float,int)):
        return value
    elif isinstance(value, Sequence) and (len(value) > 1):
        import random
        start, end = value[0] * 1000, value[1] * 1000
        return random.randrange(int(start), int(end), step=1) / 1000
    else: return 0.


###################################################################
############################## Expand #############################
###################################################################

def expand(
        func: Callable[...,Any],
        mapping: dict[_KT,Iterable[_VT]],
        partial: dict[_KT,_VT] = dict(),
        delay: Real | Sequence[Real,Real] = 0.,
        tqdm_options: dict = dict()
    ) -> list:
    return gather(func, _expand_kwargs(**mapping), partial, delay, tqdm_options)


async def expand_async(
        func: Coroutine,
        mapping: dict[_KT,Iterable[_VT]],
        partial: dict[_KT,_VT] = dict(),
        delay: Real | Sequence[Real,Real] = 0.,
        limit: int | None = None,
        tqdm_options: dict = dict()
    ) -> list:
    return await gather_async(func, _expand_kwargs(**mapping), partial, delay, limit, tqdm_options)


def _expand_kwargs(**map_kwargs: Iterable[_VT]) -> list[dict[_KT,_VT]]:
    from itertools import product
    keys = map_kwargs.keys()
    return [dict(zip(keys, values)) for values in product(*map_kwargs.values())]
