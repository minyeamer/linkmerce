from __future__ import annotations

from typing import Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Callable, Coroutine, Hashable, Iterable, TypeVar
    from types import ModuleType
    from numbers import Real
    _KT = TypeVar("_KT", Hashable)
    _VT = TypeVar("_VT", Any)


def import_tqdm() -> ModuleType:
    """`tqdm` 모듈을 반환한다. 설치되지 않았으면 인자를 그대로 내보내는 람다 함수를 반환한다."""
    try:
        from tqdm import tqdm
        return tqdm
    except:
        return lambda x, **kwargs: x


def import_tqdm_asyncio() -> ModuleType:
    """`tqdm.asyncio` 모듈을 반환한다. 설치되지 않았으면 `asyncio` 모듈을 반환한다."""
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
        func: Callable[..., Any],
        arr_args: Iterable[tuple[_VT, ...] | dict[_KT, _VT]],
        partial: dict[_KT, _VT] = dict(),
        delay: Real | Sequence[Real, Real] = 0.,
        tqdm_options: dict = dict(),
    ) -> list:
    """함수를 여러 인자 목록에 대해 순차적으로 실행한다. `partial`은 모든 실행에 공통 키워드 인자로 전달된다."""
    import time
    try:
        from tqdm import tqdm
    except:
        tqdm = lambda x: x
        tqdm_options = dict()

    def run_with_delay(args: tuple[_VT, ...] | dict[_KT, _VT]) -> Any:
        try:
            if isinstance(args, dict):
                return func(**args, **partial)
            else:
                return func(*args, **partial)
        finally: time.sleep(_get_seconds(delay))

    return [run_with_delay(args) for args in tqdm(arr_args, **tqdm_options)]


async def gather_async(
        func: Coroutine,
        arr_args: Iterable[tuple[_VT, ...] | dict[_KT, _VT]],
        partial: dict[_KT, _VT] = dict(),
        delay: Real | Sequence[Real, Real] = 0.,
        max_concurrent: int | None = None,
        tqdm_options: dict = dict(),
    ) -> list:
    """코루틴을 여러 인자 목록에 대해 비동기로 병렬 실행한다. `partial`은 모든 실행에 공통 키워드 인자로 전달된다."""
    import asyncio
    try:
        from tqdm.asyncio import tqdm_asyncio
    except:
        tqdm_asyncio = asyncio
        tqdm_options = dict()

    async def run_with_delay(args: tuple[_VT, ...] | dict[_KT, _VT]) -> Any:
        try:
            if isinstance(args, dict):
                return await func(**args, **partial)
            else:
                return await func(*args, **partial)
        finally: await asyncio.sleep(_get_seconds(delay))

    async def run_with_delay_and_lock(semaphore, args: tuple[_VT, ...] | dict[_KT, _VT]) -> Any:
        if semaphore is not None:
            async with semaphore:
                return await run_with_delay(args)
        else:
            return await run_with_delay(args)

    semaphore = asyncio.Semaphore(max_concurrent) if isinstance(max_concurrent, int) else None
    return await tqdm_asyncio.gather(*[run_with_delay_and_lock(semaphore, args) for args in arr_args], **tqdm_options)


def _get_seconds(value: Real | Sequence[Real, Real]) -> Real:
    """지연 시간을 초 단위로 반환한다. `tuple`이면 무작위 범위로 인식한다."""
    if isinstance(value, (float, int)):
        return value
    elif isinstance(value, Sequence) and (len(value) > 1):
        import random
        start, end = value[0] * 1000, value[1] * 1000
        return random.uniform(float(start), float(end))
    else: return 0.


###################################################################
############################## Expand #############################
###################################################################

def expand(
        func: Callable[..., Any],
        mapping: dict[_KT, Iterable[_VT]],
        partial: dict[_KT, _VT] = dict(),
        delay: Real | Sequence[Real, Real] = 0.,
        tqdm_options: dict = dict()
    ) -> list:
    """여러 인자 목록들의 카테시안 곱을 전개하여 함수를 순차적으로 실행한다."""
    return gather(func, _expand_kwargs(**mapping), partial, delay, tqdm_options)


async def expand_async(
        func: Coroutine,
        mapping: dict[_KT, Iterable[_VT]],
        partial: dict[_KT, _VT] = dict(),
        delay: Real | Sequence[Real, Real] = 0.,
        max_concurrent: int | None = None,
        tqdm_options: dict = dict()
    ) -> list:
    """여러 인자 목록들의 카테시안 곱을 전개하여 코루틴을 비동기로 병렬 실행한다."""
    return await gather_async(func, _expand_kwargs(**mapping), partial, delay, max_concurrent, tqdm_options)


def _expand_kwargs(**map_kwargs: Iterable[_VT]) -> list[dict[_KT, _VT]]:
    """여러 인자 목록들의 카테시안 곱을 딕셔너리 리스트로 전개한다.
    ```
    >>> print(_expand_kwargs(k1=[11, 12], k2=[21, 22]))
    ===
    [{"k1": 11, "k2", 21}, {"k1": 11, "k2", 22}, {"k1": 12, "k2", 21}, {"k1": 12, "k2", 22}]
    ```"""
    from itertools import product
    keys = map_kwargs.keys()
    return [dict(zip(keys, values)) for values in product(*map_kwargs.values())]
