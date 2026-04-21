from __future__ import annotations

from typing import Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Callable, Coroutine, Hashable, Iterable, TypeVar
    from types import ModuleType
    from numbers import Real
    _KT = TypeVar("_KT", Hashable)
    _VT = TypeVar("_VT", Any)


def import_tqdm() -> ModuleType:
    """`tqdm` 모듈을 반환한다. 설치되지 않았으면 인자를 그대로 내보내는 람다 함수를 반환한다.

    Usage
    -----
    ```
    tqdm = import_tqdm()
    for _ in tqdm(range(100), desc="Processing Data"):
        time.sleep(0.01)
    ```

    Returns
    -------
    ModuleType
        설치된 경우 `tqdm` 클래스, 설치되지 않은 경우 인자를 그대로 내보내는 람다 함수
    """
    try:
        from tqdm import tqdm
        return tqdm
    except:
        return lambda x, **kwargs: x


def import_tqdm_asyncio() -> ModuleType:
    """`tqdm.asyncio` 모듈을 반환한다. 설치되지 않았으면 `asyncio` 모듈을 반환한다.

    Usage
    -----
    ```
    tqdm_asyncio = import_tqdm_asyncio()
    await tqdm_asyncio.gather(*[asyncio.sleep(0.01) for _ in range(100)], desc="Processing Data")
    ```

    Returns
    -------
    ModuleType
        설치된 경우 `tqdm_asyncio` 클래스, 설치되지 않은 경우 `asyncio` 모듈
    """
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
        delay: Real | tuple[Real, Real] = 0.,
        tqdm_options: dict | None = None,
    ) -> list:
    """함수를 매개변수 목록에 대해 순차적으로 실행한다.

    Parameters
    ----------
    func: Callable[..., Any]
        순차 실행할 함수
    arr_args: Iterable[tuple[_VT, ...] | dict[_KT, _VT]]
        함수를 순차로 실행할 때 전달할 매개변수 목록
    partial: dict[_KT, _VT]
        모든 실행에 공통으로 전달할 키워드 인자
    delay: Real | tuple[Real, Real]
        함수 실행 간 대기 시간. 튜플이면 `[시작, 끝]` 범위에서 무작위로 결정한다.
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수

    Returns
    -------
    list
        각 매개변수에 대한 실행 결과 목록
    """
    import time
    try:
        from tqdm import tqdm
        tqdm_options = tqdm_options or dict()
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
        delay: Real | tuple[Real, Real] = 0.,
        max_concurrent: int | None = None,
        tqdm_options: dict | None = None,
    ) -> list:
    """코루틴을 매개변수 목록에 대해 비동기로 병렬 실행한다.

    Parameters
    ----------
    func: Coroutine
        병렬로 실행할 비동기 코루틴
    arr_args: Iterable[tuple[_VT, ...] | dict[_KT, _VT]]
        코루틴을 병렬로 실행할 때 전달할 매개변수 목록
    partial: dict[_KT, _VT]
        모든 실행에 공통으로 전달할 키워드 인자
    delay: Real | tuple[Real, Real]
        함수 실행 간 대기 시간. 튜플이면 `[시작, 끝]` 범위에서 무작위로 결정한다.
    max_concurrent: int | None
        비동기 실행 시 최대 동시 실행 횟수
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수

    Returns
    -------
    list
        각 매개변수에 대한 실행 결과 목록
    """
    import asyncio
    try:
        from tqdm.asyncio import tqdm_asyncio
        tqdm_options = tqdm_options or dict()
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


def _get_seconds(value: Real | tuple[Real, Real]) -> Real:
    """지연 시간을 초 단위로 반환한다."""
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
        delay: Real | tuple[Real, Real] = 0.,
        tqdm_options: dict | None = None
    ) -> list:
    """매개변수 목록들의 카테시안 곱을 전개하여 함수를 순차적으로 실행한다.

    Parameters
    ----------
    func: Callable[..., Any]
        순차 실행할 함수
    mapping: dict[_KT, Iterable[_VT]]
        카테시안 곱을 전개할 매개변수 목록. 키는 인자명, 값은 인자 목록이다.
    partial: dict[_KT, _VT]
        모든 실행에 공통으로 전달할 키워드 인자
    delay: Real | tuple[Real, Real]
        함수 실행 간 대기 시간. 튜플이면 `[시작, 끝]` 범위에서 무작위로 결정한다.
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수

    Returns
    -------
    list
        각 매개변수에 대한 실행 결과 목록
    """
    return gather(func, _expand_kwargs(**mapping), partial, delay, tqdm_options)


async def expand_async(
        func: Coroutine,
        mapping: dict[_KT, Iterable[_VT]],
        partial: dict[_KT, _VT] = dict(),
        delay: Real | tuple[Real, Real] = 0.,
        max_concurrent: int | None = None,
        tqdm_options: dict | None = None
    ) -> list:
    """매개변수 목록들의 카테시안 곱을 전개하여 코루틴을 비동기로 병렬 실행한다.

    Parameters
    ----------
    func: Coroutine
        병렬로 실행할 비동기 코루틴
    mapping: dict[_KT, Iterable[_VT]]
        카테시안 곱을 전개할 매개변수 목록. 키는 인자명, 값은 인자 목록이다.
    partial: dict[_KT, _VT]
        모든 실행에 공통으로 전달할 키워드 인자
    delay: Real | tuple[Real, Real]
        함수 실행 간 대기 시간. 튜플이면 `[시작, 끝]` 범위에서 무작위로 결정한다.
    max_concurrent: int | None
        비동기 실행 시 최대 동시 실행 횟수
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수

    Returns
    -------
    list
        각 매개변수에 대한 실행 결과 목록
    """
    return await gather_async(func, _expand_kwargs(**mapping), partial, delay, max_concurrent, tqdm_options)


def _expand_kwargs(**map_kwargs: Iterable[_VT]) -> list[dict[_KT, _VT]]:
    """매개변수 목록들의 카테시안 곱을 딕셔너리 리스트로 전개한다.

    Examples
    --------
    ```
    >>> print(_expand_kwargs(k1=[11, 12], k2=[21, 22]))
    [{"k1": 11, "k2": 21}, {"k1": 11, "k2": 22}, {"k1": 12, "k2": 21}, {"k1": 12, "k2": 22}]
    ```
    """
    from itertools import product
    keys = map_kwargs.keys()
    return [dict(zip(keys, values)) for values in product(*map_kwargs.values())]
