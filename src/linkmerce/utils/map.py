from __future__ import annotations

from typing import Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Callable, Hashable, TypeVar
    _KT = TypeVar("_KT", Hashable)
    _VT = TypeVar("_VT", Any)


def camel_to_snake(s: str) -> str:
    import re
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', s)
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def distinct_dict(*args: dict[_KT,_VT]) -> dict[_KT,list[_VT]]:
    from collections import defaultdict, OrderedDict
    base = defaultdict(OrderedDict)
    for __m in args:
        for key, value in __m.items():
            base[key][value] = None
    return {key: list(distinct.keys()) for key, distinct in base.items()}


###################################################################
############################### Get ###############################
###################################################################

def get_values(__m: dict[_KT,_VT], keys: _KT | Sequence[_KT], default: _VT | None = None) -> dict[_KT,_VT]:
    if (not isinstance(keys, str)) and isinstance(keys, Sequence):
        return {key: __m.get(key, default) for key in keys}
    else:
        return __m.get(keys, default)


def list_get(__r: list[dict[_KT,_VT]], keys: _KT | Sequence[_KT], default: _VT | None = None) -> list[dict[_KT,_VT]] | list[_VT]:
    return [get_values(__m, keys, default) for __m in __r]


def hier_get(__m: dict[_KT,_VT], path: Sequence[_KT], default: _VT | None = None) -> _VT:
    cur = __m
    for key in path:
        try:
            cur = cur[key]
        except:
            return default
    return cur


###################################################################
############################## Apply ##############################
###################################################################

def apply_values(
        __m: dict[_KT,_VT],
        func: Callable,
        keys: _KT | Sequence[_KT] | None = None,
        default: _VT | None = None
    ) -> dict[_KT,_VT]:
    if keys is None:
        return {key: func(value) for key, value in __m.items()}
    elif (not isinstance(keys, str)) and isinstance(keys, Sequence):
        return {key: func(__m.get(key, default)) for key in keys}
    else:
        return func(__m.get(keys, default))


def list_apply(
        __r: list[dict[_KT,_VT]],
        func: Callable,
        keys: _KT | Sequence[_KT] | None = None,
        default: _VT | None = None,
    ) -> list[dict[_KT,_VT]] | list[_VT]:
    return [apply_values(__m, func, keys, default) for __m in __r]


###################################################################
############################ Transform ############################
###################################################################

def to_csv(
        __r: list[dict[_KT,_VT]],
        apply: Callable | None = None,
        expected_headers: list[str] | None = None,
        include_header: bool = False,
        default: _VT | None = None,
    ) -> list[tuple]:
    func = apply if isinstance(apply, Callable) else (lambda x: x)
    header = expected_headers if isinstance(expected_headers, Sequence) else list(__r[0].keys())
    csv = [tuple(func(__m.get(key, default)) for key in header) for __m in __r]
    return ([header] if include_header else list()) + csv
