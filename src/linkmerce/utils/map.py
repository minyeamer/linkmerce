from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Hashable, Sequence, TypeVar
    _KT = TypeVar("_KT", Hashable)
    _VT = TypeVar("_VT", Any)


def hier_get(__m: dict, path: Sequence[_KT], default: _VT | None = None) -> _VT:
    cur = __m
    for key in path:
        if isinstance(cur, dict) and (key in cur):
            cur = cur[key]
        else:
            return default
    return cur


def distinct_dict(*args: dict) -> dict[_KT,list[_VT]]:
    from collections import defaultdict, OrderedDict
    base = defaultdict(OrderedDict)
    for __m in args:
        for key, value in __m.items():
            base[key][value] = None
    return {key: list(distinct.keys()) for key, distinct in base.items()}
