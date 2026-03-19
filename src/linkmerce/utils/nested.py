from __future__ import annotations

from typing import Callable, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Hashable, Literal, TypeVar
    _KT = TypeVar("_KT", bound=Hashable)
    _VT = TypeVar("_VT", bound=Any)
    KeyPath = TypeVar("KeyPath", Sequence[_KT], str)


def _concat_path(key_path1: KeyPath, key_path2: KeyPath, delimiter: str = '.') -> KeyPath:
    """Dot notation 경로를 합친다. 반환 타입은 `key_path1`에 맞춰진다."""
    if isinstance(key_path1, str):
        key_path2 = key_path2 if isinstance(key_path2, str) else delimiter.join(map(str, key_path2))
        return (key_path1 + '.' + key_path2) if key_path1 != delimiter else key_path2
    else:
        key_path2 = _split_path(key_path2) if isinstance(key_path2, str) else key_path2
        return key_path1 + key_path2


def _split_path(key_path: KeyPath, delimiter: str = '.') -> list[_KT]:
    """Dot notation 경로를 구분한다."""
    if isinstance(key_path, str):
        return key_path.split(delimiter) if key_path != delimiter else list()
    return key_path


def hier_get(
        __m: dict[_KT, _VT],
        key_path: KeyPath,
        default: _VT | None = None,
        delimiter: str = '.',
        on_missing: Literal["ignore", "raise"] = "ignore",
    ) -> _VT:
    """중첩된 딕셔너리에서 지정된 경로의 값을 추출한다."""
    try:
        for key in _split_path(key_path, delimiter):
            if isinstance(key_path, str) and isinstance(key, str) and key.isdigit():
                key = int(key)
            __m = __m[key]
        return __m
    except (KeyError, IndexError, TypeError):
        if on_missing == "raise": raise
        return default


def hier_set(
        __m: dict[_KT, _VT],
        key_path: KeyPath,
        value: _VT,
        delimiter: str = '.',
        on_missing: Literal["create", "ignore", "raise"] = "ignore",
    ):
    """중첩된 딕셔너리에서 지정된 경로에 값을 추가한다."""
    keys = _split_path(key_path, delimiter)
    try:
        for key in keys[:-1]:
            if (key not in __m) and (on_missing == "create"):
                __m[key] = dict()
            __m = __m[key]
        __m[keys[-1]] = value
    except (KeyError, IndexError, TypeError):
        if on_missing == "raise": raise
        elif on_missing == "ignore": return


def hier_update(
        __m: dict[_KT, _VT],
        items: dict[KeyPath, _VT],
        delimiter: str = '.',
        on_missing: Literal["create", "ignore", "raise"] = "ignore",
    ):
    """중첩된 딕셔너리에서 지정된 경로에 값을 추가한다."""
    for key_path, value in items.items():
        hier_set(__m, key_path, value, delimiter, on_missing)


def select_values(
        __m: dict[_KT, _VT],
        schema: dict[KeyPath, dict | list[KeyPath]] | list[KeyPath],
        default: _VT | None = None,
        delimiter: str = '.',
        on_missing: Literal["ignore", "raise"] = "ignore",
    ) -> dict[_KT, _VT]:
    """
    중첩된 딕셔너리에서 지정된 스키마에 맞춰서 각각의 경로의 값을 추출한다.

    ### schema
    1. `{"key_path": ["key1", ...]}`    >> `key_path` 하위 `dict`에서 `list`의 키값을 추출
    2. `{"key_path": dict}`             >> `key_path` 하위 dict에 대해 재귀 적용 (`list` 내 중첩 가능)
    3. `{"key_path": None}`             >> 단일 경로의 값을 그대로 추출, 없으면 스키마 값을 추가
    4. `["key1", ...]`                  >> 최상위 `dict`에서 `list`의 키값을 추출"""
    result = dict()
    common_get = dict(delimiter=delimiter, on_missing=on_missing)
    common_set = dict(delimiter=delimiter, on_missing="create")

    for key_path, spec in (schema if isinstance(schema, dict) else {delimiter: schema}).items():
        if isinstance(spec, list): # CASE 1 + 4
            __n = hier_get(__m, key_path, default=dict(), **common_get)
            for key in spec:
                if isinstance(key, dict): # list 내 dict 중첩 (재귀 호출)
                    key_values = select_values(__n, key, default, **common_get)
                    path_values = {_concat_path(key_path, sub_path): value for sub_path, value in key_values.items()}
                    hier_update(result, path_values, **common_set)
                else: # dict에서 list의 키값 조회
                    value = hier_get(__n, key, default, **common_get)
                    hier_set(result, _concat_path(key_path, key), value, **common_set)

        elif isinstance(spec, dict): # CASE 2
            __n = hier_get(__m, key_path, default=dict(), **common_get)
            for sub_path, sub_schema in spec.items(): # 하위 스키마에 대해 재귀 호출
                if isinstance(sub_schema, list):
                    # list 스키마: sub_path 하위 dict에서 키값 추출
                    __nn = hier_get(__n, sub_path, default=dict(), **common_get)
                    value = select_values(__nn, sub_schema, default, **common_get)
                else:
                    value = select_values(__n, {sub_path: sub_schema}, default, **common_get)
                hier_set(result, _concat_path(key_path, sub_path), value, **common_set)

        else: # CASE 3
            value = hier_get(__m, key_path, default=spec, delimiter=delimiter, on_missing="ignore")
            hier_set(result, key_path, value, **common_set)

    return result


def apply_values(
        __m: dict[_KT, _VT],
        func: Callable,
        key_paths: Sequence[KeyPath] | None = None,
        delimiter: str = '.',
        depth: int | None = None,
        inplace: bool = True,
    ) -> dict[_KT, _VT]:
    """중첩된 딕셔너리에서 지정된 경로 또는 모든 leaf 값에 함수를 적용한다."""
    # 1. 지정된 경로만 처리하고 딕셔너리 반환
    if key_paths:
        if not inplace:
            from copy import deepcopy
            __m = deepcopy(__m)

        for key_path in key_paths:
            val = hier_get(__m, key_path, delimiter=delimiter)
            hier_set(__m, key_path, func(val), delimiter=delimiter, on_missing="ignore")
        return __m

    # 2. 모든 leaf 노드에 함수 적용 (depth 도달 시 강제 적용)
    is_leaf = (not isinstance(__m, (dict, list)))
    if is_leaf or ((depth is not None) and (depth <= 0)):
        return func(__m)

    common = dict(key_paths=None, delimiter=delimiter, depth=(None if depth is None else depth - 1))
    if isinstance(__m, dict):
        return {key: apply_values(value, func, **common) for key, value in __m.items()}
    elif isinstance(__m, list):
        return [apply_values(value, func, **common) for value in __m]
    return func(__m)


def coalesce(
        __m: dict[_KT, _VT],
        key_paths: Sequence[KeyPath],
        default: _VT | None = None,
        delimiter: str = '.',
        condition: Callable[[_VT], bool] | Literal["notna", "exists"] = "notna",
    ) -> _VT:
    """여러 경로 중 조건을 만족하는 첫 번째 값을 반환한다."""
    if not isinstance(condition, Callable):
        condition = bool if condition == "exists" else (lambda x: x is not None)
    common = dict(default=None, delimiter=delimiter, on_missing="raise")

    for key_path in key_paths:
        try:
            if condition(value := hier_get(__m, key_path, **common)):
                return value
        except (KeyError, IndexError, TypeError):
            continue
    return default
