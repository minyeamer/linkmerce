from __future__ import annotations

from typing import Callable, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Hashable, Literal, TypeVar
    _KT = TypeVar("_KT", bound=Hashable)
    _VT = TypeVar("_VT", bound=Any)
    KeyPath = TypeVar("KeyPath", Sequence[_KT], str)


def _split_path(path: KeyPath, delimiter: str = '.') -> list[_KT]:
    """Dot notation 경로를 구분한다."""
    return path.split(delimiter) if isinstance(path, str) else path


def hier_get(
        __m: dict[_KT, _VT],
        path: KeyPath,
        default: _VT | None = None,
        delimiter: str = '.',
        on_missing: Literal["ignore", "raise"] = "ignore",
    ) -> _VT:
    """중첩된 딕셔너리에서 지정된 경로의 값을 추출한다."""
    try:
        for key in _split_path(path, delimiter):
            if isinstance(path, str) and isinstance(key, str) and key.isdigit():
                key = int(key)
            __m = __m[key]
    except (KeyError, IndexError, TypeError):
        if on_missing == "raise": raise
        return default
    return __m


def hier_set(
        __m: dict[_KT, _VT],
        path: KeyPath,
        value: _VT,
        delimiter: str = '.',
        on_missing: Literal["create", "ignore", "raise"] = "ignore",
    ):
    """중첩된 딕셔너리에서 지정된 경로에 값을 추가한다."""
    keys = _split_path(path, delimiter)
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
    for path, value in items.items():
        hier_set(__m, path, value, delimiter, on_missing)


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
    1. `{"path": None}`           → key 값을 그대로 추출, 없으면 스키마 값 반환
    2. `{"path": ["key1", ...]}`  → path 하위 dict에서 list의 키값을 추출
    3. `{"path": dict}`           → path 하위 dict에 대해 재귀 적용
    4. `["key1", ...]`            → 최상위 dict에서 list의 키값을 추출
    """
    result = dict()
    common_get = dict(delimiter=delimiter, on_missing=on_missing)
    common_set = dict(delimiter=delimiter, on_missing="create")

    for path, spec in (schema if isinstance(schema, dict) else {delimiter: schema}).items():
        fallback = dict() if isinstance(spec, (dict, list)) else spec
        __n = __m if path == delimiter else hier_get(__m, path, fallback, **common_get)

        if isinstance(spec, list): # CASE 2 + 4
            for key in spec:
                value = hier_get(__n, key, default, **common_get)
                hier_set(result, key, value, **common_set)
        elif isinstance(spec, dict): # CASE 3
            for subpath, subschema in spec.items():
                value = select_values(__n, {subpath: subschema}, default, **common_get)
                hier_set(result, subpath, value, **common_set)
        else: # CASE 1
            hier_set(result, path, __n, **common_set)

    return result


def apply_values(
        __m: dict[_KT, _VT],
        func: Callable,
        paths: Sequence[KeyPath] | None = None,
        delimiter: str = '.',
        depth: int | None = None,
        inplace: bool = True,
    ) -> dict[_KT, _VT]:
    """중첩된 딕셔너리에서 지정된 경로 또는 모든 leaf 값에 함수를 적용한다."""
    # 1. 지정된 경로만 처리하고 딕셔너리 반환
    if paths:
        if not inplace:
            from copy import deepcopy
            __m = deepcopy(__m)

        for path in paths:
            val = hier_get(__m, path, delimiter=delimiter)
            hier_set(__m, path, func(val), delimiter=delimiter, on_missing="ignore")
        return __m

    # 2. 모든 leaf 노드에 함수 적용 (depth 도달 시 강제 적용)
    is_leaf = (not isinstance(__m, (dict, list)))
    if is_leaf or ((depth is not None) and (depth <= 0)):
        return func(__m)

    common = dict(paths=None, delimiter=delimiter, depth=(None if depth is None else depth - 1))
    if isinstance(__m, dict):
        return {key: apply_values(value, func, **common) for key, value in __m.items()}
    elif isinstance(__m, list):
        return [apply_values(value, func, **common) for value in __m]
    return func(__m)


def coalesce(
        __m: dict[_KT, _VT],
        paths: Sequence[KeyPath],
        default: _VT | None = None,
        delimiter: str = '.',
        condition: Callable[[_VT], bool] | Literal["notna", "exists"] = "notna",
    ) -> _VT:
    """여러 경로 중 조건을 만족하는 첫 번째 값을 반환한다."""
    if not isinstance(condition, Callable):
        condition = bool if condition == "exists" else (lambda x: x is not None)
    common = dict(default=None, delimiter=delimiter, on_missing="raise")

    for path in paths:
        try:
            if condition(value := hier_get(__m, path, **common)):
                return value
        except (KeyError, IndexError, TypeError):
            continue
    return default
