from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any


def safe_float(object_: Any, default: float | None = None) -> float:
    """값을 `float`로 안전하게 변환한다. 실패 시 기본값을 반환한다."""
    try:
        if isinstance(object_, str) and (',' in object_):
            object_ = object_.replace(',', '')
        return float(object_)
    except:
        return default


def safe_int(object_: Any, default: int | None = None) -> int:
    """값을 `int`로 안전하게 변환한다. 실패 시 기본값을 반환한다."""
    try:
        if isinstance(object_, str) and (',' in object_):
            object_ = object_.replace(',', '')
        return int(float(object_))
    except:
        return default
