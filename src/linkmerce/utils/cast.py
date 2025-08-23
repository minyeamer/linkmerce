from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any


def safe_float(object_: Any, default: float | None = None) -> float:
    try:
        if isinstance(object_, str) and (',' in object_):
            object_ = object_.replace(',', '')
        return float(object_)
    except:
        return default


def safe_int(object_: Any, default: int | None = None) -> int:
    try:
        if isinstance(object_, str) and (',' in object_):
            object_ = object_.replace(',', '')
        return int(float(object_))
    except:
        return default
