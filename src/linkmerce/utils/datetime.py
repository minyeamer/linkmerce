from __future__ import annotations

import datetime as dt

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    ...


def strpdate(date: dt.date | str, format: str = "%Y-%m-%d") -> dt.date:
    return date if isinstance(date, dt.date) else dt.datetime.strptime(str(date), format).date()


def safe_strpdate(date: dt.date | str, format: str = "%Y-%m-%d", default: dt.date | None = None) -> dt.date:
    try:
        return strpdate(date, format)
    except:
        return default
