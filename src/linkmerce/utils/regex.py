from __future__ import annotations

import re

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any


def regexp_extract(pattern: re.Pattern | str, string: str, index: int = 0, default: Any | None = None) -> str:
    match = re.search(pattern, string)
    return match.groups()[index] if match else default
