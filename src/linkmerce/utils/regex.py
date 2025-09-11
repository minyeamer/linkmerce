from __future__ import annotations

import re

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any


def regexp_extract(pattern: re.Pattern | str, string: str, index: int = 0, default: Any | None = None) -> str:
    match = re.search(pattern, string)
    return match.groups()[index] if match else default


def regexp_replace(pattern: re.Pattern | str, repl: str, string: str, count: int = 0) -> str:
    return re.sub(pattern, repl, string, count)


def regexp_replace_map(map: dict[str,str], string: str, count: int = 0) -> str:
    for pattern, repl in map.items():
        string = re.sub(pattern, repl, string, count)
    return string
