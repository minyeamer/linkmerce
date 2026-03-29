from __future__ import annotations

import re

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any


def regexp_match(pattern: re.Pattern | str, string: str) -> bool:
    """문자열이 정규식 패턴과 일치하는지 확인한다."""
    return bool(re.search(pattern, string))


def regexp_extract(pattern: re.Pattern | str, string: str, index: int = 0, default: Any | None = None) -> str:
    """정규식 그룹에서 지정된 인덱스의 값을 추출한다."""
    match = re.search(pattern, string)
    return match.groups()[index] if match else default


def regexp_groups(pattern: re.Pattern | str, string: str, indices: list[int] = list(), default: Any | None = None) -> list[str]:
    """정규식 그룹 전체 또는 지정된 인덱스의 값 리스트를 반환한다."""
    match = re.search(pattern, string)
    if match:
        groups = match.groups()
        if indices:
            return [(groups[index] if index < len(groups) else default) for index in indices]
        else:
            return groups
    else:
        return [default for _ in indices]


def regexp_replace(pattern: re.Pattern | str, repl: str, string: str, count: int = 0) -> str:
    """정규식 패턴과 일치하는 부분을 치환한다."""
    return re.sub(pattern, repl, string, count)


def regexp_replace_map(map: dict[str, str], string: str, count: int = 0) -> str:
    """여러 정규식 패턴-치환값 쌍을 순차적으로 적용한다."""
    for pattern, repl in map.items():
        string = re.sub(pattern, repl, string, count)
    return string
