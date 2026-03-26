from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Hashable, Literal, Sequence, TypeVar
    from bs4 import BeautifulSoup, Tag
    _KT = TypeVar("_KT", bound=Hashable)
    _VT = TypeVar("_VT", bound=Any)


def camel_to_snake(s: str) -> str:
    """camelCase 문자열을 snake_case로 변환한다."""
    import re
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', s)
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


###################################################################
############################## Clean ##############################
###################################################################

def clean_html(
        __object: str,
        features: str | Sequence[str] | None = "html.parser",
        strip: bool = True,
    ) -> str:
    """HTML 문자열을 파싱하여 태그를 제거한 순수 텍스트를 반환한다."""
    from bs4 import BeautifulSoup
    return _get_text(BeautifulSoup(__object, features), strip)


def clean_tag(__object: str, strip: bool = True) -> str:
    """정규식으로 HTML 태그를 제거한 텍스트를 반환한다."""
    import re
    text = re.sub("<[^>]*>", "", __object)
    return text.strip() if strip else text


def _get_text(tag: BeautifulSoup | Tag, strip: bool = True) -> str:
    """BeautifulSoup 객체에서 텍스트를 추출한다. `strip=True`이면 연속 공백을 단일 공백으로 정규화한다."""
    if strip:
        import re
        return re.sub(r"\s+", ' ', tag.get_text()).strip()
    else:
        return tag.get_text()


###################################################################
############################## Select #############################
###################################################################

def select(
        tag: BeautifulSoup | Tag,
        selector: str,
        on_missing: Literal["ignore", "raise"] = "ignore",
    ) -> Tag | list[Tag] | str | list[str] | None:
    """CSS 선택자(Selector)로 BeautifulSoup 객체를 파싱한다.   
    마지막 자식 선택자가 `:attr:` 형식이면 해당 속성값을 추출해 반환한다.

    지원하는 가상 속성(`:attr:`) 형식:
    - `:text():` - 요소의 텍스트 추출
    - `:attr(name):` - `name`에 해당하는 태그 속성값 추출
    - `:class(n):` - n번째 클래스명 추출 (`:class():`는 전체 리스트)
    - `:id():` - 태그의 id 속성값 추출
    - `:data(name):` - `data-name` 속성값 추출
    - `:label():` - `aria-labelledby` 속성값 추출"""
    path = _split_selector(selector)
    if path[-1].startswith(':') and path[-1].endswith(':'):
        tag = _hier_select(tag, path[:-1], on_missing)
        return _select_attr(tag, _unwrap(path[-1]), on_missing)
    else:
        return _hier_select(tag, path, on_missing)


def select_attrs(
        tag: BeautifulSoup | Tag,
        schema: dict[_KT, str | tuple[str, _VT]],
        on_missing: Literal["ignore", "raise"] = "ignore",
    ) -> dict[_KT, str | list[str]]:
    """`{필드명: CSS_선택자}` 스키마를 입력받아 매칭되는 속성값들을 딕셔너리로 묶어서 반환한다.

    `CSS_선택자`에 해당되는 스키마 값이 `str` 형식이 아니라면 다음과 같이 분기 처리한다:
    - 스키마 값이 `tuple` 형식이라면 `(CSS_선택자, 기본값)`으로 인식해 실행 오류를 무시하고 기본값을 반환한다.
    - 그 외에 `str` 형식이 아닌 스키마 값은 상수로 인식하고 값으로 추가한다."""
    return {key: (select(tag, selector, on_missing=on_missing) if isinstance(selector, str) else selector)
            for key, selector in schema.items()}


def _select_attr(
        tag: BeautifulSoup | Tag | list[Tag],
        attr: str,
        on_missing: Literal["ignore", "raise"] = "ignore",
    ) -> str | list[str] | None:
    """BeautifulSoup 객체에서 가상의 속성 선택자(`attr`)에 해당하는 속성값을 추출한다."""
    if (tag is None) or (not attr):
        pass
    elif isinstance(tag, list):
        return [_select_attr(tag_, attr, on_missing=on_missing) for tag_ in tag]
    elif attr == "text()":
        return _get_text(tag)
    elif attr.startswith("attr(") and attr.endswith(')'):
        return tag.get(attr[5:-1])
    elif attr.startswith("class(") and attr.endswith(')'):
        names = [value] if isinstance(value := tag.get("class"), str) else value
        try:
            return names if attr == "class()" else names[int(attr[6:-1])]
        except IndexError:
            if on_missing == "raise": raise
    elif attr == "id()":
        return tag.get("id")
    elif attr.startswith("data(") and attr.endswith(')'):
        return tag.get("data-{}".format(attr[5:-1]))
    elif attr == "label()":
        return tag.get("aria-labelledby")

    if on_missing == "raise":
        raise ValueError(f"Could not find element for attribute: '{attr}'")
    return None


def _hier_select(
        tag: BeautifulSoup | Tag | list[Tag],
        path: Sequence[str | int],
        on_missing: Literal["ignore", "raise"] = "ignore",
    ) -> Tag | list[Tag] | None:
    """복합 CSS 선택자 경로를 탐색한다. 경로 끝에 `:`가 붙으면 `select`로, 없으면 `select_one`으로 처리한다."""
    if (tag is None) or (isinstance(tag, list) and (len(tag) == 0)):
        if on_missing == "raise":
            raise ValueError(f"Could not find element for selector path: '{path}'")
        return tag
    elif not path:
        return tag
    elif isinstance(tag, list):
        if isinstance(path[0], int):
            return _hier_select(tag[path[0]], path[1:], on_missing)
        else:
            return [_hier_select(t, path, on_missing) for t in tag]
    elif path[0].endswith(':'):
        return _hier_select(tag.select(path[0][:-1]), path[1:])
    else:
        return _hier_select(tag.select_one(path[0]), path[1:])


def _split_selector(selector: str) -> list[str]:
    """복합 CSS 선택자 문자열을 경로 리스트로 분해한다. `all`, `nth-element(n)` 등의 특수 구문을 처리한다."""
    import re
    path = list()
    split_pattern = '|'.join([r"(?<=:)all", r"(?<=:)nth-element(?=\(\d+\))", r"> (?=:)"])
    for part in re.split(split_pattern, selector):
        part = part.strip()
        if not part:
            continue
        elif re.match(r"^\(\d+\)$", part):
            path.append(int(_unwrap(part)))
        elif re.match(r"^\(\d+\) >", part):
            nth, selector = part.split('>', 1)
            path += [int(_unwrap(nth.strip())), selector.strip()]
        elif part.startswith(">"):
            path.append(part[1:].strip())
        else:
            path.append(part)
    return path


def _unwrap(s: str) -> str:
    """문자열 양 끝의 한 글자씩을 제거한다."""
    return s[1:-1]
