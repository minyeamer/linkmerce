from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Sequence
    from bs4 import BeautifulSoup, Tag



###################################################################
############################## Clean ##############################
###################################################################

def clean_html(__object: str, features: str | Sequence[str] | None = "html.parser", strip: bool = True) -> str:
    from bs4 import BeautifulSoup
    return _get_text(BeautifulSoup(__object, features), strip)


def clean_tag(__object: str, strip: bool = True) -> str:
    import re
    text = re.sub("<[^>]*>", "", __object)
    return text.strip() if strip else text


def _get_text(tag: BeautifulSoup | Tag, strip: bool = True) -> str:
    if strip:
        import re
        return re.sub(r"\s+", ' ', tag.get_text()).strip()
    else:
        return tag.get_text()


###################################################################
############################## Select #############################
###################################################################

def select(tag: BeautifulSoup | Tag, selector: str) -> Tag | list[Tag] | str | list[str] | None:
    path = _split_selector(selector)
    if path[-1].startswith(':') and path[-1].endswith(':'):
        tag = hier_select(tag, path[:-1])
        return select_attr(tag, _unwrap(path[-1]))
    else:
        return hier_select(tag, path)


def select_attr(tag: BeautifulSoup | Tag | list[Tag], attr: str) -> str | list[str] | None:
    if (tag is None) or (not attr):
        return tag
    elif isinstance(tag, list):
        return [select_attr(tag_, attr) for tag_ in tag]
    elif attr == "text()":
        return _get_text(tag)
    elif attr.startswith("attr(") and attr.endswith(')'):
        return tag.get(attr[5:-1])
    elif attr.startswith("class(") and attr.endswith(')'):
        names = [value] if isinstance(value := tag.get("class"), str) else value
        try:
            return names if attr == "class()" else names[int(attr[6:-1])]
        except:
            return None
    elif attr == "id()":
        return tag.get("id")
    elif attr.startswith("data(") and attr.endswith(')'):
        return tag.get("data-{}".format(attr[5:-1]))
    elif attr == "label()":
        return tag.get("aria-labelledby")
    else:
        raise ValueError(f"Unknown attribute: {attr}")


def hier_select(tag: BeautifulSoup | Tag | list[Tag], path: Sequence[str | int]) -> Tag | list[Tag] | None:
    if (tag is None) or (not path):
        return tag
    elif isinstance(tag, list):
        if isinstance(path[0], int):
            return hier_select(tag[path[0]], path[1:])
        else:
            return [hier_select(tag_, path) for tag_ in tag]
    elif path[0].endswith(':'):
        return hier_select(tag.select(path[0][:-1]), path[1:])
    else:
        return hier_select(tag.select_one(path[0]), path[1:])


def _split_selector(selector: str) -> list[str]:
    import re
    path = list()
    split_pattern = '|'.join([r"> \.{3} >", r"(?<=:)all", r"(?<=:)nth-element(?=\(\d+\))", r"> (?=:)"])
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
    return s[1:-1]
