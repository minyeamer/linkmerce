from __future__ import annotations

from typing import Any, Dict, Hashable, IO, List, TypeVar, Union

_KT = TypeVar("_KT", Hashable)
_VT = TypeVar("_VT", Any)

JsonObject = Union[Dict, List]
JsonSerialize = Union[Dict, List, bytes, IO]

ResponseComponent = Union[int, bytes, str, JsonObject, dict[str,str]]

TaskOption = Dict[_KT,_VT]
TaskOptions = Dict[_KT,TaskOption]
