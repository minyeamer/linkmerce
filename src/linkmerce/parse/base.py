from __future__ import annotations

from abc import ABCMeta, abstractmethod
import functools

from typing import Callable, Dict, Sequence, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Hashable, Iterator, Literal, Tuple, TypeVar
    _KT = TypeVar("_KT", Hashable)
    _VT = TypeVar("_VT", Any)
    _T = TypeVar("_T")


class Parser(metaclass=ABCMeta):
    null: Any = None

    @abstractmethod
    def parse(self, object_: Any, **kwargs) -> Any:
        raise NotImplementedError("The 'parse' method must be implemented.")

    def ensure_type(self,
            object_: Any,
            type_: Type | Tuple[Type, ...],
            func: Callable,
            args: Tuple = tuple(),
            kwargs: Dict = dict(),
            timing: Literal["before","after","both"] | None = None,
            static: bool = False
        ) -> Any:
        if timing in ("before","both") and (not isinstance(object_, type_)):
            return self.null
        result = func(object_, *args, **kwargs) if static else func(self, object_, *args, **kwargs)
        if timing in ("after","both") and (not isinstance(result, type_)):
            return self.null
        return result


class Map(Parser, dict):
    notnull: Sequence[_KT] = list()
    null: Dict = dict()

    def __init__(self, object_: Any, **kwargs):
        super().__init__(self.parse(object_, **kwargs))

    def parse(self, object_: Dict[_KT,_VT], **kwargs) -> Dict[_KT,_VT]:
        return self.map(object_)

    def map(self, map_: Dict = dict(), **kwargs) -> Dict[_KT,_VT]:
        map_ = dict(map_, **kwargs) if kwargs else map_
        if any((not map_.get(key)) for key in self.notnull):
            return self.null
        else:
            return map_

    def ensure_dict(timing: Literal["before","after","both"] | None = None):
        def ensure_type(func):
            @functools.wraps(func)
            def wrapper(self: Map, object_: Any, **kwargs):
                return self.ensure_type(object_, Dict, func, kwargs=kwargs, timing=timing, static=False)
            return wrapper
        return ensure_type


class Array(Parser, list):
    dtype: type | staticmethod = None
    null: Sequence = list()

    def __init__(self, object_: Any, context: Dict = dict(), **kwargs):
        self._assert_dtype()
        iterable = self.parse(object_, **kwargs)
        super().__init__(self.map(iterable, context))

    def parse(self, object_: Any, context: Dict = dict(), **kwargs) -> Sequence[_T]:
        return self.map(object_, context)

    def map(self, sequence_: Sequence = list(), context: Dict = dict()) -> Iterator:
        iterator = map(lambda x: self.dtype(x, **context), sequence_)
        if isinstance(self.dtype, Type):
            return filter(lambda data: isinstance(data, self.dtype), iterator)
        else:
            return filter(lambda data: data is not None, iterator)

    def ensure_sequence(timing: Literal["before","after","both"] | None = None):
        def ensure_type(func):
            @functools.wraps(func)
            def wrapper(self: Array, object_: Any, **kwargs):
                return self.ensure_type(object_, Sequence, func, kwargs=kwargs, timing=timing, static=False)
            return wrapper
        return ensure_type

    def _assert_dtype(self):
        if not isinstance(self.dtype, Callable):
            raise TypeError("A specified data type is required.")

    def __setattr__(self, name: str, value: Any):
        if name == "dtype":
            raise AttributeError("Can't assign to protected attribute 'dtype'")
        super().__setattr__(name, value)
