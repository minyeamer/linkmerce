from __future__ import annotations
from abc import ABCMeta, abstractmethod

from typing import Callable, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Dict, Hashable, Iterable, TypeVar
    _KT = TypeVar("_KT", Hashable)
    _VT = TypeVar("_VT", Any)
    _T = TypeVar("_T")


class Map(dict, metaclass=ABCMeta):
    def __init__(self, object_: Any, **kwargs):
        super().__init__(self.parse(object_, **kwargs))

    @abstractmethod
    def parse(self, object_: Any, **kwargs) -> Dict[_KT,_VT]:
        raise NotImplementedError("The 'parse' method must be implemented.")


class Array(list, metaclass=ABCMeta):
    dtype = None

    def __init__(self, object_: Any, params: Dict = dict(), **kwargs):
        if not isinstance(self.dtype, (Callable,Type)):
            raise TypeError("A specified data type is required.")

        iterable = self.parse(object_, **kwargs)
        super().__init__(map(lambda x: self.dtype(x, **params), iterable))

    @abstractmethod
    def parse(self, object_: Any, **kwargs) -> Iterable[_T]:
        raise NotImplementedError("The 'parse' method must be implemented.")

    def __setattr__(self, name: str, value: Any):
        if name == "dtype":
            raise AttributeError("Can't assign to protected attribute 'dtype'")
        super().__setattr__(name, value)
