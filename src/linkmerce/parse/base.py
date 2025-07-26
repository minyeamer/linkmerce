from __future__ import annotations

from abc import ABCMeta, abstractmethod

from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Dict, Iterable, List, Literal, Sequence, Type


class Parser(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, obj: Any, *args, **kwargs):
        raise NotImplementedError("The '__init__' method must be implemented.")

    @abstractmethod
    def parse(self, obj: Any, *args, **kwargs) -> Any:
        raise NotImplementedError("The 'parse' method must be implemented.")

    def raise_parse_error(self, msg: str):
        from linkmerce.exceptions import ParseError
        raise ParseError(msg)


class ListParser(list, Parser):
    sequential: bool = True

    def __init__(self, obj: Any, *args, **kwargs):
        list.__init__(self, self.parse(obj, *args, **kwargs))

    def parse(self, obj: Any, *args, **kwargs) -> Iterable:
        if isinstance(obj, Sequence if self.sequential else Iterable):
            return obj
        else:
            self.raise_parse_error()

    def raise_parse_error(self, msg: str = str()):
        msg = msg or "Object is not {}.".format("sequential" if self.sequential else "iterable")
        super().raise_parse_error(msg)


class RecordsParser(ListParser):
    dtype: Type = dict
    drop_empty: bool = True
    sequential: bool = True

    def parse(self, obj: Any, *args, **kwargs) -> Iterable:
        if isinstance(obj, Sequence if self.sequential else Iterable):
            iterable = map(lambda record: self.map(record, *args, **kwargs), obj)
            return filter(None, iterable) if self.drop_empty else iterable
        else:
            self.raise_parse_error()

    def map(self, record: Any, *args, **kwargs) -> Dict:
        return self.dtype(record, *args, **kwargs)


class QueryParser(RecordsParser):
    table_alias: str = "data"

    def __init__(self, obj: Any, *args, format: Literal["csv","json"] | None = None, **kwargs):
        super().__init__(obj, *args, format=format, **kwargs)

    def parse(self, obj: Any, *args, format: Literal["csv","json"] | None = None, **kwargs) -> Iterable:
        if isinstance(obj, Sequence if self.sequential else Iterable):
            return self.execute_query(obj, *args, format=format, **kwargs)
        else:
            self.raise_parse_error()

    def execute_query(self, obj: Sequence[Any], *args, format: Literal["csv","json"] | None = None, **kwargs) -> List[Any]:
        from linkmerce.utils.duckdb import execute_query
        query = self.make_query(*args, **kwargs)
        return execute_query(query, params={self.table_alias: obj}, format=format)

    def make_query(self, *args, **kwargs) -> str:
        return self.expr_table(enclose=False)

    def render_query(self, query: str, table: str = str(), **kwargs) -> str:
        from linkmerce.utils.jinja import render_string
        table = table or self.expr_table(enclose=True)
        return render_string(query, table=table, **kwargs)

    def expr(self, value: Any, type: str, alias: str = str(), safe: bool = False) -> str:
        type = type.upper()
        if type == "DATE":
            return self.expr_date(value, alias, safe)
        else:
            func = "TRY_CAST" if safe else "CAST"
            alias = f" AS {alias}" if alias else str()
            return f"{func}({value} AS {type})" + alias

    def expr_date(self, value: Any, alias: str = str(), safe: bool = False) -> str:
        alias = f" AS {alias}" if alias else str()
        if safe:
            return (f"DATE '{value}'" if value is not None else "NULL") + alias
        else:
            return f"DATE '{value}'" + alias

    def expr_table(self, enclose: bool = False) -> str:
        query = "SELECT {table}.* FROM (SELECT UNNEST(${table}) AS {table})".format(table=self.table_alias)
        return f"({query})" if enclose else query
