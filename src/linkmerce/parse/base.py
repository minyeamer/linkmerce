from __future__ import annotations

from abc import ABCMeta, abstractmethod

from typing import Iterable, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Dict, List, Literal, Sequence, Tuple, Type, TypeVar
    import datetime as dt
    _ALIAS = TypeVar("_ALIAS", str)
    _DATE = TypeVar("_DATE", str, dt.date, None)


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


class QueryParser(RecordsParser, metaclass=ABCMeta):
    format: Literal["csv","json"] = "json"
    table_alias: str = "data"

    def __init__(self, obj: Any, *args, **kwargs):
        super().__init__(obj, *args, **kwargs)

    @abstractmethod
    def make_query(self, *args, **kwargs) -> str:
        raise NotImplementedError("The 'make_query' method must be implemented.")

    def parse(self, obj: Any, *args, **kwargs) -> Iterable:
        if isinstance(obj, Sequence if self.sequential else Iterable):
            query = self.make_query(*args, **kwargs)
            return self.select(obj, query)
        else:
            self.raise_parse_error()

    def select(self, obj: Any, query: str) -> List[Any]:
        if self.format == "csv":
            from linkmerce.utils.duckdb import select_to_csv as select
        elif self.format == "json":
            from linkmerce.utils.duckdb import select_to_json as select
        else:
            raise ValueError("Invalid format. Supported formats are: csv, json.")
        return select(query, params={self.table_alias: obj})

    def render_query(self, query: str, table: str = str(), **kwargs) -> str:
        from linkmerce.utils.jinja import render_string
        table = table or self.expr_table(enclose=True)
        return render_string(query, table=table, **kwargs)

    def build_date_part(self, *args: Tuple[_ALIAS,_DATE], safe: bool = True, sep: str = ", ") -> str:
        date_fields = [self.expr_date(value, alias=alias, safe=safe) for alias, value in args]
        return sep.join(date_fields)

    def expr(self, value: Any, type: str, alias: str = str(), safe: bool = False) -> str:
        type = type.upper()
        if type == "DATE":
            return self.expr_date(value, alias, safe)
        else:
            func = "TRY_CAST" if safe else "CAST"
            alias = f" AS {alias}" if alias else str()
            return f"{func}({value} AS {type})" + alias

    def expr_date(self, value: dt.date | str | None = None, alias: str = str(), safe: bool = False) -> str:
        alias = f" AS {alias}" if alias else str()
        if safe:
            return (f"DATE '{value}'" if value is not None else "NULL") + alias
        else:
            return f"DATE '{value}'" + alias

    def expr_table(self, enclose: bool = False) -> str:
        query = "SELECT {table}.* FROM (SELECT UNNEST(${table}) AS {table})".format(table=self.table_alias)
        return f"({query})" if enclose else query

    def curret_date(
            self,
            type: Literal["DATE","STRING"] = "DATE",
            format: str | None = "%Y-%m-%d",
        ) -> str:
        from linkmerce.utils.duckdb import curret_date
        return curret_date(type, format)

    def curret_datetime(
            self,
            type: Literal["DATETIME","STRING"] = "DATETIME",
            format: str | None = "%Y-%m-%d %H:%M:%S",
            tzinfo: str | None = None,
        ) -> str:
        from linkmerce.utils.duckdb import curret_datetime
        return curret_datetime(type, format, tzinfo)
