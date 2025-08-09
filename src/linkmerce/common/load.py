from __future__ import annotations

from abc import ABCMeta, abstractmethod

from typing import overload, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Literal, Type
    from types import TracebackType
    from duckdb import DuckDBPyConnection, DuckDBPyRelation
    from io import BytesIO
    from pathlib import Path
    import datetime as dt


class Connection(metaclass=ABCMeta):
    def __init__(self, **kwargs):
        self.set_connection(**kwargs)

    @abstractmethod
    def get_connection(self) -> Any:
        raise NotImplementedError("The 'get_connection' method must be implemented.")

    @abstractmethod
    def set_connection(self, **kwargs):
        raise NotImplementedError("The 'set_connection' method must be implemented.")

    @abstractmethod
    def close(self):
        raise NotImplementedError("The 'close' method must be implemented.")

    @abstractmethod
    def execute(self, *args, **kwargs) -> Any:
        raise NotImplementedError("The 'execute' method must be implemented.")

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, type: Type[BaseException], value: BaseException, traceback: TracebackType):
        self.close()

    ############################## Fetch ##############################

    def fetch_all_to_csv(self, query: str) -> list[tuple]:
        raise NotImplementedError("The 'fetch_all_to_csv' method must be implemented.")

    def fetch_all_to_json(self, query: str) -> list[dict]:
        raise NotImplementedError("The 'fetch_all_to_json' method must be implemented.")

    def fetch_all_to_parquet(self, query: str, file_name: str | None = None) -> BytesIO | None:
        raise NotImplementedError("The 'fetch_all_to_parquet' method must be implemented.")

    ############################ Expression ###########################

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

    def curret_date(
            self,
            type: Literal["DATE","STRING"] = "DATE",
            format: str | None = "%Y-%m-%d",
            interval: str | int | None = None,
        ) -> str:
        from linkmerce.utils.duckdb import curret_date
        return curret_date(type, format, interval)

    def curret_datetime(
            self,
            type: Literal["DATETIME","STRING"] = "DATETIME",
            format: str | None = "%Y-%m-%d %H:%M:%S",
            interval: str | int | None = None,
            tzinfo: str | None = None,
        ) -> str:
        from linkmerce.utils.duckdb import curret_datetime
        return curret_datetime(type, format, interval, tzinfo)


class DuckDBConnection(Connection):
    def __init__(self, **kwargs):
        self.set_connection(**kwargs)

    def get_connection(self) -> DuckDBPyConnection:
        return self.__conn

    def set_connection(self, **kwargs):
        import duckdb
        self.__conn = duckdb.connect(**kwargs)

    def close(self):
        try:
            self.get_connection().close()
        except:
            pass

    ############################# Execute #############################

    @overload
    def execute(self, query: str, **params) -> DuckDBPyRelation:
        ...

    @overload
    def execute(self, query: str, obj: list, **params) -> DuckDBPyRelation:
        ...

    def execute(self, query: str, obj: list | None = None, **params) -> DuckDBPyRelation:
        if obj is None:
            return self.get_connection().execute(query, parameters=(params or None))
        else:
            return self.get_connection().execute(query, parameters=dict(obj=obj, **params))

    ############################ Fetch CSV ############################

    @overload
    def fetch_all_to_csv(self, query: str, params: dict | None = None) -> list[tuple]:
        ...

    @overload
    def fetch_all_to_csv(self, *, table: str) -> Any:
        ...

    def fetch_all_to_csv(
            self,
            query: str | None = None,
            params: dict | None = None,
            *,
            table: str | None = None
        ) -> list[tuple]:
        from linkmerce.utils.duckdb import select_to_json
        if table:
            query = "SELECT * FROM {};".format(table)
        if not query:
            raise TypeError("DuckDBConnection.fetch_all_to_csv() missing 1 required positional argument: 'query'")
        return select_to_json(self.get_connection(), query, params)

    ############################ Fetch JSON ###########################

    @overload
    def fetch_all_to_json(self, query: str, params: dict | None = None) -> list[dict]:
        ...

    @overload
    def fetch_all_to_json(self, *, table: str) -> list[dict]:
        ...

    def fetch_all_to_json(
            self,
            query: str | None = None,
            params: dict | None = None,
            *,
            table: str | None = None
        ) -> list[dict]:
        from linkmerce.utils.duckdb import select_to_json
        if table:
            query = "SELECT * FROM {};".format(table)
        if not query:
            raise TypeError("DuckDBConnection.fetch_all_to_json() missing 1 required positional argument: 'query'")
        return select_to_json(self.get_connection(), query, params)

    ########################## Fetch Parquet ##########################

    @overload
    def fetch_all_to_parquet(self, query: str, file_name: str | None = None, params: dict | None = None) -> BytesIO | None:
        ...

    @overload
    def fetch_all_to_parquet(self, *, table: str, file_name: str | None = None, params: dict | None = None) -> BytesIO:
        ...

    def fetch_all_to_parquet(
            self,
            query: str | None = None,
            file_name: str | None = None,
            params: dict | None = None,
            *,
            table: str | None = None
        ) -> BytesIO | None:
        from linkmerce.utils.duckdb import select_to_parquet
        if table:
            query = "SELECT * FROM {};".format(table)
        if not query:
            raise TypeError("DuckDBConnection.fetch_all_to_parquet() missing 1 required positional argument: 'query'")
        return select_to_parquet(self.get_connection(), query, file_name, params)
