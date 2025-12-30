from __future__ import annotations

from abc import ABCMeta, abstractmethod

from typing import Union, overload, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Hashable, Literal, Sequence, Type, TypeVar
    from types import TracebackType
    _KT = TypeVar("_KT", Hashable)
    Expression = TypeVar("Expression", bound=str)
    TableName = TypeVar("TableName", bound=str)
    QueryKey = TypeVar("QueryKey", bound=str)

    from bs4 import BeautifulSoup, Tag

    from linkmerce.common.load import Connection, DuckDBConnection
    from linkmerce.common.models import Models

    from duckdb import DuckDBPyConnection, DuckDBPyRelation
    from pathlib import Path

JsonObject = Union[dict, list]


class Transformer(metaclass=ABCMeta):
    def __init__(self, **kwargs):
        ...

    @abstractmethod
    def transform(self, obj: Any, **kwargs) -> Any:
        raise NotImplementedError("The 'transform' method must be implemented.")

    def raise_parse_error(self, msg: str):
        from linkmerce.common.exceptions import ParseError
        raise ParseError(msg)

    def raise_request_error(self, msg: str):
        from linkmerce.common.exceptions import RequestError
        raise RequestError(msg)


###################################################################
########################## HTTP Response ##########################
###################################################################

class JsonTransformer(Transformer, metaclass=ABCMeta):
    dtype: type = dict
    path: list[_KT] | None = None

    def __init__(self, path: _KT | list[_KT] | None = None, **kwargs):
        if path is not None:
            self.path = [path] if isinstance(path, str) else path

    def transform(self, obj: Any, **kwargs) -> JsonObject:
        if isinstance(obj, self.dtype):
            if self.is_valid_response(obj):
                return self.parse(obj, **kwargs)
            else:
                self.raise_request_error("HTTP response is not valid.")
        else:
            self.raise_parse_error("Could not parse the HTTP response.")

    def parse(self, obj: JsonObject, **kwargs) -> JsonObject:
        if self.path is not None:
            from linkmerce.utils.map import hier_get
            return hier_get(obj, self.path)
        else:
            return obj

    def is_valid_response(self, obj: JsonObject) -> bool:
        return True


class HtmlTransformer(Transformer, metaclass=ABCMeta):
    selector: str | None = None
    mapping: dict[str,str] | None = None

    def __init__(self, selector: str | None = None, mapping: dict[str,str] | None = None, **kwargs):
        if isinstance(selector, str):
            self.selector = selector
        if isinstance(mapping, dict):
            self.mapping = mapping

    def transform(self, obj: BeautifulSoup, **kwargs) -> JsonObject:
        if isinstance(self.selector, str):
            obj = self.select(obj, self.selector)

        if isinstance(obj, list):
            return [self.parse(tag, **kwargs) for tag in obj]
        else:
            return self.parse(obj, **kwargs)

    def parse(self, obj: BeautifulSoup | Tag | list[Tag], **kwargs) -> JsonObject:
        if isinstance(self.mapping, dict):
            return {key: self.select(obj, selector) for key, selector in self.mapping.items()}
        else:
            raise NotImplementedError("The 'parse' method must be implemented.")

    def select(self, obj: BeautifulSoup | Tag, selector: str) -> Tag | list[Tag] | str | list[str]:
        from linkmerce.utils.parse import select
        return select(obj, selector)


###################################################################
############################# Database ############################
###################################################################

class DBTransformer(Transformer, metaclass=ABCMeta):
    queries: list[str] = ["create"]

    def __init__(
            self,
            db_info: dict = dict(),
            model_path: Literal["this"] | str | Path = "this",
            tables: dict | None = None,
            create_options: dict[str,TableName] | None = None,
        ):
        self.set_connection(**db_info)
        self.set_models(model_path)
        self.set_queries(keys=self.queries)
        self.set_tables(tables)
        if isinstance(create_options, dict):
            self.create(**create_options)

    @property
    def conn(self) -> Connection:
        return self.get_connection()

    @abstractmethod
    def transform(self, obj: Any, **kwargs) -> Any:
        raise NotImplementedError("The 'transform' method must be implemented.")

    def execute(self, *args, **kwargs) -> Any:
        return self.get_connection().execute(*args, **kwargs)

    def close(self):
        self.conn.close()

    ############################ Connection ###########################

    @abstractmethod
    def get_connection(self) -> Connection:
        raise NotImplementedError("The 'get_connection' method must be implemented.")

    @abstractmethod
    def set_connection(self, **kwargs):
        raise NotImplementedError("The 'set_connection' method must be implemented.")

    def __enter__(self) -> DBTransformer:
        return self

    def __exit__(self, type: Type[BaseException], value: BaseException, traceback: TracebackType):
        self.close()

    ############################## Models #############################

    def get_models(self) -> Models:
        return self.__models

    def set_models(self, models: Literal["this"] | str | Path = "this"):
        from linkmerce.common.models import Models
        self.__models = Models(self.default_models if models == "this" else models)

    @property
    def default_models(self) -> Path:
        from pathlib import Path
        root = Path(__file__).parent.parent
        return root / '/'.join(self.__class__.__module__.split('.')[1:-1]) / "models.sql"

    ############################# Queries #############################

    def get_queries(self) -> dict[str,str]:
        return self.__queires

    def set_queries(self, name: Literal["self"] | str = "self", keys: Sequence[str] | None = None):
        name = self.__class__.__name__ if name == "self" else name
        self.__queires = self.get_models().read_models(name, keys=(self.queries if keys is None else keys))

    def get_query(self, key: str, render: dict | None = None) -> str:
        if self.has_query(key):
            query = self.get_queries()[key]
            return self.render_query(query, **render) if isinstance(render, dict) else query
        else:
            raise KeyError(f"'{key}' query does not exist.")

    def has_query(self, key: str) -> bool:
        return key in self.get_queries()

    ############################## Tables #############################

    def get_table(self, name: str) -> TableName:
        return self.get_tables()[name[1:-1]] if name.startswith(':') and name.endswith(':') else name

    def get_tables(self) -> dict[str,TableName]:
        return self.__tables

    def set_tables(self, tables: dict[str,TableName] | None = None):
        self.__tables = tables if isinstance(tables, dict) else dict(default="data")

    ############################## Fetch ##############################

    def fetch_all(self, format: Literal["csv","json","parquet"], query: str) -> list[tuple] | list[dict] | bytes:
        return self.conn.fetch_all(format, query)

    def fetch_all_to_csv(self, query: str) -> list[tuple]:
        return self.conn.fetch_all_to_csv(query)

    def fetch_all_to_json(self, query: str) -> list[dict]:
        return self.conn.fetch_all_to_json(query)

    def fetch_all_to_parquet(self, query: str) -> bytes:
        return self.conn.fetch_all_to_parquet(query)

    ############################### CRUD ##############################

    def create(self, query: str = str(), key: str = "create", render: dict | None = None) -> Any:
        query = self._get_or_render_query(query, key, render)
        return self.conn.execute(query)

    def select(self, query: str = str(), key: str = "select", render: dict | None = None) -> Any:
        query = self._get_or_render_query(query, key, render)
        return self.conn.execute(query)

    def update(self, query: str = str(), key: str = "update", render: dict | None = None) -> Any:
        query = self._get_or_render_query(query, key, render)
        return self.conn.execute(query)

    def delete(self, query: str = str(), key: str = "delete", render: dict | None = None) -> Any:
        query = self._get_or_render_query(query, key, render)
        return self.conn.execute(query)

    def insert_into(self, query: str = str(), key: str = "insert", render: dict | None = None) -> Any:
        query = self._get_or_render_query(query, key, render)
        return self.conn.execute(query)

    def upsert_into(self, query: str = str(), key: str = "upsert", render: dict | None = None) -> Any:
        query = self._get_or_render_query(query, key, render)
        return self.conn.execute(query)

    ############################## Render #############################

    def render_query(self, query_: str, **kwargs) -> str:
        from linkmerce.utils.jinja import render_string
        return render_string(query_, **kwargs)

    def _get_or_render_query(self, query: str = str(), key: str = str(), render: dict | None = None) -> str:
        if query:
            return self.render_query(query, **render) if isinstance(render, dict) else query
        else:
            return self.get_query(key, render)


###################################################################
############################## DuckDB #############################
###################################################################

class DuckDBTransformer(DBTransformer, metaclass=ABCMeta):
    queries: list[str] = ["create"]

    def __init__(
            self,
            db_info: dict = dict(),
            model_path: Literal["this"] | str | Path = "this",
            tables: dict | None = None,
            create_options: dict | None = dict(),
        ):
        super().__init__(db_info, model_path, tables)
        if isinstance(create_options, dict):
            self.create_table(**create_options)

    @property
    def conn(self) -> DuckDBConnection:
        return self.get_connection()

    def get_connection(self) -> DuckDBConnection:
        return self.__conn

    def set_connection(self, conn: DuckDBConnection | None = None, **kwargs):
        from linkmerce.common.load import DuckDBConnection
        self.__conn = conn if isinstance(conn, DuckDBConnection) else  DuckDBConnection(**kwargs)

    def __enter__(self) -> DuckDBTransformer:
        return self

    ############################# Execute #############################

    @overload
    def execute(self, query: str, **params) -> DuckDBPyConnection:
        ...

    @overload
    def execute(self, query: str, obj: list, **params) -> DuckDBPyConnection:
        ...

    def execute(self, query: str, obj: list | None = None, **params) -> DuckDBPyConnection:
        if obj is None:
            return self.conn.execute(query, **params)
        else:
            return self.conn.execute(query, obj, **params)

    ############################### SQL ###############################

    @overload
    def sql(self, query: str, **params) -> DuckDBPyRelation:
        ...

    @overload
    def sql(self, query: str, obj: list, **params) -> DuckDBPyRelation:
        ...

    def sql(self, query: str, obj: list | None = None, **params) -> DuckDBPyRelation:
        if obj is None:
            return self.conn.sql(query, **params)
        else:
            return self.conn.sql(query, obj, **params)

    ########################### Fetch Table ###########################

    def fetch_all(
            self,
            format: Literal["csv","json","parquet"],
            table: Literal[":default:"] | TableName = ":default:",
        ) -> list[tuple] | list[tuple] | bytes:
        try:
            return getattr(self, f"fetch_all_to_{format}")(table)
        except AttributeError:
            raise ValueError("Invalid value for data format. Supported formats are: csv, json, parquet.")

    def fetch_all_to_csv(self, table: Literal[":default:"] | TableName = ":default:") -> list[tuple]:
        return self.conn.fetch_all_to_csv(f"SELECT * FROM {self.get_table(table)}")

    def fetch_all_to_json(self, table: Literal[":default:"] | TableName = ":default:") -> list[dict]:
        return self.conn.fetch_all_to_json(f"SELECT * FROM {self.get_table(table)}")

    def fetch_all_to_parquet(self, table: Literal[":default:"] | TableName = ":default:") -> bytes:
        return self.conn.fetch_all_to_parquet(f"SELECT * FROM {self.get_table(table)}")

    ############################### CRUD ##############################

    def create(self, query: str = str(), key: str = "create", render: dict | None = None, params: dict | None = None) -> DuckDBPyConnection:
        query = self._get_or_render_query(query, key, render)
        return self.conn.execute(query, **(params or dict()))

    def select(self, query: str = str(), key: str = "select", render: dict | None = None, params: dict | None = None) -> DuckDBPyConnection:
        query = self._get_or_render_query(query, key, render)
        return self.conn.execute(query, **(params or dict()))

    def update(self, query: str = str(), key: str = "update", render: dict | None = None, params: dict | None = None) -> DuckDBPyConnection:
        query = self._get_or_render_query(query, key, render)
        return self.conn.execute(query, **(params or dict()))

    def delete(self, query: str = str(), key: str = "delete", render: dict | None = None, params: dict | None = None) -> DuckDBPyConnection:
        query = self._get_or_render_query(query, key, render)
        return self.conn.execute(query, **(params or dict()))

    def insert_into(self, query: str = str(), key: str = "insert", render: dict | None = None, params: dict | None = None) -> DuckDBPyConnection:
        query = self._get_or_render_query(query, key, render)
        return self.conn.execute(query, **(params or dict()))

    def upsert_into(self, query: str = str(), key: str = "upsert", render: dict | None = None, params: dict | None = None) -> DuckDBPyConnection:
        query = self._get_or_render_query(query, key, render)
        return self.conn.execute(query, **(params or dict()))

    ############################ CRUD Table ###########################

    def create_table(
            self,
            query: str = str(),
            key: str = "create",
            table: Literal[":default:"] | TableName | None = ":default:",
            render: dict | None = None,
            params: dict | None = None,
            **kwargs
        ) -> DuckDBPyConnection:
        render = self._put_items_into_render_kwargs(render, table=table)
        return self.create(query, key, render, params)

    def select_from_table(
            self,
            query: str = str(),
            key: str = "select",
            table: Literal[":default:"] | TableName | None = ":default:",
            render: dict | None = None,
            params: dict | None = None,
            **kwargs
        ) -> DuckDBPyConnection:
        render = self._put_items_into_render_kwargs(render, table=table)
        return self.select(query, key, render, params)

    def update_table(
            self,
            query: str = str(),
            key: str = "update",
            table: Literal[":default:"] | TableName | None = ":default:",
            render: dict | None = None,
            params: dict | None = None,
            **kwargs
        ) -> DuckDBPyConnection:
        render = self._put_items_into_render_kwargs(render, table=table)
        return self.update(query, key, render, params)

    def delete_from_table(
            self,
            query: str = str(),
            key: str = "delete",
            table: Literal[":default:"] | TableName | None = ":default:",
            render: dict | None = None,
            params: dict | None = None,
            **kwargs
        ) -> DuckDBPyConnection:
        render = self._put_items_into_render_kwargs(render, table=table)
        return self.delete(query, key, render, params)

    def insert_into_table(
            self,
            obj: list,
            query: str = str(),
            key: str = "insert",
            table: Literal[":default:"] | TableName | None = ":default:",
            values: Literal[":default:"] | Expression | None = ":default:",
            render: dict | None = None,
            params: dict | None = None,
            **kwargs
        ) -> DuckDBPyConnection:
        if values is not None:
            values = self.expr_values(values, render=render)
        render = self._put_items_into_render_kwargs(render, table=table, values=values)
        return self.insert_into(query, key, render, dict(obj=obj, **(params or dict())))

    def upsert_into_table(
            self,
            obj: list,
            query: str = str(),
            key: str = "upsert",
            table: Literal[":default:"] | TableName | None = ":default:",
            values: Literal[":default:"] | Expression | QueryKey | None = ":default:",
            render: dict | None = None,
            params: dict | None = None,
            **kwargs
        ) -> DuckDBPyConnection:
        if values is not None:
            values = self.expr_values(values, render=render)
        render = self._put_items_into_render_kwargs(render, table=table, values=values)
        return self.upsert_into(query, key, render, dict(obj=obj, **(params or dict())))

    ############################## Render #############################

    def render_query(
            self,
            query_: str,
            table: Literal[":default:"] | TableName | None = None,
            array: Literal[":default:"] | Expression | None = None,
            **kwargs
        ) -> str:
        if table is not None:
            kwargs["table"] = self.get_table(table)
        if array is not None:
            kwargs["array"] = self.expr_array("obj") if array == ":default:" else array
        return super().render_query(query_, **kwargs)

    def _put_items_into_render_kwargs(self, render: dict | None = None, **items) -> str:
        if items:
            render = render.copy() if isinstance(render, dict) else dict()
            render.update({key: value for key, value in items.items() if value is not None})
        return render

    ############################ Expression ###########################

    def expr_array(self, array: str = "obj") -> str:
        return f"(SELECT {array}.* FROM (SELECT UNNEST(${array}) AS {array}))"

    def expr_values(
            self,
            values: Literal[":default:"] | Expression | QueryKey = ":default:",
            array: Literal[":default:"] | Expression | None = ":default:",
            render: dict | None = None,
        ) -> str:
        if values.startswith(':') and values.endswith(':'):
            key = "select" if values == ":default:" else values[1:-1]
            render = dict(render or dict(), array=array)
            query = self.get_query(key, render=render).strip()
            return query[:-1] if query.endswith(';') else query
        elif (render is not None) or (array is not None):
            render = dict(render or dict(), array=array)
            return self.render_query(values, **render)
        else:
            return values
