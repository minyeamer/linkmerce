from __future__ import annotations

from abc import ABCMeta, abstractmethod
from linkmerce.common.tasks import Task
from pathlib import Path

from typing import overload, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Callable, Literal, Sequence, Type
    from types import TracebackType

    from duckdb import DuckDBPyConnection, DuckDBPyRelation

NAME, TYPE = 0, 0


def concat_sql(*statement: str, drop_empty: bool = True, sep=' ', terminate: bool = True) -> str:
    query = sep.join(filter(None, statement) if drop_empty else statement)
    return query + ';' if terminate and not query.endswith(';') else query


def where(where_clause: str | None = None, default: str | None = None) -> str:
    if not (where_clause or default):
        return str()
    return f"WHERE {where_clause or default}"


def csv_to_json(obj: list[tuple], header: int | list[str] = 0) -> list[dict]:
    if isinstance(header, int):
        header, obj = obj[header], obj[header+1:]
    return [dict(zip(header, row)) for row in obj]


def json_to_csv(obj: list[dict], header: int | list[str] = 0) -> list[tuple]:
    if isinstance(header, int):
        header = list(obj[header].keys())
    return [header] + [[row.get(key) for key in header] for row in obj]


def save_to_csv(obj: list[tuple], file_path: str | Path, encoding: str | None = "utf-8", **kwargs):
    import csv
    with open(file_path, 'w', newline='', encoding=encoding) as file:
        writer = csv.writer(file, **kwargs)
        writer.writerows(obj)


def save_to_json(obj: list[dict], file_path: str | Path, encoding: str | None = "utf-8", **kwargs):
    import json
    with open(file_path, 'w', encoding=encoding) as file:
        json.dump(obj, file, **kwargs)


def run_with_tempfile(func: Callable[[str],Any], values: bytes, mode = "w+b", suffix: str | None = None, **kwargs) -> Any:
    import tempfile
    with tempfile.NamedTemporaryFile(mode, suffix=suffix, **kwargs) as temp_file:
        temp_file.write(values)
        return func(temp_file.name)


def write_tempfile(write_func: Callable[[str],None], mode = "w+b", suffix: str | None = None, **kwargs) -> bytes:
    import tempfile
    with tempfile.NamedTemporaryFile(mode, suffix=suffix, **kwargs) as temp_file:
        file_path = temp_file.name
        write_func(file_path)
        with open(file_path, "rb") as file:
            return file.read()


###################################################################
############################ Connection ###########################
###################################################################

class Connection(metaclass=ABCMeta):
    def __init__(self, **kwargs):
        self.set_connection(**kwargs)

    @property
    def conn(self) -> Any:
        return self.get_connection()

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

    def fetch_all(self, format: Literal["csv","json","parquet"], query: str) -> list[tuple] | list[dict] | bytes:
        raise NotImplementedError("The 'fetch_all' method must be implemented.")

    def fetch_all_to_csv(self, query: str) -> list[tuple]:
        raise NotImplementedError("The 'fetch_all_to_csv' method must be implemented.")

    def fetch_all_to_json(self, query: str) -> list[dict]:
        raise NotImplementedError("The 'fetch_all_to_json' method must be implemented.")

    def fetch_all_to_parquet(self, query: str) -> bytes:
        raise NotImplementedError("The 'fetch_all_to_parquet' method must be implemented.")

    ############################ Expression ###########################

    def expr_cast(self, value: Any | None, type: str, alias: str = str(), safe: bool = False) -> str:
        cast = "TRY_CAST" if safe else "CAST"
        alias = f" AS {alias}" if alias else str()
        return f"{cast}({self.expr_value(value)} AS {type.upper()})" + alias

    def expr_create(self, option: Literal["replace","ignore"] | None = None, temp: bool = False) -> str:
        temp = "TEMP" if temp else str()
        if option == "replace":
            return f"CREATE OR REPLACE {temp} TABLE"
        elif option == "ignore":
            return f"CREATE {temp} TABLE IF NOT EXISTS"
        else:
            return f"CREATE {temp} TABLE"

    def expr_value(self, value: Any | None) -> str:
        import datetime as dt
        if value is None:
            return "NULL"
        elif isinstance(value, (float,int)):
            return str(value)
        else:
            return f"'{value}'"

    def expr_now(
            self,
            type: Literal["DATETIME","STRING"] = "DATETIME",
            format: str | None = "%Y-%m-%d %H:%M:%S",
            interval: str | int | None = None,
            tzinfo: str | None = None,
        ) -> str:
        expr = "CURRENT_TIMESTAMP {}".format(f"AT TIME ZONE '{tzinfo}'" if tzinfo else str()).strip()
        expr = f"{expr} {self.expr_interval(interval)}".strip()
        if format:
            expr = f"STRFTIME({expr}, '{format}')"
            if type.upper() == "DATETIME":
                return f"CAST({expr} AS TIMESTAMP)"
        return expr if type.upper() == "DATETIME" else "NULL"

    def expr_today(
            self,
            type: Literal["DATE","STRING"] = "DATE",
            format: str | None = "%Y-%m-%d",
            interval: str | int | None = None,
        ) -> str:
        expr = "CURRENT_DATE"
        if interval is not None:
            expr = f"CAST(({expr} {self.expr_interval(interval)}) AS DATE)"
        if (type.upper() == "STRING") and format:
            return f"STRFTIME({expr}, '{format}')"
        return expr if type.upper() == "DATE" else "NULL"

    def expr_interval(days: str | int | None = None) -> str:
        if isinstance(days, str):
            return days
        elif isinstance(days, int):
            return "{} INTERVAL {} DAY".format('-' if days < 0 else '+', abs(days))
        else:
            return str()


###################################################################
############################## DuckDB #############################
###################################################################

class DuckDBConnection(Connection):
    def __init__(self, tzinfo: str | None = None, **kwargs):
        self.set_connection(tzinfo, **kwargs)

    @property
    def conn(self) -> DuckDBPyConnection:
        return self.get_connection()

    def get_connection(self) -> DuckDBPyConnection:
        return self.__conn

    def set_connection(self, tzinfo: str | None = None, **kwargs):
        import duckdb
        self.__conn = duckdb.connect(**kwargs)
        if tzinfo is not None:
            self.conn.execute(f"SET TimeZone = '{tzinfo}';")

    def close(self):
        try:
            self.conn.close()
        except:
            pass

    def __enter__(self) -> DuckDBConnection:
        return self

    def __exit__(self, type: Type[BaseException], value: BaseException, traceback: TracebackType):
        self.close()

    ############################# Execute #############################

    @overload
    def execute(self, query: str, **params) -> DuckDBPyConnection:
        ...

    @overload
    def execute(self, query: str, obj: list, **params) -> DuckDBPyConnection:
        ...

    def execute(self, query: str, obj: list | None = None, **params) -> DuckDBPyConnection:
        if obj is None:
            return self.conn.execute(query, parameters=(params or None))
        else:
            return self.conn.execute(query, parameters=dict(obj=obj, **params))

    ############################### SQL ###############################

    @overload
    def sql(self, query: str, **params) -> DuckDBPyRelation:
        ...

    @overload
    def sql(self, query: str, obj: list, **params) -> DuckDBPyRelation:
        ...

    def sql(self, query: str, obj: list | None = None, **params) -> DuckDBPyRelation:
        if obj is None:
            return self.conn.sql(query, params=(params or None))
        else:
            return self.conn.sql(query, params=dict(obj=obj, **params))

    ############################## Fetch ##############################

    def fetch_all(
            self,
            format: Literal["csv","json","parquet"],
            query: str,
            params: dict | None = None,
            save_to: str | Path | None = None,
        ) -> list[tuple] | list[tuple] | bytes | None:
        try:
            return getattr(self, f"fetch_all_to_{format}")(query, params, save_to)
        except AttributeError:
            raise ValueError("Invalid value for data format. Supported formats are: csv, json, parquet.")

    def fetch_all_to_csv(
            self,
            query: str,
            params: dict | None = None,
            save_to: str | Path | None = None,
        ) -> list[tuple] | None:
        relation = self.conn.execute(query, parameters=params)
        results = [tuple(self.get_columns(relation))] + relation.fetchall()
        if save_to:
            return save_to_csv(results, save_to, delimiter=',')
        else:
            return results

    def fetch_all_to_json(
            self,
            query: str,
            params: dict | None = None,
            save_to: str | Path | None = None,
        ) -> list[dict] | None:
        relation = self.conn.execute(query, parameters=params)
        columns = self.get_columns(relation)
        results = [dict(zip(columns, row)) for row in relation.fetchall()]
        if save_to:
            return save_to_json(results, save_to, indent=2, ensure_ascii=False, default=str)
        else:
            return results

    def fetch_all_to_parquet(
            self,
            query: str,
            params: dict | None = None,
            save_to: str | Path | None = None,
        ) -> bytes | None:
        relation = self.conn.sql(query, params=params)
        if save_to:
            return relation.to_parquet(save_to)
        else:
            def to_parquet(temp_file: str):
                relation.to_parquet(temp_file)
            return write_tempfile(to_parquet, mode="w+b", suffix=".parquet")

    ############################### Read ##############################

    def read(
            self,
            format: Literal["csv","json","parquet"],
            values: list[tuple] | list[dict] | bytes | str | Path,
            params: dict | None = None,
            prefix: str | None = None,
            suffix: str | None = None,
        ) -> DuckDBPyConnection:
        try:
            return getattr(self, f"read_{format}")(values, params, prefix, suffix)
        except AttributeError:
            raise ValueError("Invalid value for data format. Supported formats are: csv, json, parquet.")

    def read_csv(
            self,
            values: list[tuple] | str | Path,
            params: dict | None = None,
            prefix: str | None = None,
            suffix: str | None = None,
        ) -> DuckDBPyConnection:
        if isinstance(values, (str,Path)):
            query = f"SELECT * FROM read_csv('{values}')"
        else:
            query = "SELECT values.* FROM (SELECT UNNEST($values) AS values)"
            params = dict(params or dict(), values=csv_to_json(values))
        return self.conn.execute(concat_sql(prefix, query, suffix), parameters=params)

    def read_json(
            self,
            values: list[dict] | str | Path,
            params: dict | None = None,
            prefix: str | None = None,
            suffix: str | None = None,
        ) -> DuckDBPyConnection:
        if isinstance(values, (str,Path)):
            query = f"SELECT * FROM read_json_auto('{values}')"
        else:
            query = "SELECT values.* FROM (SELECT UNNEST($values) AS values)"
            params = dict(params or dict(), values=values)
        return self.conn.execute(concat_sql(prefix, query, suffix), parameters=params)

    def read_parquet(
            self,
            values: bytes | str | Path,
            params: dict | None = None,
            prefix: str | None = None,
            suffix: str | None = None,
        ) -> DuckDBPyConnection:
        if isinstance(values, (str,Path)):
            query = f"SELECT * FROM read_parquet('{values}')"
            return self.conn.execute(concat_sql(prefix, query, suffix), parameters=params)
        else:
            def create_table(temp_file: str) -> DuckDBPyConnection:
                query = concat_sql(prefix, f"SELECT * FROM read_parquet('{temp_file}')", suffix)
                return self.conn.execute(query, parameters=params)
            return run_with_tempfile(create_table, values, mode="w+b", suffix=".parquet")

    ############################## Create #############################

    def create_table(
            self,
            table: str,
            values: list[tuple] | list[dict] | bytes | str | Path,
            format: Literal["csv","json","parquet"],
            option: Literal["replace","ignore"] | None = None,
            temp: bool = False,
            params: dict | None = None,
        ) -> DuckDBPyConnection:
        try:
            return getattr(self, f"create_table_from_{format}")(table, values, option, temp, params)
        except AttributeError:
            raise ValueError("Invalid value for data format. Supported formats are: csv, json, parquet.")

    def create_table_from_csv(
            self,
            table: str,
            values: list[tuple] | str | Path,
            option: Literal["replace","ignore"] | None = None,
            temp: bool = False,
            params: dict | None = None,
        ) -> DuckDBPyConnection:
        return self.read_csv(values, params=params, prefix=f"{self.expr_create(option, temp)} {table} AS")

    def create_table_from_json(
            self,
            table: str,
            values: list[dict] | str | Path,
            option: Literal["replace","ignore"] | None = None,
            temp: bool = False,
            params: dict | None = None,
        ) -> DuckDBPyConnection:
        return self.read_json(values, params=params, prefix=f"{self.expr_create(option, temp)} {table} AS")

    def create_table_from_parquet(
            self,
            table: str,
            values: bytes | str | Path,
            option: Literal["replace","ignore"] | None = None,
            temp: bool = False,
            params: dict | None = None,
        ) -> DuckDBPyConnection:
        return self.read_parquet(values, params=params, prefix=f"{self.expr_create(option, temp)} {table} AS")

    def copy_table(
            self,
            source_table: str,
            target_table: str,
            limit: int | None = 0,
            option: Literal["replace","ignore"] | None = None,
            temp: bool = False,
        ) -> DuckDBPyConnection:
        limit_ = f"LIMIT {limit}" if isinstance(limit, int) else None
        query = concat_sql(f"{self.expr_create(option, temp)} {target_table} AS SELECT * FROM {source_table}", limit_)
        return self.conn.execute(query)

    ############################## Insert #############################

    def insert_into_table(
            self,
            table: str,
            values: list[tuple] | list[dict] | bytes | str | Path,
            format: Literal["csv","json","parquet"],
            on_conflict: str | None = None,
            params: dict | None = None,
        ) -> DuckDBPyConnection:
        try:
            return getattr(self, f"insert_into_table_from_{format}")(table, values, on_conflict, params)
        except AttributeError:
            raise ValueError("Invalid value for data format. Supported formats are: csv, json, parquet.")

    def insert_into_table_from_csv(
            self,
            table: str,
            values: list[tuple] | str | Path,
            on_conflict: str | None = None,
            params: dict | None = None,
        ) -> DuckDBPyConnection:
        suffix = f"ON CONFLICT {on_conflict}" if on_conflict else None
        return self.read_csv(values, params=params, prefix=f"INSERT INTO {table}", suffix=suffix)

    def insert_into_table_from_json(
            self,
            table: str,
            values: list[dict] | str | Path,
            on_conflict: str | None = None,
            params: dict | None = None,
        ) -> DuckDBPyConnection:
        suffix = f"ON CONFLICT {on_conflict}" if on_conflict else None
        return self.read_json(values, params=params, prefix=f"INSERT INTO {table}", suffix=suffix)

    def insert_into_table_from_parquet(
            self,
            table: str,
            values: bytes | str | Path,
            on_conflict: str | None = None,
            params: dict | None = None,
        ) -> DuckDBPyConnection:
        suffix = f"ON CONFLICT {on_conflict}" if on_conflict else None
        return self.read_parquet(values, params=params, prefix=f"INSERT INTO {table}", suffix=suffix)

    ############################# Group By ############################

    def groupby(
            self,
            source: str,
            by: str | Sequence[str],
            agg: str | dict[str,Literal["count","sum","avg","min","max","first","last","list"]],
            dropna: bool = True,
            params: dict | None = None,
        ) -> DuckDBPyRelation:
        by = [by] if isinstance(by, str) else by
        where = "WHERE " + " AND ".join([f"{col} IS NOT NULL" for col in by]) if dropna else None
        groupby = "GROUP BY {}".format(", ".join(by))
        query = concat_sql(f"SELECT {', '.join(by)}, {self.agg(agg)} FROM {source}", where, groupby)
        return self.conn.sql(query, params=params)

    def agg(self, func: str | dict[str,Literal["count","sum","avg","min","max","first","last","list"]]) -> str:
        if isinstance(func, dict):
            def render(col: str, agg: str) -> str:
                if agg in {"count","sum","avg","min","max"}:
                    return f"{agg.upper()}({col})"
                elif agg in {"first","last","list"}:
                    return f"{agg.upper()}({col}) FILTER (WHERE {col} IS NOT NULL)"
                else:
                    return f"{agg}({col})"
            return ", ".join([f"{render(col, agg)} AS {col}" for col, agg in func.items()])
        else:
            return func

    ############################## Utils ##############################

    def table_exists(self, table: str) -> bool:
        query = f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table}' LIMIT 1;"
        return bool(self.conn.execute(query).fetchone())

    def table_has_rows(self, table: str) -> bool:
        if self.table_exists(table):
            query = f"SELECT 1 FROM {table} LIMIT 1;"
            return bool(self.conn.execute(query).fetchone())
        return False

    def count_table(self, table: str) -> int:
        return self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchall()[0][0]

    def get_columns(self, obj: str | DuckDBPyConnection) -> list[str]:
        if isinstance(obj, str):
            obj = self.conn.execute(f"DESCRIBE {obj}")
            return [column[NAME] for column in obj.fetchall()]
        else:
            return [column[NAME] for column in obj.description]

    def has_column(self, obj: str | DuckDBPyConnection, column: str) -> bool:
        return column in self.get_columns(obj)

    def unique(self, table: str, expr: str, ascending: bool | None = None, where_clause: str | None = None) -> list:
        select = f"SELECT DISTINCT {expr} AS expr FROM {table}"
        order_by = "ORDER BY expr {}".format({True:"ASC", False:"DESC"}[ascending]) if isinstance(ascending, bool) else None
        query = concat_sql(select, where(where_clause), order_by)
        return [row[0] for row in self.conn.execute(query).fetchall()]


###################################################################
############################# Iterator ############################
###################################################################

class DuckDBIterator(Task):
    temp_table: str = "temp_table"

    def __init__(self, conn: DuckDBConnection, format: Literal["csv","json","parquet"]):
        self.conn = conn
        self.format = format
        self.table = str()
        self.partitions = [dict()]
        self.index = 0

    def run(self):
        ...

    def from_table(self, table: str) -> DuckDBIterator:
        return self.setattr("table", table)

    def from_values(
            self,
            values: list[tuple] | list[dict] | bytes | str | Path,
            format: Literal["csv","json","parquet"],
            params: dict | None = None,
        ) -> DuckDBIterator:
        self.conn.create_table(self.temp_table, values, format, option="replace", temp=True, params=params)
        return self.setattr("table", self.temp_table)

    def partition_by(
            self,
            by: str | list[str],
            ascending: bool | None = True,
            where_clause: str | None = None,
            if_errors: Literal["ignore","raise"] = "raise",
        ) -> DuckDBIterator:
        from linkmerce.utils.tqdm import _expand_kwargs
        map_partitions = dict()
        for expr in ([by] if isinstance(by, str) else by):
            if if_errors == "ignore":
                from duckdb import BinderException
                try:
                    map_partitions[expr] = self.conn.unique(self.table, expr, ascending, where_clause)
                except BinderException:
                    continue
            else:
                map_partitions[expr] = self.conn.unique(self.table, expr, ascending, where_clause)
        return self.setattr("partitions", _expand_kwargs(**map_partitions))

    def __iter__(self) -> DuckDBIterator:
        self.index = 0
        return self

    def __next__(self) -> list[tuple] | list[tuple] | bytes:
        if self.index >= len(self):
            raise StopIteration
        map_partition = self.partitions[self.index]
        where_clause = " AND ".join([f"{expr} = {self.conn.expr_value(value)}" for expr, value in map_partition.items()])
        query = concat_sql(f"SELECT * FROM {self.table}", where(where_clause))
        results = self.conn.fetch_all(self.format, query)
        self.index += 1
        return results

    def __len__(self) -> int:
        return len(self.partitions)
