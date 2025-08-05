from __future__ import annotations

import duckdb
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Literal, Sequence
    from duckdb import DuckDBPyConnection

DEFAULT_TEMP_TABLE = "temp_table"

NAME, TYPE = 0, 0


def with_connection(func):
    @functools.wraps(func)
    def wrapper(*args, conn: DuckDBPyConnection | None = None, **kwargs):
        if conn is None:
            with duckdb.connect() as conn:
                return func(*args, conn=conn, **kwargs)
        else:
            return func(*args, conn=conn, **kwargs)
    return wrapper


def get_columns(conn: DuckDBPyConnection, table: str) -> list[str]:
    return [column[NAME] for column in conn.execute(f"DESCRIBE {table}").fetchall()]


###################################################################
############################## Create #############################
###################################################################

def create_table(
        conn: DuckDBPyConnection,
        table: str,
        data: list[dict],
        option: Literal["replace", "ignore"] | None = None,
        temp: bool = False
    ):
    source = "SELECT data.* FROM (SELECT UNNEST($data) AS data)"
    query = f"{_create(option, temp)} {table} AS ({source})"
    conn.execute(query, parameters={"data": data})


def _create(option: Literal["replace", "ignore"] | None = None, temp: bool = False) -> str:
    temp = "TEMP" if temp else str()
    if option == "replace":
        return f"CREATE OR REPLACE {temp} TABLE"
    elif option == "ignore":
        return f"CREATE {temp} TABLE IF NOT EXISTS"
    else:
        return f"CREATE {temp} TABLE"


###################################################################
############################## Select #############################
###################################################################

def select_to_csv(
        query: str,
        params: dict | None = None,
        conn: DuckDBPyConnection | None = None,
    ) -> list[tuple]:
    relation = (conn if conn is not None else duckdb).execute(query, parameters=params)
    columns = [column[NAME] for column in relation.description]
    return [columns] + relation.fetchall()


def select_to_json(
        query: str,
        params: dict | None = None,
        conn: DuckDBPyConnection | None = None,
    ) -> list[dict]:
    relation = (conn if conn is not None else duckdb).execute(query, parameters=params)
    columns = [column[NAME] for column in relation.description]
    return [dict(zip(columns, row)) for row in relation.fetchall()]


###################################################################
############################# Datetime ############################
###################################################################

def curret_date(
        type: Literal["DATE","STRING"] = "DATE",
        format: str | None = "%Y-%m-%d",
        interval: str | int | None = None,
    ) -> str:
    expr = "CURRENT_DATE"
    if interval is not None:
        expr = f"CAST(({expr} {_interval(interval)}) AS DATE)"
    if (type.upper() == "STRING") and format:
        return f"STRFTIME({expr}, '{format}')"
    return expr if type.upper() == "DATE" else "NULL"


def curret_datetime(
        type: Literal["DATETIME","STRING"] = "DATETIME",
        format: str | None = "%Y-%m-%d %H:%M:%S",
        interval: str | int | None = None,
        tzinfo: str | None = None,
    ) -> str:
    expr = "CURRENT_TIMESTAMP {}".format(f"AT TIME ZONE '{tzinfo}'" if tzinfo else str()).strip()
    expr = f"{expr} {_interval(interval)}".strip()
    if format:
        expr = f"STRFTIME({expr}, '{format}')"
        if type.upper() == "DATETIME":
            return f"CAST({expr} AS TIMESTAMP)"
    return expr if type.upper() == "DATETIME" else "NULL"


def _interval(value: str | int | None = None) -> str:
    if isinstance(value, str):
        return value
    elif isinstance(value, int):
        return "{} INTERVAL {} DAY".format('-' if value < 0 else '+', abs(value))
    else:
        return str()


###################################################################
############################## Rename #############################
###################################################################

@with_connection
def rename_keys(
        data: list[dict],
        rename: dict[str,str],
        *,
        conn: DuckDBPyConnection | None = None,
        temp_table: str = DEFAULT_TEMP_TABLE,
    ) -> list[dict]:
    create_table(conn, temp_table, data, option="ignore", temp=True)
    def alias(column: str) -> str:
        return f"{column} AS {rename[column]}" if column in rename else column
    columns = ", ".join(map(alias, get_columns(conn, temp_table)))
    query = f"SELECT {columns} FROM {temp_table};"
    return select_to_json(query, conn=conn)


###################################################################
############################# Group By ############################
###################################################################

@with_connection
def groupby(
        data: list[dict],
        by: str | Sequence[str],
        agg: str | dict[str,Literal["count","sum","avg","min","max","first","last","list"]],
        dropna: bool = True,
        *,
        conn: DuckDBPyConnection | None = None,
        temp_table: str = DEFAULT_TEMP_TABLE,
    ) -> list[dict]:
    create_table(conn, temp_table, data, option="ignore", temp=True)
    by = [by] if isinstance(by, str) else by
    query = f"SELECT {', '.join(by)}, {_agg(agg)} FROM {temp_table} {_groupby(by, dropna)};"
    return select_to_json(query, conn=conn)


def _groupby(by: Sequence[str], dropna: bool = True):
    where = "WHERE " + " AND ".join([f"{col} IS NOT NULL" for col in by]) if dropna else str()
    groupby = "GROUP BY {}".format(", ".join(by))
    return f"{where} {groupby}"


def _agg(func: str | dict[str,Literal["count","sum","avg","min","max","first","last","list"]]) -> str:
    if isinstance(func, dict):
        def render(col: str, agg: str) -> str:
            if agg in {"count","sum","avg","min","max"}:
                return f"{agg.upper()}({col})"
            elif agg in {"first","last","list"}:
                return f"{agg.upper()}({col}) FILTER (WHERE {col} IS NOT NULL)"
            else:
                return agg
        return ", ".join([f"{render(col, agg)} AS {col}" for col, agg in func.items()])
    else:
        return func


@with_connection
def combine_first(
        *data: list[dict],
        index: str | Sequence[str],
        dropna: bool = True,
        conn: DuckDBPyConnection | None = None,
        temp_table: str = DEFAULT_TEMP_TABLE,
    ) -> list[dict]:
    from itertools import chain
    create_table(conn, temp_table, list(chain.from_iterable(data)), option="ignore", temp=True)
    index = [index] if isinstance(index, str) else index
    agg = _agg({col: "first" for col in get_columns(conn, temp_table) if col not in index})
    query = f"SELECT {', '.join(index)}, {agg} FROM {temp_table} {_groupby(index, dropna)};"
    return select_to_json(query, conn=conn)


###################################################################
########################### Partition By ##########################
###################################################################

class Partition:
    def __init__(
            self,
            data: list[dict],
            field: str,
            type: str | None = None,
            condition: str | None = None,
            sort: bool = True,
            temp_table: str = DEFAULT_TEMP_TABLE,
        ):
        self.conn = duckdb.connect()
        self.table = temp_table
        try:
            self.set_data(data)
            self.set_field(field, type)
            self.set_partitions(condition, sort)
        except:
            ...

    def set_data(self, data: list[dict]):
        create_table(self.conn, self.table, data, option="ignore", temp=True)

    def set_field(self, field: str, type: str | None = None):
        if field not in get_columns(self.conn, self.table):
            field = "_PARTITIONFIELD"
            if not type:
                type = self.conn.execute(f"SELECT {field} FROM {self.table} LIMIT 1").description[0][TYPE]
            self.conn.execute(f"ALTER TABLE {self.table} ADD COLUMN {field} {type};")
            self.conn.execute(f"UPDATE {self.table} SET {field} = {field};")
        self.field = field

    def set_partitions(self,  condition: str | None = None, sort: bool = True):
        query = f"SELECT DISTINCT {self.field} FROM {self.table} {_where(condition, self.field)};"
        if sort:
            self.partitions = sorted(map(lambda x: x[0], self.conn.execute(query).fetchall()))
        else:
            self.partitions = [row[0] for row in self.conn.execute(query).fetchall()]

    def __iter__(self) -> Partition:
        self.index = 0
        return self

    def __next__(self) -> list[dict]:
        if self.index < len(self):
            exclude = "EXCLUDE (_PARTITIONFIELD)" if self.field == "_PARTITIONFIELD" else str()
            query = f"SELECT * {exclude} FROM temp_table WHERE {self.field} = {_quote(self.partitions[self.index])};"
            self.index += 1
            return select_to_json(query, conn=self.conn)
        else:
            raise StopIteration

    def __len__(self) -> int:
        return len(self.partitions)

    def __exit__(self):
        self.close()

    def close(self):
        try:
            self.conn.close()
        except:
            pass


def _where(condition: str | None = None, field: str | None = None, **kwargs) -> str:
    if condition is not None:
        if condition.split(' ', maxsplit=1)[0].upper() == "WHERE":
            return condition
        elif field:
            return f"WHERE {field} {condition}"
        else:
            return str()
    else:
        return str()


def _quote(value: Any) -> str:
    import datetime as dt
    return f"'{value}'" if isinstance(value, (str,dt.date)) else str(value)
