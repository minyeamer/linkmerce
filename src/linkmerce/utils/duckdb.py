from __future__ import annotations

import duckdb
import functools

from typing import Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Generator, List, Literal, Sequence, Tuple
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


def get_columns(conn: DuckDBPyConnection, table: str) -> List[str]:
    return [column[NAME] for column in conn.execute(f"DESCRIBE {table}").fetchall()]


###################################################################
############################## Create #############################
###################################################################

def create_table(
        conn: DuckDBPyConnection,
        table: str,
        data: List[Dict],
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
        params: Dict | None = None,
        conn: DuckDBPyConnection | None = None,
    ) -> List[Tuple]:
    relation = (conn if conn is not None else duckdb).execute(query, parameters=params)
    columns = [column[NAME] for column in relation.description]
    return [columns] + relation.fetchall()


def select_to_json(
        query: str,
        params: Dict | None = None,
        conn: DuckDBPyConnection | None = None,
    ) -> List[Dict]:
    relation = (conn if conn is not None else duckdb).execute(query, parameters=params)
    columns = [column[NAME] for column in relation.description]
    return [dict(zip(columns, row)) for row in relation.fetchall()]


###################################################################
############################# Datetime ############################
###################################################################

def curret_date(
        type: Literal["DATE","STRING"] = "DATE",
        format: str | None = "%Y-%m-%d",
    ) -> str:
    expr = "CURRENT_DATE"
    if format:
        expr = f"STRFTIME({expr}, '{format}')"
        if type.upper() == "DATE":
            return f"CAST({expr} AS DATE)"
    return expr


def curret_datetime(
        type: Literal["DATETIME","STRING"] = "DATETIME",
        format: str | None = "%Y-%m-%d %H:%M:%S",
        tzinfo: str | None = None,
    ) -> str:
    expr = "CURRENT_TIMESTAMP {}".format(f"AT TIME ZONE '{tzinfo}'" if tzinfo else str()).strip()
    if format:
        expr = f"STRFTIME({expr}, '{format}')"
        if type.upper() == "DATETIME":
            return f"CAST({expr} AS TIMESTAMP)"
    return expr


###################################################################
############################## Rename #############################
###################################################################

@with_connection
def rename_keys(
        data: List[Dict],
        rename: Dict[str,str],
        *,
        conn: DuckDBPyConnection | None = None,
        temp_table: str = DEFAULT_TEMP_TABLE,
    ) -> List[Dict]:
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
        data: List[Dict],
        by: str | Sequence[str],
        agg: str | Dict[str,Literal["count","sum","avg","min","max","first","last","list"]],
        dropna: bool = True,
        *,
        conn: DuckDBPyConnection | None = None,
        temp_table: str = DEFAULT_TEMP_TABLE,
    ) -> List[Dict]:
    create_table(conn, temp_table, data, option="ignore", temp=True)
    by = [by] if isinstance(by, str) else by
    query = f"SELECT {', '.join(by)}, {_agg(agg)} FROM {temp_table} {_groupby(by, dropna)};"
    return select_to_json(query, conn=conn)


def _groupby(by: Sequence[str], dropna: bool = True):
    where = "WHERE " + " AND ".join([f"{col} IS NOT NULL" for col in by]) if dropna else str()
    groupby = "GROUP BY {}".format(", ".join(by))
    return f"{where} {groupby}"


def _agg(func: str | Dict[str,Literal["count","sum","avg","min","max","first","last","list"]]) -> str:
    if isinstance(func, Dict):
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
        *data: List[Dict],
        index: str | Sequence[str],
        dropna: bool = True,
        conn: DuckDBPyConnection | None = None,
        temp_table: str = DEFAULT_TEMP_TABLE,
    ) -> List[Dict]:
    from itertools import chain
    create_table(conn, temp_table, list(chain.from_iterable(data)), option="ignore", temp=True)
    index = [index] if isinstance(index, str) else index
    agg = _agg({col: "first" for col in get_columns(conn, temp_table) if col not in index})
    query = f"SELECT {', '.join(index)}, {agg} FROM {temp_table} {_groupby(index, dropna)};"
    return select_to_json(query, conn=conn)


###################################################################
########################### Partition By ##########################
###################################################################

@with_connection
def partition_by(
        data: List[Dict],
        field: str,
        type: str | None = None,
        condition: str | None = None,
        sort: bool = True,
        *,
        conn: DuckDBPyConnection | None = None,
        temp_table: str = DEFAULT_TEMP_TABLE,
    ) -> Generator[List[Dict], None, None]:
    create_table(conn, temp_table, data, option="ignore", temp=True)
    if field not in get_columns(conn, temp_table):
        field = _add_partition(conn, temp_table, field, type)
    exclude = "EXCLUDE (_PARTITIONFIELD)" if field == "_PARTITIONFIELD" else str()
    for partition in _select_partition(conn, temp_table, field, condition, sort):
        yield select_to_json(
            f"SELECT * {exclude} FROM temp_table WHERE {field} = {_quote(partition)};", conn=conn)


def _add_partition(
        conn: DuckDBPyConnection,
        table: str,
        expr: str,
        type: str | None = None,
    ) -> str:
    field = "_PARTITIONFIELD"
    if not type:
        type = conn.execute(f"SELECT {expr} FROM {table} LIMIT 1").description[0][TYPE]
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {field} {type};")
    conn.execute(f"UPDATE {table} SET {field} = {expr};")
    return field


def _select_partition(
        conn: DuckDBPyConnection,
        table: str,
        field: str,
        condition: str | None = None,
        sort: bool = True
    ) -> List[Any]:
    query = f"SELECT DISTINCT {field} FROM {table} {_where(condition, field)};"
    if sort:
        return sorted(map(lambda x: x[0], conn.execute(query).fetchall()))
    else:
        return [row[0] for row in conn.execute(query).fetchall()]


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
