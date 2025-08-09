from __future__ import annotations

import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Literal, Sequence
    from duckdb import DuckDBPyConnection
    from io import BytesIO
    TableName = str
    TableReturn = list[tuple] | list[dict] | BytesIO

DEFAULT_TEMP_TABLE = "temp_table"

NAME, TYPE = 0, 0


def get_columns(conn: DuckDBPyConnection, table: str) -> list[str]:
    return [column[NAME] for column in conn.execute(f"DESCRIBE {table}").fetchall()]


###################################################################
############################## Create #############################
###################################################################

def create_table_from_json(
        conn: DuckDBPyConnection,
        table: str,
        data: list[dict],
        option: Literal["replace", "ignore"] | None = None,
        temp: bool = False,
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


def with_temp_table(func):
    @functools.wraps(func)
    def wrapper(conn: DuckDBPyConnection, source: TableName | list[dict], *args, **kwargs):
        if not isinstance(source, str):
            create_table_from_json(conn, DEFAULT_TEMP_TABLE, source, option="replace", temp=True)
            source = DEFAULT_TEMP_TABLE
            try:
                return func(conn, source, *args, **kwargs)
            finally:
                conn.execute(f"DROP TABLE {source};")
        else:
            return func(conn, source, *args, **kwargs)
    return wrapper


###################################################################
############################## Select #############################
###################################################################

def select(
        conn: DuckDBPyConnection,
        query: str,
        return_type: Literal["csv","json","parquet"],
        params: dict | None = None,
    ) -> TableReturn:
    if return_type == "csv":
        return select_to_csv(conn, query, params)
    elif return_type == "json":
        return select_to_json(conn, query, params)
    elif return_type == "parquet":
        return select_to_csv(conn, query, params=params)
    else:
        raise ValueError("Invalid value for return_type. Supported formats are: csv, json, parquet.")


def select_to_csv(
        conn: DuckDBPyConnection,
        query: str,
        params: dict | None = None,
    ) -> list[tuple]:
    relation = conn.execute(query, parameters=params)
    columns = [column[NAME] for column in relation.description]
    return [columns] + relation.fetchall()


def select_to_json(
        conn: DuckDBPyConnection,
        query: str,
        params: dict | None = None,
    ) -> list[dict]:
    relation = conn.execute(query, parameters=params)
    columns = [column[NAME] for column in relation.description]
    return [dict(zip(columns, row)) for row in relation.fetchall()]


def select_to_parquet(
        conn: DuckDBPyConnection,
        query: str,
        file_name: str | None = None,
        params: dict | None = None,
    ) -> BytesIO | None:
    relation = conn.sql(query, params=params)
    if file_name is None:
        from io import BytesIO
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".parquet") as temp_file:
            file_path = temp_file.name
            relation.to_parquet(file_path)
            with open(file_path, "rb") as file:
                return BytesIO(file.read())
    else:
        relation.to_parquet(file_name)


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
############################# Group By ############################
###################################################################

@with_temp_table
def groupby(
        conn: DuckDBPyConnection,
        source: TableName | list[dict],
        by: str | Sequence[str],
        agg: str | dict[str,Literal["count","sum","avg","min","max","first","last","list"]],
        return_type: Literal["csv","json","parquet"],
        dropna: bool = True,
    ) -> TableReturn:
    by = [by] if isinstance(by, str) else by
    query = f"SELECT {', '.join(by)}, {_agg(agg)} FROM {source} {_groupby(by, dropna)};"
    return select(conn, query, return_type)


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


###################################################################
########################### Partition By ##########################
###################################################################

class Partition:
    def __init__(
            self,
            conn: DuckDBPyConnection,
            source: TableName | list[dict],
            field: str,
            type: str | None = None,
            condition: str | None = None,
            sort: bool = True,
        ):
        self.conn = conn
        self.set_table(source)
        self.set_field(field, type)
        self.set_partitions(condition, sort)

    def set_table(self, source: TableName | list[dict]):
        if not isinstance(source, str):
            create_table_from_json(self.conn, DEFAULT_TEMP_TABLE, source, option="replace", temp=True)
            source = DEFAULT_TEMP_TABLE
        self.table = source

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
