from __future__ import annotations

from abc import ABCMeta, abstractmethod
from linkmerce.common.tasks import Task
from pathlib import Path

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Callable, Literal, Sequence
    from types import TracebackType
    import datetime as dt

    from duckdb import DuckDBPyConnection, DuckDBPyRelation

NAME, TYPE = 0, 0


def concat_sql(*statement: str, drop_empty: bool = True, sep=' ', terminate: bool = True) -> str:
    """SQL 문장들을 하나로 연결한다."""
    query = sep.join(filter(None, statement) if drop_empty else statement)
    return query + ';' if terminate and not query.endswith(';') else query


def where(where_clause: str | None = None, default: str | None = None) -> str:
    """WHERE 절을 구성한다."""
    if not (where_clause or default):
        return str()
    return f"WHERE {where_clause or default}"


def csv_to_json(obj: list[tuple], header: int | list[str] = 0) -> list[dict]:
    """CSV 형식의 튜플 리스트를 JSON 형식의 딕셔너리 리스트로 변환한다."""
    if isinstance(header, int):
        header, obj = obj[header], obj[header+1:]
    return [dict(zip(header, row)) for row in obj]


def json_to_csv(obj: list[dict], header: int | list[str] = 0) -> list[tuple]:
    """JSON 형식의 딕셔너리 리스트를 CSV 형식의 튜플 리스트로 변환한다."""
    if isinstance(header, int):
        header = list(obj[header].keys())
    return [header] + [[row.get(key) for key in header] for row in obj]


def save_to_csv(obj: list[tuple], file_path: str | Path, encoding: str | None = "utf-8", **kwargs):
    """CSV 데이터를 파일로 저장한다."""
    import csv
    with open(file_path, 'w', newline='', encoding=encoding) as file:
        writer = csv.writer(file, **kwargs)
        writer.writerows(obj)


def save_to_json(obj: list[dict], file_path: str | Path, encoding: str | None = "utf-8", **kwargs):
    """JSON 데이터를 파일로 저장한다."""
    import json
    with open(file_path, 'w', encoding=encoding) as file:
        json.dump(obj, file, **kwargs)


def run_with_tempfile(func: Callable[[str], Any], values: bytes, mode = "w+b", suffix: str | None = None, **kwargs) -> Any:
    """임시 파일에 데이터를 쓰고 함수를 실행한 뒤 결과를 반환한다. 실행 후 임시 파일은 삭제한다."""
    import tempfile, os
    with tempfile.NamedTemporaryFile(mode, suffix=suffix, delete=False, **kwargs) as temp_file:
        temp_file.write(values)
        temp_path = temp_file.name
    # DB가 내부적으로 임시 파일을 이동하거나 잠금을 걸려고 하면 OS가 거부할 수 있다.
    # 이를 방지하기 위해 `with`문 밖에서 함수를 실행한다.
    try:
        return func(temp_path)
    finally:
        os.unlink(temp_path)


def write_tempfile(write_func: Callable[[str], None], mode = "w+b", suffix: str | None = None, **kwargs) -> bytes:
    """임시 파일에 쓰기 함수를 실행하고 파일 내용을 바이트로 반환한다. 실행 후 임시 파일은 삭제한다."""
    import tempfile, os
    with tempfile.NamedTemporaryFile(mode, suffix=suffix, delete=False, **kwargs) as temp_file:
        temp_path = temp_file.name
    # DB가 내부적으로 임시 파일을 이동하거나 잠금을 걸려고 하면 OS가 거부할 수 있다.
    # 이를 방지하기 위해 `with`문 밖에서 함수를 실행한다.
    try:
        write_func(temp_path)
        with open(temp_path, "rb") as file:
            return file.read()
    finally:
        os.unlink(temp_path)


###################################################################
############################ Connection ###########################
###################################################################

class Connection(metaclass=ABCMeta):
    """데이터베이스 연결의 최상위 추상 클래스."""

    def __init__(self, **kwargs):
        self.set_connection(**kwargs)

    @property
    def conn(self) -> Any:
        return self.get_connection()

    @abstractmethod
    def get_connection(self) -> Any:
        """데이터베이스 연결을 반환한다."""
        raise NotImplementedError("The 'get_connection' method must be implemented.")

    @abstractmethod
    def set_connection(self, **kwargs):
        """데이터베이스 연결을 설정한다."""
        raise NotImplementedError("The 'set_connection' method must be implemented.")

    @abstractmethod
    def close(self):
        """데이터베이스 연결을 닫는다."""
        raise NotImplementedError("The 'close' method must be implemented.")

    @abstractmethod
    def execute(self, *args, **kwargs) -> Any:
        """데이터베이스 연결을 통해 SQL 쿼리를 실행한다."""
        raise NotImplementedError("The 'execute' method must be implemented.")

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, type: type[BaseException], value: BaseException, traceback: TracebackType):
        self.close()

    ############################## Fetch ##############################

    def fetch_all(self, format: Literal["csv", "json", "parquet"], query: str) -> list[tuple] | list[dict] | bytes:
        """SQL 쿼리를 실행하고 결과를 지정한 형식(`csv`, `json`, `parquet`)으로 반환한다."""
        raise NotImplementedError("The 'fetch_all' method must be implemented.")

    def fetch_all_to_csv(self, query: str) -> list[tuple]:
        """SQL 쿼리를 실행하고 결과를 CSV 형식의 튜플 리스트로 반환한다."""
        raise NotImplementedError("The 'fetch_all_to_csv' method must be implemented.")

    def fetch_all_to_json(self, query: str) -> list[dict]:
        """SQL 쿼리를 실행하고 결과를 JSON 형식의 딕셔너리 리스트로 반환한다."""
        raise NotImplementedError("The 'fetch_all_to_json' method must be implemented.")

    def fetch_all_to_parquet(self, query: str) -> bytes:
        """SQL 쿼리를 실행하고 결과를 Parquet 바이너리로 반환한다."""
        raise NotImplementedError("The 'fetch_all_to_parquet' method must be implemented.")

    ############################ Expression ###########################

    def expr_cast(self, value: Any | None, type: str, alias: str = str(), safe: bool = False) -> str:
        """SQL CAST 표현식을 생성한다."""
        cast = "TRY_CAST" if safe else "CAST"
        alias = f" AS {alias}" if alias else str()
        return f"{cast}({self.expr_value(value)} AS {type.upper()})" + alias

    def expr_create(self, option: Literal["replace", "ignore"] | None = None, temp: bool = False) -> str:
        """CREATE TABLE 표현식을 생성한다."""
        temp = "TEMP" if temp else str()
        if option == "replace":
            return f"CREATE OR REPLACE {temp} TABLE"
        elif option == "ignore":
            return f"CREATE {temp} TABLE IF NOT EXISTS"
        else:
            return f"CREATE {temp} TABLE"

    def expr_value(self, value: Any | None) -> str:
        """Python 값을 SQL 리터럴로 변환한다."""
        import datetime as dt
        if value is None:
            return "NULL"
        elif isinstance(value, (float, int)):
            return str(value)
        else:
            return f"'{value}'"

    def expr_now(
            self,
            type: Literal["DATETIME", "STRING"] = "DATETIME",
            format: str | None = "%Y-%m-%d %H:%M:%S",
            interval: str | int | None = None,
            tzinfo: str | None = None,
        ) -> str:
        """현재 시간 SQL 표현식을 생성한다."""
        expr = "CURRENT_TIMESTAMP {}".format(f"AT TIME ZONE '{tzinfo}'" if tzinfo else str()).strip()
        expr = f"{expr} {self.expr_interval(interval)}".strip()
        if format:
            expr = f"STRFTIME({expr}, '{format}')"
            if type.upper() == "DATETIME":
                return f"CAST({expr} AS TIMESTAMP)"
        return expr if type.upper() == "DATETIME" else "NULL"

    def expr_today(
            self,
            type: Literal["DATE", "STRING"] = "DATE",
            format: str | None = "%Y-%m-%d",
            interval: str | int | None = None,
        ) -> str:
        """오늘 날짜 SQL 표현식을 생성한다."""
        expr = "CURRENT_DATE"
        if interval is not None:
            expr = f"CAST(({expr} {self.expr_interval(interval)}) AS DATE)"
        if (type.upper() == "STRING") and format:
            return f"STRFTIME({expr}, '{format}')"
        return expr if type.upper() == "DATE" else "NULL"

    def expr_interval(self, days: str | int | None = None) -> str:
        """SQL INTERVAL 표현식을 생성한다."""
        if isinstance(days, str):
            return days
        elif isinstance(days, int):
            return "{} INTERVAL {} DAY".format('-' if days < 0 else '+', abs(days))
        else:
            return str()

    def expr_date_range(self, date_column: str, date_array: list[str | dt.date], format: str = "%Y-%m-%d") -> str:
        """날짜 배열을 BETWEEN 및 IN 절로 최적화한 SQL WHERE 표현식을 생성한다."""
        if len(date_array) < 2:
            return f"{date_column} = '{date_array[0]}'" if date_array else str()

        import datetime as dt
        def strptime(obj: str | dt.date) -> dt.date:
            return obj if isinstance(obj, dt.date) else dt.datetime.strptime(str(obj), format).date()
        array = sorted(map(strptime, date_array))

        between, isin = list(), list()
        start, prev = array[0], array[0]
        for date in array[1:] + [array[-1] + dt.timedelta(days=2)]:
            if (date - prev).days != 1: # if sequence breaks
                if (start != prev) and (date - start).days != 1: # has 2+ days between dates
                    between.append((str(start), str(prev)))
                else:
                    isin.append(str(prev))
                start = date
            prev = date

        expr = list()
        for start, end in between:
            expr.append(f"{date_column} BETWEEN '{start}' AND '{end}'")
        if isin:
            expr.append(f"{date_column} IN ('"+"', '".join(isin)+"')")
        return ('(('+") OR (".join(expr)+'))') if len(expr) > 1 else expr[0]


###################################################################
######################## DuckDB Connection ########################
###################################################################

class DuckDBConnection(Connection):
    """DuckDB 인메모리 데이터베이스 연결을 관리하는 클래스.

    SQL 실행, 테이블 CRUD, 데이터 읽기/쓰기, 그룹 집계 등의 기능을 제공한다."""

    def __init__(self, tzinfo: str | None = None, **kwargs):
        self.set_connection(tzinfo, **kwargs)

    @property
    def conn(self) -> DuckDBPyConnection:
        return self.get_connection()

    def get_connection(self) -> DuckDBPyConnection:
        """DuckDB 연결을 반환한다."""
        return self.__conn

    def set_connection(self, tzinfo: str | None = None, **kwargs):
        """DuckDB 연결을 생성한다."""
        import duckdb
        self.__conn = duckdb.connect(**kwargs)
        if tzinfo is not None:
            self.conn.execute(f"SET TimeZone = '{tzinfo}'")

    def close(self):
        """DuckDB 연결을 닫는다."""
        try:
            self.conn.close()
        except:
            pass

    def __enter__(self) -> DuckDBConnection:
        return self

    def __exit__(self, type: type[BaseException], value: BaseException, traceback: TracebackType):
        self.close()

    ############################# Execute #############################

    def execute(self, query: str, params: object | None = None) -> list[DuckDBPyConnection]:
        """하나 이상의 SQL 문을 실행한다. 세미콜론으로 구분된 다중 쿼리를 지원한다."""
        if ';' not in query:
            return [self.conn.execute(query, parameters=params)]

        # DuckDB는 `;`로 구분된 다중 쿼리에 파라미터를 적용할 수 없으므로 개별 쿼리로 나눠서 실행한다.
        statements = [s for statement in query.split(';') if (s := statement.strip())]
        if (params is not None) and (len(statements) > 1):
            import re
            results = list()
            for statement in statements:
                if isinstance(params, dict) and params:
                    used_keys = set(re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", statement))
                    used_params = {key: value for key, value in params.items() if key in used_keys}
                    # 파라미터로 전달되는 `rows`가 비어있으면 `BinderException: Binder Error` 발생을 회피하기 위해 스킵한다.
                    # `DuckDBTransformer`에서 `execute`를 호출할 때 `rows` 길이를 검증하지만, 다중 쿼리에서는 예외가 있다.
                    if any((isinstance(value, list) and not value) for value in used_params.values()):
                        continue
                    results.append(self.conn.execute(statement, parameters=(used_params or None)))
                else:
                    results.append(self.conn.execute(statement, parameters=params))
            return results
        return [self.conn.execute(query, parameters=params)]

    def sql(self, query: str, params: object | None = None) -> list[DuckDBPyRelation]:
        """하나 이상의 SQL 문을 실행한다. 세미콜론으로 구분된 다중 쿼리를 지원한다."""
        if ';' not in query:
            return [self.conn.sql(query, params=params)]

        # DuckDB does not support to pass parameters to multiple statements
        statements = [s for statement in query.split(';') if (s := statement.strip())]
        if (params is not None) and (len(statements) > 1):
            import re
            results = list()
            for statement in statements:
                if isinstance(params, dict) and params:
                    used_keys = set(re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", statement))
                    used_params = {key: value for key, value in params.items() if key in used_keys}
                    results.append(self.conn.sql(statement, params=(used_params or None)))
                else:
                    results.append(self.conn.sql(statement, params=params))
            return results
        return [self.conn.sql(query, params=params)]

    ############################## Fetch ##############################

    def fetch_one(self, query: str, index: int = 0, params: object | None = None) -> Any:
        """SQL 쿼리로 값 하나를 가져온다."""
        relation = self.conn.execute(query, parameters=params)
        return relation.fetchall()[0][index]

    def fetch_values(self, query: str, params: object | None = None) -> tuple[Any, ...]:
        """SQL 쿼리 결과의 첫 번째 행을 반환한다."""
        relation = self.conn.execute(query, parameters=params)
        return relation.fetchall()[0]

    def fetch_all(
            self,
            format: Literal["csv", "json", "parquet"],
            query: str,
            params: object | None = None,
            save_to: str | Path | None = None,
        ) -> list[tuple] | list[dict] | bytes | None:
        """SQL 쿼리를 실행하고, 결과를 지정한 형식(`csv`, `json`, `parquet`)으로 반환하거나 파일로 저장한다."""
        try:
            return getattr(self, f"fetch_all_to_{format}")(query, params, save_to)
        except AttributeError:
            raise ValueError("Invalid value for data format. Supported formats are: csv, json, parquet.")

    def fetch_all_to_csv(
            self,
            query: str,
            params: object | None = None,
            save_to: str | Path | None = None,
            header: bool = True,
        ) -> list[tuple] | None:
        """SQL 쿼리를 실행하고, 결과를 CSV 형식의 튜플 리스트로 반환하거나 CSV 파일로 저장한다."""
        relation = self.conn.execute(query, parameters=params)
        headers = [tuple(self.get_columns(relation))] if header else list()
        results = headers + relation.fetchall()
        if save_to:
            return save_to_csv(results, save_to, delimiter=',')
        else:
            return results

    def fetch_all_to_json(
            self,
            query: str,
            params: object | None = None,
            save_to: str | Path | None = None,
        ) -> list[dict] | None:
        """SQL 쿼리를 실행하고, 결과를 JSON 형식의 딕셔너리 리스트로 반환하거나 JSON 파일로 저장한다."""
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
            params: object | None = None,
            save_to: str | Path | None = None,
        ) -> bytes | None:
        """SQL 쿼리를 실행하고, 결과를 Parquet 바이너리로 반환하거나 Parquet 파일로 저장한다."""
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
            format: Literal["csv", "json", "parquet"],
            values: list[tuple] | list[dict] | bytes | str | Path,
            params: object | None = None,
            prefix: str | None = None,
            suffix: str | None = None,
        ) -> DuckDBPyConnection:
        """지정된 포맷의 데이터를 SELECT 쿼리로 조회하고 결과를 반환한다."""
        try:
            return getattr(self, f"read_{format}")(values, params, prefix, suffix)
        except AttributeError:
            raise ValueError("Invalid value for data format. Supported formats are: csv, json, parquet.")

    def read_csv(
            self,
            values: list[tuple] | str | Path,
            params: object | None = None,
            prefix: str | None = None,
            suffix: str | None = None,
        ) -> DuckDBPyConnection:
        """CSV 파일 또는 튜플 리스트를 SELECT 쿼리로 조회하고 결과를 반환한다."""
        if isinstance(values, (str, Path)):
            query = f"SELECT * FROM read_csv('{values}')"
        else:
            query = "SELECT values.* FROM (SELECT UNNEST($values) AS values)"
            params = (params or dict()) | {"values": csv_to_json(values)}
        return self.conn.execute(concat_sql(prefix, query, suffix), parameters=params)

    def read_json(
            self,
            values: list[dict] | str | Path,
            params: object | None = None,
            prefix: str | None = None,
            suffix: str | None = None,
        ) -> DuckDBPyConnection:
        """JSON 파일 또는 딕셔너리 리스트를 SELECT 쿼리로 조회하고 결과를 반환한다."""
        if isinstance(values, (str, Path)):
            query = f"SELECT * FROM read_json_auto('{values}')"
        else:
            query = "SELECT values.* FROM (SELECT UNNEST($values) AS values)"
            params = (params or dict()) | {"values": values}
        return self.conn.execute(concat_sql(prefix, query, suffix), parameters=params)

    def read_parquet(
            self,
            values: bytes | str | Path,
            params: object | None = None,
            prefix: str | None = None,
            suffix: str | None = None,
        ) -> DuckDBPyConnection:
        """Parquet 파일 또는 Parquet 바이너리를 SELECT 쿼리로 조회하고 결과를 반환한다."""
        if isinstance(values, (str, Path)):
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
            format: Literal["csv", "json", "parquet"],
            option: Literal["replace", "ignore"] | None = None,
            temp: bool = False,
            params: object | None = None,
        ) -> DuckDBPyConnection:
        """지정된 포맷의 데이터를 조회하고 새 테이블을 생성한다."""
        try:
            return getattr(self, f"create_table_from_{format}")(table, values, option, temp, params)
        except AttributeError:
            raise ValueError("Invalid value for data format. Supported formats are: csv, json, parquet.")

    def create_table_from_csv(
            self,
            table: str,
            values: list[tuple] | str | Path,
            option: Literal["replace", "ignore"] | None = None,
            temp: bool = False,
            params: object | None = None,
        ) -> DuckDBPyConnection:
        """CSV 파일 또는 튜플 리스트를 조회하고 새 테이블을 생성한다."""
        return self.read_csv(values, params=params, prefix=f"{self.expr_create(option, temp)} {table} AS")

    def create_table_from_json(
            self,
            table: str,
            values: list[dict] | str | Path,
            option: Literal["replace", "ignore"] | None = None,
            temp: bool = False,
            params: object | None = None,
        ) -> DuckDBPyConnection:
        """JSON 파일 또는 딕셔너리 리스트를 조회하고 새 테이블을 생성한다."""
        return self.read_json(values, params=params, prefix=f"{self.expr_create(option, temp)} {table} AS")

    def create_table_from_parquet(
            self,
            table: str,
            values: bytes | str | Path,
            option: Literal["replace", "ignore"] | None = None,
            temp: bool = False,
            params: object | None = None,
        ) -> DuckDBPyConnection:
        """Parquet 파일 또는 Parquet 바이너리를 조회하고 새 테이블을 생성한다."""
        return self.read_parquet(values, params=params, prefix=f"{self.expr_create(option, temp)} {table} AS")

    def copy_table(
            self,
            source_table: str,
            target_table: str,
            columns: list[str] | str = "*",
            limit: int | None = None,
            option: Literal["replace", "ignore"] | None = None,
            temp: bool = False,
        ) -> DuckDBPyConnection:
        """기존 테이블을 복사하여 새 테이블을 생성한다."""
        columns_ = ", ".join(columns) if isinstance(columns, list) else columns
        limit_ = f"LIMIT {limit}" if isinstance(limit, int) else None
        query = concat_sql(f"{self.expr_create(option, temp)} {target_table} AS SELECT {columns_} FROM {source_table}", limit_)
        return self.conn.execute(query)

    ############################## Insert #############################

    def insert_into_table(
            self,
            table: str,
            values: list[tuple] | list[dict] | bytes | str | Path,
            format: Literal["csv", "json", "parquet"],
            on_conflict: str | None = None,
            params: object | None = None,
        ) -> DuckDBPyConnection:
        """지정된 포맷의 데이터를 조회하여 기존 테이블에 삽입한다."""
        try:
            return getattr(self, f"insert_into_table_from_{format}")(table, values, on_conflict, params)
        except AttributeError:
            raise ValueError("Invalid value for data format. Supported formats are: csv, json, parquet.")

    def insert_into_table_from_csv(
            self,
            table: str,
            values: list[tuple] | str | Path,
            on_conflict: str | None = None,
            params: object | None = None,
        ) -> DuckDBPyConnection:
        """CSV 파일 또는 튜플 리스트를 조회하여 기존 테이블에 삽입한다."""
        suffix = f"ON CONFLICT {on_conflict}" if on_conflict else None
        return self.read_csv(values, params=params, prefix=f"INSERT INTO {table}", suffix=suffix)

    def insert_into_table_from_json(
            self,
            table: str,
            values: list[dict] | str | Path,
            on_conflict: str | None = None,
            params: object | None = None,
        ) -> DuckDBPyConnection:
        """JSON 파일 또는 딕셔너리 리스트를 조회하여 기존 테이블에 삽입한다."""
        suffix = f"ON CONFLICT {on_conflict}" if on_conflict else None
        return self.read_json(values, params=params, prefix=f"INSERT INTO {table}", suffix=suffix)

    def insert_into_table_from_parquet(
            self,
            table: str,
            values: bytes | str | Path,
            on_conflict: str | None = None,
            params: object | None = None,
        ) -> DuckDBPyConnection:
        """Parquet 파일 또는 Parquet 바이너리를 조회하여 기존 테이블에 삽입한다."""
        suffix = f"ON CONFLICT {on_conflict}" if on_conflict else None
        return self.read_parquet(values, params=params, prefix=f"INSERT INTO {table}", suffix=suffix)

    ############################# Group By ############################

    def groupby(
            self,
            source: str,
            by: str | Sequence[str],
            agg: str | dict[str, Literal["count", "sum", "avg", "min", "max", "first", "last", "list"]],
            dropna: bool = True,
            params: object | None = None,
        ) -> DuckDBPyRelation:
        """지정된 칼럼을 기준으로 그룹 집계를 수행한다."""
        by = [by] if isinstance(by, str) else by
        where = "WHERE " + " AND ".join([f"{col} IS NOT NULL" for col in by]) if dropna else None
        groupby = "GROUP BY {}".format(", ".join(by))
        query = concat_sql(f"SELECT {', '.join(by)}, {self.agg(agg)} FROM {source}", where, groupby)
        return self.conn.sql(query, params=params)

    def agg(self, func: str | dict[str, Literal["count", "sum", "avg", "min", "max", "first", "last", "list"]]) -> str:
        """그룹 집계 표현식을 생성한다."""
        if isinstance(func, dict):
            def render(col: str, agg: str) -> str:
                if agg in {"count", "sum", "avg", "min", "max"}:
                    return f"{agg.upper()}({col})"
                elif agg in {"first", "last", "list"}:
                    return f"{agg.upper()}({col}) FILTER (WHERE {col} IS NOT NULL)"
                else:
                    return f"{agg}({col})"
            return ", ".join([f"{render(col, agg)} AS {col}" for col, agg in func.items()])
        else:
            return func

    ############################## Utils ##############################

    def table_exists(self, table: str) -> bool:
        """테이블 존재 여부를 확인한다."""
        query = f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table}' LIMIT 1"
        return bool(self.conn.execute(query).fetchone())

    def table_has_rows(self, table: str) -> bool:
        """테이블이 존재하고 데이터가 있는지 확인한다."""
        if self.table_exists(table):
            query = f"SELECT 1 FROM {table} LIMIT 1"
            return bool(self.conn.execute(query).fetchone())
        return False

    def count_table(self, table: str) -> int:
        """테이블의 데이터 행 수를 카운트한다."""
        return self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchall()[0][0]

    def show_tables(self) -> list[str]:
        """현재 연결에 존재하는 모든 테이블 이름을 반환한다."""
        return [table[NAME] for table in self.conn.execute("SHOW TABLES").fetchall()]

    def get_columns(self, obj: str | DuckDBPyConnection) -> list[str]:
        """테이블 또는 쿼리 결과의 칼럼 목록을 반환한다."""
        if isinstance(obj, str):
            obj = self.conn.execute(f"DESCRIBE {obj}")
            return [column[NAME] for column in obj.fetchall()]
        else:
            return [column[NAME] for column in obj.description]

    def has_column(self, obj: str | DuckDBPyConnection, column: str) -> bool:
        """테이블 또는 쿼리 결과에 특정 열이 있는지 확인한다."""
        return column in self.get_columns(obj)

    def unique(self, table: str, expr: str, ascending: bool | None = None, where_clause: str | None = None) -> list:
        """테이블에서 특정 표현식으로 조회했을 때 고유한 값 목록을 반환한다."""
        select = f"SELECT DISTINCT {expr} AS expr FROM {table}"
        order_by = "ORDER BY expr {}".format({True:"ASC", False:"DESC"}[ascending]) if isinstance(ascending, bool) else None
        query = concat_sql(select, where(where_clause), order_by)
        return [row[0] for row in self.conn.execute(query).fetchall()]


class DuckDBIterator(Task):
    """테이블 또는 데이터를 파티션별로 순회하는 DuckDB 이터레이터."""

    temp_table: str = "temp_table"

    def __init__(self, conn: DuckDBConnection, format: Literal["csv", "json", "parquet"]):
        """DuckDB 연결과 반환할 데이터의 포맷을 초기화한다."""
        self.conn = conn
        self.format = format
        self.table = str()
        self.partitions = [dict()]
        self.index = 0

    def run(self):
        """`Task` 부모 클래스의 추상 메서드는 구현하지 않는다."""
        ...

    def from_table(self, table: str) -> DuckDBIterator:
        """기존 테이블을 `table` 속성으로 추가한다."""
        return self.setattr("table", table)

    def from_values(
            self,
            values: list[tuple] | list[dict] | bytes | str | Path,
            format: Literal["csv", "json", "parquet"],
            params: object | None = None,
        ) -> DuckDBIterator:
        """지정된 포맷의 데이터를 조회하여 임시 테이블을 생성한다."""
        self.conn.create_table(self.temp_table, values, format, option="replace", temp=True, params=params)
        return self.setattr("table", self.temp_table)

    def partition_by(
            self,
            by: str | list[str],
            ascending: bool | None = True,
            where_clause: str | None = None,
            if_errors: Literal["ignore", "raise"] = "raise",
        ) -> DuckDBIterator:
        """특정 표현식으로 조회했을 때 고유한 값 목록을 `partitions` 속성으로 추가한다.
        `BinderException`이 발생하는 표현식은 무시한다."""
        from linkmerce.utils.progress import _expand_kwargs
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
        """`partitions` 속성을 순회하면서, 각 파티션 값을 WHERE 절로 치환하여 조회한 결과를 반환한다."""
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
