from __future__ import annotations
import functools

from linkmerce.common.load import Connection, concat_sql, where, build_temp_table_name

from typing import Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Literal, TypeVar
    from types import TracebackType
    from pathlib import Path

    from psycopg2.extensions import connection as PgConnection
    from psycopg2.extensions import cursor as PgCursor
    Clause = TypeVar("Clause", str)
    Columns = TypeVar("Columns", Sequence[str])

    from linkmerce.common.load import DuckDBConnection
    from duckdb import DuckDBPyConnection
    DuckDBTable = TypeVar("DuckDBTable", str)
    PgTable = TypeVar("PgTable", str)


class BreakExecution(Exception):
    pass


def split_table(table: str) -> tuple[str, str]:
    """`schema.table` 문자열을 `(schema, table)` 튜플로 분리한다."""
    if "." in table:
        schema, table_name = table.rsplit(".", 1)
        return schema, table_name
    return "public", table


def ensure_cursor(return_cursor: bool = True, rollback_on_error: bool = True, pass_commit: bool = True):
    """PostgreSQL 커서를 보장하는 데코레이터.

    Parameters
    ----------
    return_cursor: bool
        커서 반환 여부
            - `True`: 커서를 닫지 않고 반환한다. `close=True` 조건에서는 항상 커서를 닫고 `None`을 반환한다. (기본값)
            - `False`: 함수 실행 결과를 반환한다. 커서를 자동 생성했을 경우 커서를 닫는다.
    rollback_on_error: bool
        함수 실행 중 오류 발생 시 롤백 여부. autocommit 설정 시 롤백할 수 없다. 기본값은 `True`
    pass_commit: bool
        `commit` 키워드 인자를 함수에 넘겨줄지 여부. 함수가 해당 키워드 인자를 받지 않을 경우 비활성화 한다. 기본값은 `True`

    **NOTE** 생성된 데코레이터는 아래 2가지 키워드 인자를 선택적으로 입력받아 사용한다.

    cursor: psycopg2.extensions.cursor | None
        PostgreSQL 커서
    commit: bool
        함수 실행 후 커밋 실행 여부. 기본값은 `False`
    close: bool
        함수 실행 후 커서 종료 여부. 기본값은 `False`
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(
                self: PostgresClient,
                *args,
                cursor: PgCursor | None = None,
                commit: bool = False,
                close: bool = False,
                **kwargs
            ):
            instant = False
            if cursor is None:
                cursor, instant = self.conn.cursor(), True

            try:
                optional = {"commit": commit} if pass_commit else dict()
                result = func(self, *args, cursor=cursor, **optional, **kwargs)
                if commit:
                    self.conn.commit()
            except Exception:
                if rollback_on_error:
                    self.conn.rollback()
                raise
            finally:
                if close or (instant and (not return_cursor)):
                    cursor.close()
                    cursor = None

            return cursor if return_cursor else result
        return wrapper
    return decorator


###################################################################
######################## PostgreSQL Client ########################
###################################################################

class PostgresClient(Connection):
    """PostgreSQL 클라이언트. `Connection`을 상속하며 테이블 CRUD 작업을 지원한다.

    PostgreSQL 13+ 환경을 기준으로 설계되었으며,
    [`psycopg2`](https://pypi.org/project/psycopg2/) 라이브러리를 사용한다.
    """

    def __init__(self, dsn: str, autocommit: bool = False, **kwargs):
        self.set_connection(dsn, **kwargs)
        self.conn.autocommit = autocommit

    @property
    def conn(self) -> PgConnection:
        return self.get_connection()

    def get_connection(self) -> PgConnection:
        """PostgreSQL 연결을 반환한다."""
        return self.__conn

    def set_connection(self, dsn: str, **kwargs):
        """PostgreSQL 연결을 설정한다.

        Parameters
        ----------
            dsn: str | dict | None
                연결 문자열(`postgresql://user:pass@host:port/db`) 또는 키워드 인자.
        """
        import psycopg2
        from psycopg2.extensions import make_dsn
        self.__dsn = make_dsn(dsn, **kwargs)
        self.__conn = psycopg2.connect(dsn, **kwargs)

    def close(self):
        """PostgreSQL 연결을 닫는다."""
        try:
            self.conn.close()
        except Exception:
            pass

    def __enter__(self) -> PostgresClient:
        return self

    def __exit__(self, type: type[BaseException], value: BaseException, traceback: TracebackType):
        if type is not None:
            self.conn.rollback()
        self.close()

    ############################# Execute #############################

    @ensure_cursor(return_cursor=True)
    def execute(
            self,
            query: str,
            params: Sequence | None = None,
            *,
            cursor: PgCursor | None = None,
            commit: bool = False,
            close: bool = False,
        ) -> PgCursor | None:
        """SQL 쿼리를 실행한다. 커서를 생략하면 자동으로 생성하고 커서를 반환한다."""
        cursor.execute(query, params)

    @ensure_cursor(return_cursor=True)
    def execute_batch(
            self,
            query: str,
            params_seq: Sequence[Sequence | dict[str, Any]],
            page_size: int = 1000,
            *,
            cursor: PgCursor | None = None,
            commit: bool = False,
            close: bool = False,
        ) -> PgCursor | None:
        """여러 매개변수 목록에 대해 SQL 쿼리를 배치 실행한다. 커서를 생략하면 자동으로 생성하고 커서를 반환한다."""
        from psycopg2.extras import execute_batch
        execute_batch(cursor, query, params_seq, page_size=page_size)

    ############################## Fetch ##############################

    @ensure_cursor(return_cursor=False, pass_commit=False)
    def fetch_one(
            self,
            query: str,
            params: Sequence | None = None,
            index: int = 0,
            *,
            cursor: PgCursor | None = None,
        ) -> Any:
        """SQL 쿼리를 실행하여 값 하나를 가져온다. 결과 행이 없다면 `TypeError`가 발생한다."""
        cursor.execute(query, params)
        return cursor.fetchone()[index]

    @ensure_cursor(return_cursor=False, pass_commit=False)
    def fetch_values(
            self,
            query: str,
            params: Sequence | None = None,
            axis: int = 0,
            *,
            cursor: PgCursor | None = None,
        ) -> tuple[Any, ...]:
        """SQL 쿼리 실행 결과를 1차원 리스트로 반환한다. `axis=0`은 첫 번째 행, `axis=1`은 첫 번째 열을 반환한다."""
        cursor.execute(query, params)
        if axis == 1:
            return tuple(row[0] for row in cursor.fetchall())
        return cursor.fetchone()

    def fetch_all(
            self,
            format: Literal["csv", "json", "parquet"],
            query: str,
            params: object | None = None,
            save_to: str | Path | None = None,
            *,
            cursor: PgCursor | None = None,
        ) -> list[tuple] | list[dict] | bytes | None:
        """SQL 쿼리를 실행하고, 결과를 지정한 형식(`csv`, `json`, `parquet`)으로 반환하거나 파일로 저장한다."""
        try:
            if format == "parquet":
                return self.fetch_all_to_parquet(query, params, save_to)
            return getattr(self, f"fetch_all_to_{format}")(query, params, save_to, cursor=cursor)
        except AttributeError:
            raise ValueError("Invalid value for data format. Supported formats are: csv, json, parquet.")

    @ensure_cursor(return_cursor=False, pass_commit=False)
    def fetch_all_to_csv(
            self,
            query: str,
            params: object | None = None,
            save_to: str | Path | None = None,
            header: bool = True,
            *,
            cursor: PgCursor | None = None,
        ) -> list[tuple] | None:
        """SQL 쿼리를 실행하고, 결과를 CSV 형식의 튜플 리스트로 반환하거나 CSV 파일로 저장한다."""
        cursor.execute(query, params)
        headers = [tuple(column[0] for column in cursor.description)] if header else list()
        results = headers + cursor.fetchall()
        if save_to:
            from linkmerce.common.load import save_to_csv
            return save_to_csv(results, save_to, delimiter=',')
        return results

    @ensure_cursor(return_cursor=False, pass_commit=False)
    def fetch_all_to_json(
            self,
            query: str,
            params: object | None = None,
            save_to: str | Path | None = None,
            *,
            cursor: PgCursor | None = None,
        ) -> list[dict] | None:
        """SQL 쿼리를 실행하고, 결과를 JSON 형식의 딕셔너리 리스트로 반환하거나 JSON 파일로 저장한다."""
        cursor.execute(query, params)
        columns = [column[0] for column in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        if save_to:
            from linkmerce.common.load import save_to_json
            return save_to_json(results, save_to, indent=2, ensure_ascii=False, default=str)
        return results

    def fetch_all_to_parquet(
            self,
            query: str,
            params: object | None = None,
            save_to: str | Path | None = None,
        ) -> bytes | None:
        """SQL 쿼리를 실행하고, 결과를 Parquet 바이너리로 반환하거나 Parquet 파일로 저장한다.

        **NOTE** DuckDB 엔진과 `postgres` 확장을 사용하며, 현재 연결 정보를 그대로 재사용한다.
        쿼리에서는 PostgreSQL 테이블을 `db.{schema}.{table}` 형식으로 참조해야 한다.
        """
        import duckdb
        with duckdb.connect() as conn:
            if save_to:
                query = f"COPY ({query}) TO '{save_to}' (FORMAT PARQUET);"
                self.execute_with_duckdb(conn, query, params, database="db", install_extension=True)
                return None
            else:
                from linkmerce.common.load import write_tempfile
                def to_parquet(temp_file: str):
                    query = f"COPY ({query}) TO '{temp_file}' (FORMAT PARQUET);"
                    self.execute_with_duckdb(conn, query, params, database="db", install_extension=True)
                return write_tempfile(to_parquet, mode="w+b", suffix=".parquet")

    ############################## Create #############################

    @ensure_cursor(return_cursor=True)
    def copy_table(
            self,
            source_table: str,
            target_table: str,
            where_clause: str | None = None,
            limit: int | None = None,
            option: Literal["replace", "ignore"] | None = None,
            *,
            cursor: PgCursor | None = None,
            commit: bool = False,
            close: bool = False,
        ) -> PgCursor | None:
        """소스 테이블의 제약조건 등 모든 속성을 복사한 새 테이블을 생성하고,
        소스 테이블 조회 결과를 새 테이블로 복사한다.
        """
        query = concat_sql(
            (f"DROP TABLE IF EXISTS {target_table};" if option == "replace" else None),
            "CREATE TABLE",
            ("IF NOT EXISTS" if option == "ignore" else None),
            f"{target_table} (LIKE {source_table} INCLUDING ALL)",
        )
        if limit != 0:
            query = concat_sql(
                query,
                f"INSERT INTO {target_table} SELECT * FROM {source_table}",
                where(where_clause),
                (f"LIMIT {limit}" if isinstance(limit, int) else None),
            )
        cursor.execute(query)

    ############################## Upsert #############################

    @ensure_cursor(return_cursor=True)
    def upsert(
            self,
            source_table: str,
            target_table: str,
            on_conflict: str | Sequence[str],
            do_action: Clause
                | dict[str, Literal["replace", "ignore", "greatest", "least", "source_first", "target_first"]]
                | Literal[":replace_all:", ":do_nothing:"] = ":replace_all:",
            where_clause: Clause | None = None,
            *,
            cursor: PgCursor | None = None,
            commit: bool = False,
            close: bool = False,
        ) -> PgCursor | None:
        """소스 테이블을 타겟 테이블에 `INSERT ... ON CONFLICT` 병합한다.

        **NOTE** WHERE 절에서 소스 테이블 칼럼은 `S.`로 참조한다.
        """
        on_conflict = [on_conflict] if isinstance(on_conflict, str) else list(on_conflict)
        columns = self.get_description(source_table, cursor)
        if not columns:
            raise ValueError("Source table does not contain any columns.")

        query = self._compose_upsert_query(source_table, target_table, columns, on_conflict, do_action, where_clause)
        cursor.execute(query)

    def _compose_upsert_query(
            self,
            source_table: str,
            target_table: str,
            columns: Sequence[str],
            on_conflict: str | Sequence[str],
            do_action: Clause
                | dict[str, Literal["replace", "ignore", "greatest", "least", "source_first", "target_first"]]
                | Literal[":replace_all:", ":do_nothing:"] = ":replace_all:",
            where_clause: Clause | None = None,
        ) -> str:
        on_conflict = [on_conflict] if isinstance(on_conflict, str) else list(on_conflict)
        return concat_sql(
            f"INSERT INTO {target_table} AS T ({', '.join(columns)})",
            "SELECT {}".format(', '.join(f"S.{column}" for column in columns)),
            f"FROM {source_table} AS S",
            where(where_clause),
            f"ON CONFLICT ({', '.join(on_conflict)})",
            self._compose_upsert_action(do_action, columns, on_conflict),
        )

    def _compose_upsert_action(
            self,
            do_action: Clause
                | dict[str, Literal["replace", "ignore", "greatest", "least", "source_first", "target_first"]]
                | Literal[":replace_all:", ":do_nothing:"],
            columns: Sequence[str] = list(),
            on_conflict: str | Sequence[str] = list(),
        ) -> str:
        """UPSERT의 `DO UPDATE` 또는 `DO NOTHING` 절을 생성한다."""
        prefix = "DO UPDATE SET "
        if do_action == ":replace_all:":
            on_conflict = [on_conflict] if isinstance(on_conflict, str) else list(on_conflict)
            return self._compose_upsert_action({col: "replace" for col in columns if col not in on_conflict})
        elif (do_action == ":do_nothing:") or (not do_action):
            return "DO NOTHING"
        elif isinstance(do_action, dict):
            def render(column: str, agg: str) -> str:
                if agg in {"source_first", "target_first"}:
                    alias = ("EXCLUDED", "T") if agg == "source_first" else ("T", "EXCLUDED")
                    kwargs = dict(zip(["left", "right"], alias))
                    return "COALESCE({left}.{column}, {right}.{column})".format(column=column, **kwargs)
                if agg in {"greatest", "least"}:
                    return f"{agg.upper()}(EXCLUDED.{column}, T.{column})"
                if agg in {"replace", "ignore"}:
                    return f"EXCLUDED.{column}" if agg == "replace" else f"T.{column}"
                return f"{agg}({column})"
            return prefix + ", ".join([f"{col} = {render(col, agg)}" for col, agg in do_action.items()])
        return prefix + str(do_action)

    ############################## DuckDB #############################

    def attach_postgres_to_duckdb(
            self,
            connection: DuckDBConnection | DuckDBPyConnection,
            database: str = "db",
            install_extension: bool = True,
            read_only: bool = True,
        ):
        """DuckDB 연결에 PostgreSQL을 `database`로 붙인다.

        Parameters
        ----------
        connection: DuckDBConnection | DuckDBPyConnection
            기본 DuckDB 연결 또는 LinkMerce의 DuckDB 클라이언트
        database: str
            PostgreSQL 연결 시 적용할 DB 이름. PostgreSQL 테이블 참조 시 DB 이름을 붙여야 한다. 기본값은 `"db"`
        install_extension: bool
            DuckDB 연결에 PostgreSQL 확장 기능 설치 및 연동 여부
                - `True`: DuckDB 연결에 PostgreSQL 확장 기능을 설치하고, 읽기/쓰기 모드에 맞게 연동한다. (기본값)
                - `False`: 확장 기능이 설치 및 연동되었다는 전제로 읽기/쓰기 모드가 일치하는지 검증한다.
        read_only: bool
            READ_ONLY 모드 사용 여부
                - `True`: `install_extension=True` 조건에서 PostgreSQL과 연동하면서 READ_ONLY 모드를 적용한다. (기본값)
                - `False`: `install_extension=False` 조건에서 READ_ONLY 모드인지 확인하고 맞다면 기존 연동 해제 후 재연동한다.
        """
        dsn = self.__dsn.replace("'", "''")
        options = "TYPE postgres, READ_ONLY" if read_only else "TYPE postgres"
        if install_extension:
            connection.execute("INSTALL postgres;")
        connection.execute("LOAD postgres;")

        query = f"SELECT readonly FROM duckdb_databases() WHERE database_name = '{database}'"
        result = connection.execute(query).fetchone()
        if not result:
            connection.execute(f"ATTACH '{dsn}' AS {database} ({options});")
        elif result[0] != read_only:
            connection.execute(f"DETACH {database};")
            connection.execute(f"ATTACH '{dsn}' AS {database} ({options});")

    def execute_with_duckdb(
            self,
            connection: DuckDBConnection | DuckDBPyConnection,
            query: str,
            params: object | None = None,
            database: str = "db",
            install_extension: bool = False,
            read_only: bool = True,
        ) -> DuckDBPyConnection:
        """DuckDB 연결에 PostgreSQL을 붙이고, DuckDB 연결을 통해서 PostgreSQL에 읽기/쓰기 작업을 한다."""
        self.attach_postgres_to_duckdb(connection, database, install_extension, read_only)
        return connection.execute(query, params)

    def load_table_from_duckdb(
            self,
            connection: DuckDBConnection,
            source_table: DuckDBTable,
            target_table: PgTable,
            columns: Sequence[str] = list(),
            if_source_table_empty: Literal["break", "continue"] | None = "break",
            if_target_table_not_found: Literal["break", "create", "raise"] = "raise",
            install_extension: bool = False,
        ) -> bool:
        """DuckDB 테이블 행을 PostgreSQL 테이블에 적재한다."""
        if self._is_duckdb_table_empty(connection, source_table, if_source_table_empty):
            return True

        column_string = ", ".join(columns if columns else connection.get_columns(source_table))

        if self.table_exists(target_table):
            query = f"INSERT INTO db.{target_table} ({column_string}) SELECT {column_string} FROM {source_table};"
        elif if_target_table_not_found == "create":
            query = f"CREATE TABLE db.{target_table} AS SELECT {column_string} FROM {source_table};"
        elif if_target_table_not_found == "break":
            return False
        else:
            from psycopg2.errors import UndefinedTable
            raise UndefinedTable()

        self.execute_with_duckdb(connection, query, None, "db", install_extension, read_only=False)
        return True

    def overwrite_table_from_duckdb(
            self,
            connection: DuckDBConnection,
            source_table: DuckDBTable,
            target_table: PgTable,
            columns: Sequence[str] = list(),
            where_clause: Clause | None = None,
            if_source_table_empty: Literal["break", "continue"] | None = "break",
            if_target_table_not_found: Literal["break", "create", "raise"] = "raise",
            cleanup_staging_table: bool = True,
            install_extension: bool = False,
        ) -> bool:
        """DuckDB 테이블을 스테이징 테이블에 적재한 후, PostgreSQL 테이블의 기존 데이터를 삭제하고 스테이징 테이블 행을 덮어쓴다.

        **NOTE** 삭제(DELETE) 및 적재(INSERT) 쿼리 실행 앞뒤로 트랜잭션 시작과 커밋을 실행하며,   
        쿼리가 실패하면 직전 시점으로 롤백한다.
        """
        if self._is_duckdb_table_empty(connection, source_table, if_source_table_empty):
            return True

        common = (connection, source_table, target_table, columns)
        if not self.table_has_rows(target_table, where_clause):
            return self.load_table_from_duckdb(*common, None, if_target_table_not_found, install_extension)

        with self.conn.cursor() as cursor:
            staging_table = None
            try:
                staging_table = self._prepare_staging_table_from_duckdb(*common, install_extension, cursor=cursor)
                if where_clause is None:
                    delete_clause = f"TRUNCATE TABLE {target_table};"
                else:
                    delete_clause = concat_sql("DELETE FROM", target_table, where(where_clause))

                insert_clause = f"INSERT INTO {target_table} SELECT * FROM {staging_table};"
                query = f"BEGIN; {delete_clause} {insert_clause} COMMIT;"
                cursor.execute(query)
                return True
            except Exception:
                cursor.execute("ROLLBACK;")
                raise
            finally:
                if cleanup_staging_table and staging_table:
                    cursor.execute(f"BEGIN; DROP TABLE IF EXISTS {staging_table}; COMMIT;")

    def upsert_table_from_duckdb(
            self,
            connection: DuckDBConnection,
            source_table: DuckDBTable,
            target_table: PgTable,
            columns: Sequence[str] = list(),
            where_clause: Clause | None = None,
            on_conflict: str | Sequence[str] = list(),
            do_action: Clause
                | dict[str, Literal["replace", "ignore", "greatest", "least", "source_first", "target_first"]]
                | Literal[":replace_all:", ":do_nothing:"] = ":replace_all:",
            if_source_table_empty: Literal["break", "continue"] | None = "break",
            if_target_table_not_found: Literal["break", "create", "raise"] = "raise",
            cleanup_staging_table: bool = True,
            install_extension: bool = False,
        ) -> bool:
        """DuckDB 테이블을 스테이징 테이블에 적재한 후, 스테이징 테이블을 BigQuery 테이블에 UPESRT 한다.

        **NOTE** WHERE 절에서 소스 테이블 칼럼은 `S.`로 참조한다.
        """
        if self._is_duckdb_table_empty(connection, source_table, if_source_table_empty):
            return True

        common = (connection, source_table, target_table, columns)
        if where_clause is not None:
            import re
            unalias_where = re.sub(r"(^|[^A-Za-z0-9_])(T|S)\.", r"\1", where_clause)
        else:
            unalias_where = "TRUE"

        if not self.table_has_rows(target_table, unalias_where):
            return self.load_table_from_duckdb(*common, None, if_target_table_not_found, install_extension)

        with self.conn.cursor() as cursor:
            staging_table = None
            try:
                staging_table = self._prepare_staging_table_from_duckdb(*common, install_extension, cursor=cursor)
                staging_columns = self.get_description(staging_table, cursor)
                query = self._compose_upsert_query(staging_table, target_table, staging_columns, on_conflict, do_action, where_clause)
                cursor.execute(f"BEGIN; {query} COMMIT;")
                return True
            except Exception:
                cursor.execute("ROLLBACK;")
                raise
            finally:
                if cleanup_staging_table and staging_table:
                    cursor.execute(f"BEGIN; DROP TABLE IF EXISTS {staging_table}; COMMIT;")

    def _is_duckdb_table_empty(
            self,
            connection: DuckDBConnection,
            source_table: str,
            if_source_table_empty: Literal["break", "continue"] | None = "break",
        ) -> bool:
        """`if_source_table_empty` 값에 따라 DuckDB 테이블을 검증한다.
            - `"break"`: 테이블 행이 존재하는지 여부를 반환한다.
            - `"continue"`: 테이블이 존재하는지 여부를 반환한다.
            - `None`: 테이블을 검증하지 않는다.
        """
        if if_source_table_empty == "break":
            return not connection.table_has_rows(source_table)
        elif if_source_table_empty == "continue":
            return not connection.table_exists(source_table)
        return False

    @ensure_cursor(return_cursor=False, pass_commit=False)
    def _prepare_staging_table_from_duckdb(
            self,
            connection: DuckDBConnection,
            source_table: DuckDBTable,
            target_table: PgTable,
            columns: Sequence[str] = list(),
            install_extension: bool = False,
            *,
            cursor: PgCursor | None = None,
            close: bool = False,
        ) -> PgTable:
        """DuckDB 소스 테이블 행을 적재할 빈 스테이징 테이블을 생성하고 적재한다."""
        staging_table = build_temp_table_name(target_table, max_name_length=62)
        while self.table_exists(staging_table):
            staging_table = build_temp_table_name(target_table, max_name_length=62)
        self.copy_table(target_table, staging_table, limit=0, option="replace", cursor=cursor, commit=True)

        self.load_table_from_duckdb(connection, source_table, staging_table, columns,
            if_source_table_empty=None, if_target_table_not_found="raise", install_extension=install_extension)
        return staging_table

    ######################### Partition ###############################

    def create_partitions(
            self,
            parent_table: str,
            control_column: str,
            interval: str = "daily",
            start_partition: str | None = None,
            premake_days: int = 35,
            *,
            cursor: PgCursor | None = None,
            commit: bool = False,
            close: bool = False,
        ) -> PgCursor:
        """파티션이 없으면 생성한다. 파티션 테이블 이름을 반환한다.

        **NOTE** `pg_partman` 확장을 활성화해야 한다.

        Parameters
        ----------
        parent_table: str
            부모 테이블명 (예: `schema.table`)
        control_column: str
            파티션 기준으로 사용할 시간(DATE, TIMESTAMP) 또는 정수형 칼럼명
        interval: str
            파티션 생성 간격. 기본값은 `"daily"`
        start_partition: str | None
            파티션 생성을 시작할 기준 시점. 생략하면 현재 시간을 기준으로 자동 계산된다.
        premake_days: int
            현재 시점 이후로 미리 생성할 파티션의 개수. 기본값은 `35`

        Returns
        -------
        psycopg2.extensions.cursor
            파티션 생성 실행 결과가 담긴 커서를 반환한다.
        """
        from textwrap import dedent
        query = dedent("""
            SELECT partman.create_parent(
                p_parent_table := %s,
                p_control := %s,
                p_type := 'native',
                p_interval := %s,
                p_premake := %s,
                {has_start}p_start_partition := %s,
                p_automatic_maintenance := 'on',
                p_jobmon := false
            );
            """).strip().format(has_start="--" if start_partition is None else str())
        params = (parent_table, control_column, interval, premake_days, start_partition)
        return self.execute(query, params=params, cursor=cursor, commit=commit, close=close)

    def get_partitions(self, table: str) -> list[tuple[str, str]]:
        """테이블의 파티션 목록을 반환한다."""
        from textwrap import dedent
        schema, table_name = split_table(table)
        query = dedent("""
            SELECT
                c.relname AS partition_name
                , pg_get_expr(c.relpartbound, c.oid) AS partition_range
            FROM pg_class p
            JOIN pg_namespace n ON n.oid = p.relnamespace
            JOIN pg_inherits i ON i.inhparent = p.oid
            JOIN pg_class c ON c.oid = i.inhrelid
            WHERE n.nspname = %s AND p.relname = %s
            ORDER BY c.relname;
            """).strip()
        return self.fetch_all_to_csv(query, params=(schema, table_name))

    def is_partitioned(self, table: str) -> bool:
        """테이블이 파티셔닝되어 있는지 확인한다."""
        from textwrap import dedent
        schema, table_name = split_table(table)
        query = dedent("""
            SELECT c.relkind FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s;
            """).strip()
        return self.fetch_one(query, params=(schema, table_name)) == 'p'

    ############################## Helper #############################

    def table_exists(self, table: str) -> bool:
        """테이블 존재 여부를 확인한다."""
        schema, table_name = split_table(table)
        query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)"
        return self.fetch_one(query, params=(schema, table_name))

    def table_has_rows(self, table: str, where_clause: str | None = None) -> bool:
        """테이블이 존재하고 데이터가 있는지 확인한다."""
        if not self.table_exists(table):
            return False
        query = concat_sql(f"SELECT EXISTS (SELECT 1 FROM {table}", where(where_clause), "LIMIT 1)")
        return self.fetch_one(query)

    def count_table(self, table: str, where_clause: str | None = None) -> int:
        """테이블의 행 수를 집계한다."""
        query = concat_sql(f"SELECT COUNT(*) FROM {table}", where(where_clause))
        return self.fetch_one(query)

    def get_columns(self, table: str) -> list[str]:
        """테이블의 칼럼명 리스트를 반환한다."""
        from textwrap import dedent
        schema, table_name = split_table(table)
        query = dedent("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND is_generated = 'NEVER'
            ORDER BY ordinal_position;
            """).strip()
        return self.fetch_values(query, params=(schema, table_name), axis=1)

    def get_description(self, table: str, cursor: PgCursor) -> list[str]:
        """현재 커서에서 접근 가능한 테이블 칼럼명 리스트를 반환한다."""
        cursor.execute(f"SELECT * FROM {table} LIMIT 0;")
        return [column[0] for column in (cursor.description or list())]
