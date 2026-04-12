from __future__ import annotations

from linkmerce.common.load import Connection, concat_sql, where

from typing import Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, IO, Literal, TypeVar
    from types import TracebackType

    from psycopg2.extensions import connection as PgConnection
    from psycopg2.extensions import cursor as PgCursor

    from linkmerce.common.load import DuckDBConnection
    Clause = TypeVar("Clause", str)
    Columns = TypeVar("Columns", Sequence[str])
    DuckDBTable = TypeVar("DuckDBTable", str)
    PgTable = TypeVar("PgTable", str)


###################################################################
######################## PostgreSQL Client #########################
###################################################################

class PostgresClient(Connection):
    """PostgreSQL 클라이언트. `Connection`을 상속하며 테이블 CRUD, COPY, UPSERT 작업을 지원한다.

    PostgreSQL 13+ 환경을 기준으로 설계되었으며, `psycopg2`를 사용한다.
    BigQuery 모듈과 동등한 인터페이스를 제공하되, PostgreSQL 고유 용어와 기능을 활용한다.
    """

    def __init__(self, dsn: str | dict[str, Any] | None = None, **kwargs):
        self.set_connection(dsn, **kwargs)

    @property
    def conn(self) -> PgConnection:
        return self.get_connection()

    def get_connection(self) -> PgConnection:
        return self.__conn

    def set_connection(self, dsn: str | dict[str, Any] | None = None, **kwargs):
        """PostgreSQL 연결을 설정한다.

        Args:
            dsn: 연결 문자열(``postgresql://user:pass@host:port/db``) 또는 키워드 딕셔너리.
            **kwargs: ``psycopg2.connect``에 전달할 추가 키워드 인자.
        """
        import psycopg2
        if isinstance(dsn, dict):
            self.__conn = psycopg2.connect(**dsn, **kwargs)
        elif isinstance(dsn, str):
            self.__conn = psycopg2.connect(dsn, **kwargs)
        else:
            self.__conn = psycopg2.connect(**kwargs)
        self.__conn.autocommit = False

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def execute(self, query: str, params: tuple | dict | None = None, commit: bool = True) -> PgCursor:
        """SQL 쿼리를 실행한다. 기본적으로 자동 커밋한다."""
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        if commit:
            self.conn.commit()
        return cursor

    def execute_batch(self, query: str, params_seq: Sequence[tuple | dict], page_size: int = 1000, commit: bool = True):
        """여러 파라미터 세트에 대해 쿼리를 배치 실행한다."""
        from psycopg2.extras import execute_batch
        cursor = self.conn.cursor()
        execute_batch(cursor, query, params_seq, page_size=page_size)
        if commit:
            self.conn.commit()
        cursor.close()

    def __enter__(self) -> PostgresClient:
        return self

    def __exit__(self, type: type[BaseException], value: BaseException, traceback: TracebackType):
        if type is not None:
            self.conn.rollback()
        self.close()

    ############################## Fetch ##############################

    def fetch_all(self, format: Literal["csv", "json"], query: str) -> list[tuple] | list[dict]:
        """SQL 쿼리를 실행하고 결과를 지정한 형식(``csv`` 또는 ``json``)으로 반환한다."""
        try:
            return getattr(self, f"fetch_all_to_{format}")(query)
        except AttributeError:
            raise ValueError("Invalid value for data format. Supported formats are: csv, json.")

    def fetch_all_to_csv(self, query: str, header: bool = True) -> list[tuple]:
        """SQL 쿼리를 실행하고 결과를 CSV 형식의 튜플 리스트로 반환한다."""
        cursor = self.conn.cursor()
        cursor.execute(query)
        rows = list()
        if header:
            rows.append(tuple(desc[0] for desc in cursor.description))
        rows.extend(cursor.fetchall())
        cursor.close()
        return rows

    def fetch_all_to_json(self, query: str) -> list[dict]:
        """SQL 쿼리를 실행하고 결과를 JSON 형식의 딕셔너리 리스트로 반환한다."""
        cursor = self.conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        return rows

    ############################ CRUD Table ###########################

    def table_exists(self, table: str) -> bool:
        """테이블 존재 여부를 확인한다. ``schema.table`` 형식을 지원한다."""
        schema, table_name = self._split_table(table)
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)",
            (schema, table_name),
        )
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists

    def table_has_rows(self, table: str, where_clause: str | None = None) -> bool:
        """테이블이 존재하고 데이터가 있는지 확인한다."""
        if not self.table_exists(table):
            return False
        query = concat_sql(f"SELECT EXISTS (SELECT 1 FROM {table}", where(where_clause), ")")
        cursor = self.conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()[0]
        cursor.close()
        return result

    def get_columns(self, table: str) -> list[str]:
        """테이블의 칼럼명 리스트를 반환한다. 생성된(GENERATED) 컬럼은 제외한다."""
        schema, table_name = self._split_table(table)
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s AND is_generated = 'NEVER' "
            "ORDER BY ordinal_position",
            (schema, table_name),
        )
        columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return columns

    def get_primary_key(self, table: str) -> list[str]:
        """테이블의 PRIMARY KEY 칼럼 리스트를 반환한다."""
        schema, table_name = self._split_table(table)
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT a.attname FROM pg_index i "
            "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
            "JOIN pg_class c ON c.oid = i.indrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE i.indisprimary AND n.nspname = %s AND c.relname = %s "
            "ORDER BY array_position(i.indkey, a.attnum)",
            (schema, table_name),
        )
        columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return columns

    def truncate_table(self, table: str) -> None:
        """테이블의 모든 데이터를 삭제한다."""
        self.execute(f"TRUNCATE TABLE {table};")

    def drop_table(self, table: str, if_exists: bool = True) -> None:
        """테이블을 삭제한다."""
        exists = "IF EXISTS " if if_exists else ""
        self.execute(f"DROP TABLE {exists}{table};")

    ############################# COPY ################################

    def copy_from_csv(
            self,
            table: str,
            file_obj: IO[bytes] | IO[str],
            columns: Sequence[str] | None = None,
            delimiter: str = ",",
            null: str = "",
            header: bool = True,
    ) -> None:
        """CSV 파일을 ``COPY FROM`` 으로 테이블에 적재한다."""
        col_clause = f"({', '.join(columns)})" if columns else ""
        sql = f"COPY {table} {col_clause} FROM STDIN WITH (FORMAT csv, DELIMITER '{delimiter}', NULL '{null}', HEADER {str(header).upper()})"
        cursor = self.conn.cursor()
        cursor.copy_expert(sql, file_obj)
        self.conn.commit()
        cursor.close()

    def copy_to_csv(
            self,
            table_or_query: str,
            file_obj: IO[bytes] | IO[str],
            delimiter: str = ",",
            header: bool = True,
    ) -> None:
        """테이블 또는 쿼리 결과를 ``COPY TO``로 CSV 파일에 출력한다."""
        if table_or_query.strip().upper().startswith("SELECT"):
            source = f"({table_or_query})"
        else:
            source = table_or_query
        sql = f"COPY {source} TO STDOUT WITH (FORMAT csv, DELIMITER '{delimiter}', HEADER {str(header).upper()})"
        cursor = self.conn.cursor()
        cursor.copy_expert(sql, file_obj)
        cursor.close()

    ########################### Insert / Upsert #######################

    def insert_rows(
            self,
            table: str,
            values: list[dict],
            columns: Sequence[str] | None = None,
    ) -> None:
        """딕셔너리 리스트를 테이블에 INSERT한다."""
        if not values:
            return
        columns = columns or list(values[0].keys())
        col_clause = ", ".join(columns)
        placeholders = ", ".join([f"%({col})s" for col in columns])
        query = f"INSERT INTO {table} ({col_clause}) VALUES ({placeholders})"
        self.execute_batch(query, values)

    def upsert(
            self,
            table: str,
            values: list[dict],
            conflict_columns: str | Sequence[str],
            matched: Clause | dict[str, Literal["replace", "ignore", "greatest", "least", "source_first", "target_first"]] | Literal[":replace_all:", ":do_nothing:"] = ":replace_all:",
            columns: Sequence[str] | None = None,
    ) -> None:
        """``INSERT ... ON CONFLICT ... DO UPDATE`` (UPSERT)를 실행한다.

        BigQuery의 ``merge_into_table``에 대응하는 PostgreSQL 네이티브 구현이다.

        Args:
            table: 대상 테이블 (``schema.table``).
            values: 삽입할 딕셔너리 리스트.
            conflict_columns: 충돌 감지 칼럼 (PK 또는 UNIQUE 제약).
            matched: 충돌 시 업데이트 전략.
                - ``:replace_all:``: 충돌 칼럼을 제외한 모든 칼럼을 새 값으로 교체.
                - ``:do_nothing:``: 충돌 행을 무시.
                - ``dict``: 칼럼별 전략 (``replace``, ``ignore``, ``greatest``, ``least``,
                  ``source_first``, ``target_first``).
            columns: 삽입 칼럼 목록. ``None``이면 ``values[0]``의 키를 사용한다.
        """
        if not values:
            return
        conflict_columns = [conflict_columns] if isinstance(conflict_columns, str) else list(conflict_columns)
        columns = columns or list(values[0].keys())
        col_clause = ", ".join(columns)
        placeholders = ", ".join([f"%({col})s" for col in columns])
        conflict_clause = ", ".join(conflict_columns)

        do_clause = self._build_on_conflict_action(table, matched, conflict_columns, columns)
        query = f"INSERT INTO {table} ({col_clause}) VALUES ({placeholders}) ON CONFLICT ({conflict_clause}) {do_clause}"
        self.execute_batch(query, values)

    def upsert_from_staging(
            self,
            source_table: str,
            target_table: str,
            conflict_columns: str | Sequence[str],
            matched: Clause | dict[str, Literal["replace", "ignore", "greatest", "least", "source_first", "target_first"]] | Literal[":replace_all:", ":do_nothing:"] = ":replace_all:",
            not_matched: Clause | Columns | Literal[":insert_all:", ":do_nothing:"] = ":insert_all:",
            where_clause: Clause | None = None,
    ) -> None:
        """스테이징 테이블에서 타겟 테이블로 ``INSERT ... ON CONFLICT``를 실행한다.

        BigQuery의 ``merge_into_table``(테이블 간 병합)에 대응한다.
        소스 테이블은 ``S``, 타겟 테이블은 ``T``로 참조할 수 있다.
        """
        conflict_columns = [conflict_columns] if isinstance(conflict_columns, str) else list(conflict_columns)
        source_columns = self.get_columns(source_table)
        target_columns = self.get_columns(target_table)
        insert_columns = [col for col in source_columns if col in target_columns]

        # SELECT from staging
        select_cols = ", ".join(insert_columns)
        select_clause = concat_sql(f"SELECT {select_cols} FROM {source_table}", where(where_clause), terminate=False)

        # ON CONFLICT action
        do_clause = self._build_on_conflict_action(target_table, matched, conflict_columns, insert_columns)
        conflict_clause = ", ".join(conflict_columns)

        query = f"INSERT INTO {target_table} ({select_cols}) {select_clause} ON CONFLICT ({conflict_clause}) {do_clause};"
        self.execute(query)

    def _build_on_conflict_action(
            self,
            table: str,
            matched: Clause | dict[str, Literal["replace", "ignore", "greatest", "least", "source_first", "target_first"]] | Literal[":replace_all:", ":do_nothing:"],
            conflict_columns: list[str],
            columns: list[str] | None = None,
    ) -> str:
        """``ON CONFLICT`` 절의 ``DO UPDATE SET`` 또는 ``DO NOTHING``을 생성한다."""
        if matched == ":do_nothing:":
            return "DO NOTHING"

        if matched == ":replace_all:":
            columns = columns or self.get_columns(table)
            update_cols = [col for col in columns if col not in conflict_columns]
            return "DO UPDATE SET " + ", ".join(f"{col} = EXCLUDED.{col}" for col in update_cols)

        if isinstance(matched, dict):
            def render(col: str, agg: str) -> str:
                if agg == "replace":
                    return f"EXCLUDED.{col}"
                elif agg == "ignore":
                    return f"{table}.{col}"
                elif agg == "greatest":
                    return f"GREATEST(EXCLUDED.{col}, {table}.{col})"
                elif agg == "least":
                    return f"LEAST(EXCLUDED.{col}, {table}.{col})"
                elif agg == "source_first":
                    return f"COALESCE(EXCLUDED.{col}, {table}.{col})"
                elif agg == "target_first":
                    return f"COALESCE({table}.{col}, EXCLUDED.{col})"
                else:
                    return f"{agg}({col})"
            return "DO UPDATE SET " + ", ".join(f"{col} = {render(col, agg)}" for col, agg in matched.items())

        # 커스텀 SQL 문자열
        return f"DO UPDATE SET {matched}"

    ########################## Overwrite #############################

    def overwrite_table(
            self,
            table: str,
            values: list[dict],
            where_clause: str = "TRUE",
            columns: Sequence[str] | None = None,
    ) -> None:
        """테이블의 기존 데이터를 삭제한 후 새 데이터를 삽입한다."""
        if not values:
            return
        self.execute(f"DELETE FROM {table} WHERE {where_clause};", commit=False)
        columns = columns or list(values[0].keys())
        col_clause = ", ".join(columns)
        placeholders = ", ".join([f"%({col})s" for col in columns])
        query = f"INSERT INTO {table} ({col_clause}) VALUES ({placeholders})"
        from psycopg2.extras import execute_batch
        cursor = self.conn.cursor()
        execute_batch(cursor, query, values, page_size=1000)
        self.conn.commit()
        cursor.close()

    ######################### Partition ###############################

    def ensure_partition(
            self,
            table: str,
            start: str,
            end: str,
            suffix: str | None = None,
    ) -> str:
        """파티션이 없으면 생성한다. 파티션 테이블 이름을 반환한다.

        Args:
            table: 부모 테이블 (``schema.table``).
            start: 파티션 시작 범위 (inclusive, 예: ``'2025-01-01'``).
            end: 파티션 끝 범위 (exclusive, 예: ``'2025-02-01'``).
            suffix: 파티션 접미사. ``None``이면 ``_y2025m01`` 형식으로 자동 생성한다.
        """
        if suffix is None:
            # start에서 YYYY-MM 추출
            suffix = f"_y{start[:4]}m{start[5:7]}"
        partition_name = f"{table}{suffix}"
        if not self.table_exists(partition_name):
            self.execute(
                f"CREATE TABLE IF NOT EXISTS {partition_name} "
                f"PARTITION OF {table} FOR VALUES FROM ('{start}') TO ('{end}');"
            )
        return partition_name

    def ensure_monthly_partitions(
            self,
            table: str,
            start_date: str,
            end_date: str,
    ) -> list[str]:
        """지정 기간의 월별 파티션을 모두 생성한다.

        Args:
            table: 부모 테이블 (``schema.table``).
            start_date: 시작 날짜 (``YYYY-MM-DD``).
            end_date: 종료 날짜 (``YYYY-MM-DD``, inclusive).
        Returns:
            생성된 파티션 테이블 이름 리스트.
        """
        from datetime import datetime, timedelta
        start = datetime.strptime(start_date[:7] + "-01", "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        partitions = []
        current = start
        while current <= end:
            # 다음 달 1일 계산
            if current.month == 12:
                next_month = current.replace(year=current.year + 1, month=1, day=1)
            else:
                next_month = current.replace(month=current.month + 1, day=1)
            name = self.ensure_partition(table, str(current), str(next_month))
            partitions.append(name)
            current = next_month
        return partitions

    def get_partitions(self, table: str) -> list[dict]:
        """테이블의 파티션 목록을 반환한다."""
        schema, table_name = self._split_table(table)
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT c.relname AS partition_name, "
            "pg_get_expr(c.relpartbound, c.oid) AS partition_range "
            "FROM pg_class p "
            "JOIN pg_namespace n ON n.oid = p.relnamespace "
            "JOIN pg_inherits i ON i.inhparent = p.oid "
            "JOIN pg_class c ON c.oid = i.inhrelid "
            "WHERE n.nspname = %s AND p.relname = %s "
            "ORDER BY c.relname",
            (schema, table_name),
        )
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        return rows

    def is_partitioned(self, table: str) -> bool:
        """테이블이 파티셔닝되어 있는지 확인한다."""
        schema, table_name = self._split_table(table)
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT c.relkind FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relname = %s",
            (schema, table_name),
        )
        row = cursor.fetchone()
        cursor.close()
        return row is not None and row[0] == "p"

    ############################## DuckDB #############################

    def load_from_duckdb(
            self,
            connection: DuckDBConnection,
            source_table: DuckDBTable,
            target_table: PgTable,
            columns: Sequence[str] | None = None,
            progress: bool = True,
            batch_size: int = 10000,
            if_source_table_empty: Literal["break", "continue"] = "break",
    ) -> bool:
        """DuckDB 테이블을 PostgreSQL 테이블에 ``COPY``로 적재한다."""
        from io import StringIO

        if not (connection.table_has_rows(source_table) if if_source_table_empty == "break" else connection.table_exists(source_table)):
            return True

        columns = columns or self.get_columns(target_table)
        col_clause = ", ".join(columns)

        # DuckDB에서 CSV로 export하여 PG COPY로 적재
        csv_data = connection.conn.execute(
            f"SELECT {col_clause} FROM {source_table}"
        ).fetchdf().to_csv(index=False, header=True)

        buf = StringIO(csv_data)
        self.copy_from_csv(target_table, buf, columns=columns, header=True)
        return True

    def overwrite_from_duckdb(
            self,
            connection: DuckDBConnection,
            source_table: DuckDBTable,
            target_table: PgTable,
            where_clause: Clause = "TRUE",
            columns: Sequence[str] | None = None,
            progress: bool = True,
            if_source_table_empty: Literal["break", "continue"] = "break",
    ) -> bool:
        """PostgreSQL 테이블의 기존 데이터를 삭제한 후 DuckDB 테이블에서 적재한다."""
        if not (connection.table_has_rows(source_table) if if_source_table_empty == "break" else connection.table_exists(source_table)):
            return True
        elif not self.table_has_rows(target_table, where_clause):
            return self.load_from_duckdb(connection, source_table, target_table, columns, progress)

        # 기존 데이터 삭제 후 새 데이터 적재 — 트랜잭션으로 보호
        self.execute(f"DELETE FROM {target_table} WHERE {where_clause};", commit=False)
        try:
            columns = columns or self.get_columns(target_table)
            col_clause = ", ".join(columns)
            from io import StringIO
            csv_data = connection.conn.execute(
                f"SELECT {col_clause} FROM {source_table}"
            ).fetchdf().to_csv(index=False, header=True)
            buf = StringIO(csv_data)

            cursor = self.conn.cursor()
            col_list = f"({', '.join(columns)})"
            sql = f"COPY {target_table} {col_list} FROM STDIN WITH (FORMAT csv, DELIMITER ',', NULL '', HEADER TRUE)"
            cursor.copy_expert(sql, buf)
            self.conn.commit()
            cursor.close()
            return True
        except Exception:
            self.conn.rollback()
            raise

    def upsert_from_duckdb(
            self,
            connection: DuckDBConnection,
            source_table: DuckDBTable,
            staging_table: PgTable,
            target_table: PgTable,
            conflict_columns: str | Sequence[str],
            matched: Clause | dict[str, Literal["replace", "ignore", "greatest", "least", "source_first", "target_first"]] | Literal[":replace_all:", ":do_nothing:"] = ":replace_all:",
            not_matched: Clause | Columns | Literal[":insert_all:", ":do_nothing:"] = ":insert_all:",
            where_clause: Clause | None = None,
            columns: Sequence[str] | None = None,
            progress: bool = True,
            if_source_table_empty: Literal["break", "continue"] = "break",
            drop_staging_after_upsert: bool = True,
    ) -> bool:
        """DuckDB 테이블을 스테이징 테이블에 적재한 후 타겟 테이블에 UPSERT한다.

        BigQuery의 ``merge_into_table_from_duckdb``에 대응하는 PostgreSQL 구현이다."""
        import re
        if not (connection.table_has_rows(source_table) if if_source_table_empty == "break" else connection.table_exists(source_table)):
            return True
        elif not self.table_has_rows(target_table, (re.sub(r"(^|[^A-Za-z0-9_])(T|S)\.", r"\1", where_clause) if where_clause else None)):
            return self.load_from_duckdb(connection, source_table, target_table, columns, progress)

        try:
            # 스테이징 테이블 생성 (타겟 테이블과 동일 구조, 비파티션)
            self.execute(
                f"CREATE TEMP TABLE {staging_table} (LIKE {target_table} INCLUDING DEFAULTS) ON COMMIT DROP;",
                commit=False,
            )
            # DuckDB → 스테이징 테이블 적재
            columns = columns or self.get_columns(target_table)
            col_clause = ", ".join(columns)
            from io import StringIO
            csv_data = connection.conn.execute(
                f"SELECT {col_clause} FROM {source_table}"
            ).fetchdf().to_csv(index=False, header=True)
            buf = StringIO(csv_data)

            cursor = self.conn.cursor()
            col_list = f"({', '.join(columns)})"
            sql = f"COPY {staging_table} {col_list} FROM STDIN WITH (FORMAT csv, DELIMITER ',', NULL '', HEADER TRUE)"
            cursor.copy_expert(sql, buf)
            cursor.close()

            # 스테이징 → 타겟 UPSERT
            self.upsert_from_staging(staging_table, target_table, conflict_columns, matched, not_matched, where_clause)
            return True
        except Exception:
            self.conn.rollback()
            raise
        finally:
            if drop_staging_after_upsert:
                try:
                    self.execute(f"DROP TABLE IF EXISTS {staging_table};")
                except Exception:
                    pass

    ############################ Expression ###########################

    def expr_cast(self, value: Any | None, type: str, alias: str = str(), safe: bool = False) -> str:
        """``CAST`` SQL 표현식을 생성한다. ``safe=True``이면 에러 시 NULL을 반환하는 표현식을 사용한다."""
        # PostgreSQL에는 SAFE_CAST가 없으므로 CASE WHEN으로 대체
        if safe:
            alias_clause = f" AS {alias}" if alias else ""
            return f"(CASE WHEN {self.expr_value(value)}::TEXT ~ '^[0-9.\\-]+$' THEN {self.expr_value(value)}::{type.upper()} ELSE NULL END){alias_clause}"
        alias_clause = f" AS {alias}" if alias else ""
        return f"CAST({self.expr_value(value)} AS {type.upper()}){alias_clause}"

    def expr_now(
            self,
            type: Literal["TIMESTAMP", "STRING"] = "TIMESTAMP",
            format: str | None = "YYYY-MM-DD HH24:MI:SS",
            interval: str | int | None = None,
            tzinfo: str | None = None,
    ) -> str:
        """``CURRENT_TIMESTAMP`` SQL 표현식을 생성한다."""
        if tzinfo:
            expr = f"(CURRENT_TIMESTAMP AT TIME ZONE '{tzinfo}')"
        else:
            expr = "CURRENT_TIMESTAMP"
        if isinstance(interval, int):
            expr = f"({expr} {'+' if interval >= 0 else '-'} INTERVAL '{abs(interval)} days')"
        elif isinstance(interval, str):
            expr = f"({expr} {interval})"
        if type.upper() == "STRING" and format:
            return f"TO_CHAR({expr}, '{format}')"
        return expr if type.upper() == "TIMESTAMP" else "NULL"

    def expr_today(
            self,
            type: Literal["DATE", "STRING"] = "DATE",
            format: str | None = "YYYY-MM-DD",
            interval: str | int | None = None,
    ) -> str:
        """``CURRENT_DATE`` SQL 표현식을 생성한다."""
        expr = "CURRENT_DATE"
        if isinstance(interval, int):
            expr = f"({expr} {'+' if interval >= 0 else '-'} INTERVAL '{abs(interval)} days')::DATE"
        elif isinstance(interval, str):
            expr = f"({expr} {interval})::DATE"
        if type.upper() == "STRING" and format:
            return f"TO_CHAR({expr}, '{format}')"
        return expr if type.upper() == "DATE" else "NULL"

    ############################ Helpers ##############################

    @staticmethod
    def _split_table(table: str) -> tuple[str, str]:
        """``schema.table`` 문자열을 ``(schema, table)`` 튜플로 분리한다."""
        if "." in table:
            schema, table_name = table.split(".", 1)
            return schema, table_name
        return "public", table
