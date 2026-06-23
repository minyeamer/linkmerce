from __future__ import annotations

from typing import TYPE_CHECKING
from functools import wraps

if TYPE_CHECKING:
    from typing import Any, Callable, Literal, Sequence, TypedDict, TypeVar
    from linkmerce.common.load import DuckDBConnection
    from linkmerce.extensions.bigquery import BigQueryClient
    from linkmerce.extensions.postgres import PostgresClient

    class LoadResult(TypedDict):
        count: int
        table: str
        pg_success: bool | None
        bq_success: bool | None

    DuckDBTable = TypeVar("DuckDBTable", str)
    BigQueryTable = TypeVar("BigQueryTable", str)
    PgTable = TypeVar("PgTable", str)


def load_table_from_duckdb(
        connection: DuckDBConnection,
        source_table: DuckDBTable,
        target_table: PgTable,
        columns: Sequence[str] = list(),
        extra_metadata: dict | None = None,
        execute: bool = True,
        **kwargs
    ) -> LoadResult:
    """DuckDB 테이블 행을 BigQuery/PostgreSQL 테이블에 적재한다."""
    common = (connection, source_table, target_table, columns)
    return {
        "table": target_table,
        "count": (connection.count_table(source_table) if connection.table_exists(source_table) else 0),
        **(extra_metadata if isinstance(extra_metadata, dict) else dict()),
        # 제약 조건이 엄격한 PostgreSQL에 먼저 적재하여 데이터 유효성을 검증한다.
        "pg_success": (load_pg_table_from_duckdb(*common, **kwargs) if execute else None),
        "bq_success": (load_bq_table_from_duckdb(*common, **kwargs) if execute else None),
    }


def overwrite_table_from_duckdb(
        connection: DuckDBConnection,
        source_table: DuckDBTable,
        target_table: PgTable,
        columns: Sequence[str] = list(),
        where_clause: str | None = None,
        extra_metadata: dict | None = None,
        execute: bool = True,
        **kwargs
    ) -> LoadResult:
    """DuckDB 테이블을 스테이징 테이블에 적재한 후,
    BigQuery/PostgreSQL 테이블의 기존 데이터를 삭제하고 스테이징 테이블 행을 덮어쓴다.

    **NOTE** 삭제(DELETE) 및 적재(INSERT) 쿼리 실행 앞뒤로 트랜잭션 시작과 커밋을 실행하며,   
    쿼리가 실패하면 직전 시점으로 롤백한다.
    """
    common = (connection, source_table, target_table, columns, where_clause)
    return {
        "count": (connection.count_table(source_table) if connection.table_exists(source_table) else 0),
        "table": target_table,
        **(extra_metadata if isinstance(extra_metadata, dict) else dict()),
        # 제약 조건이 엄격한 PostgreSQL에 먼저 적재하여 데이터 유효성을 검증한다.
        "pg_success": (overwrite_pg_table_from_duckdb(*common, **kwargs) if execute else None),
        "bq_success": (overwrite_bq_table_from_duckdb(*common, **kwargs) if execute else None),
    }


def merge_table_from_duckdb(
        connection: DuckDBConnection,
        source_table: DuckDBTable,
        target_table: PgTable,
        columns: Sequence[str] = list(),
        where_clause: str | None = None,
        on_conflict: str | Sequence[str] = list(),
        matched: str
            | dict[str, Literal["replace", "ignore", "greatest", "least", "source_first", "target_first"]]
            | Literal[":replace_all:", ":do_nothing:"] = ":replace_all:",
        not_matched: str | Sequence[str] | Literal[":insert_all:", ":do_nothing:"] = ":insert_all:",
        extra_metadata: dict | None = None,
        execute: bool = True,
        **kwargs
    ) -> LoadResult:
    """DuckDB 테이블을 스테이징 테이블에 적재한 후, PostgreSQL/BigQuery 테이블에 MERGE 한다.

    **NOTE** WHERE 절에서 타겟 테이블 칼럼은 `T.`, 소스 테이블 칼럼은 `S.`로 참조한다.
    """
    common = (connection, source_table, target_table, columns, where_clause, on_conflict, matched, not_matched)
    return {
        "count": (connection.count_table(source_table) if connection.table_exists(source_table) else 0),
        "table": target_table,
        **(extra_metadata if isinstance(extra_metadata, dict) else dict()),
        # 제약 조건이 엄격한 PostgreSQL에 먼저 적재하여 데이터 유효성을 검증한다.
        "pg_success": (merge_into_pg_table_from_duckdb(*common, **kwargs) if execute else None),
        "bq_success": (merge_into_bq_table_from_duckdb(*common, **kwargs) if execute else None),
    }


###################################################################
############################# BigQuery ############################
###################################################################

def bigquery_client(func):
    """`BigQueryClient`를 생성하여 데이터를 적재하고, 실행이 끝나면 연결을 닫는 데코레이터.

    적재 실패 시 `BadRequest` 예외를 발생시킨다.
    """
    @wraps(func)
    def wrapper(*args, bigquery_conn_id: str = "gcp_bigquery", **kwargs):
        from linkmerce.extensions.bigquery import BigQueryClient

        service_account = _read_google_service_account(bigquery_conn_id)
        with BigQueryClient(service_account) as client:
            if not (success := func(*args, **kwargs, client=client)):
                from google.api_core.exceptions import BadRequest
                raise BadRequest("BigQuery load job failed due to an unknown error.")
            return success
    return wrapper


def _read_google_service_account(conn_id: str) -> dict:
    """Google Cloud 타입의 Connection의 `keyfile_dict`에서 서비스 계정 정보를 읽어온다."""
    from airflow.sdk import Connection

    airflow_connection: Connection = Connection.get(conn_id)
    return airflow_connection.extra_dejson["keyfile_dict"]


@bigquery_client
def load_bq_table_from_duckdb(
        connection: DuckDBConnection,
        source_table: DuckDBTable,
        target_table: BigQueryTable,
        columns: Sequence[str] = list(),
        schema: Literal["auto"] | str | Sequence[dict] = "auto",
        write: Literal["append", "empty", "truncate", "truncate_data"] = "append",
        if_source_table_empty: Literal["break", "continue"] | None = "break",
        if_target_table_not_found: Literal["break", "raise"] = "raise",
        **kwargs
    ) -> bool:
    """DuckDB 테이블을 BigQuery 테이블에 적재한다."""
    client: BigQueryClient = kwargs["client"]
    return client.load_table_from_duckdb(
        connection, source_table, target_table, columns, schema, write,
        if_source_table_empty, if_target_table_not_found
    )


@bigquery_client
def overwrite_bq_table_from_duckdb(
        connection: DuckDBConnection,
        source_table: DuckDBTable,
        target_table: BigQueryTable,
        columns: Sequence[str] = list(),
        where_clause: str | None = "TRUE",
        schema: Literal["auto"] | str | Sequence[dict] = "auto",
        if_source_table_empty: Literal["break", "continue"] | None = "break",
        if_target_table_not_found: Literal["break", "raise"] = "raise",
        cleanup_staging_table: bool = True,
        **kwargs
    ) -> bool:
    """DuckDB 테이블을 스테이징 테이블에 적재한 후,
    BigQuery 테이블의 기존 데이터를 삭제하고 스테이징 테이블 행을 덮어쓴다.

    **NOTE** 삭제(DELETE) 및 적재(INSERT) 쿼리 실행 앞뒤로 트랜잭션 시작과 커밋을 실행하며,   
    쿼리가 실패하면 직전 시점으로 롤백한다.
    """
    client: BigQueryClient = kwargs["client"]
    return client.overwrite_table_from_duckdb(
        connection, source_table, target_table, columns, where_clause, schema,
        if_source_table_empty, if_target_table_not_found, cleanup_staging_table
    )


@bigquery_client
def merge_into_bq_table_from_duckdb(
        connection: DuckDBConnection,
        source_table: DuckDBTable,
        target_table: BigQueryTable,
        columns: Sequence[str] = list(),
        where_clause: str | None = None,
        on_conflict: str | Sequence[str] = list(),
        matched: str
            | dict[str, Literal["replace", "ignore", "greatest", "least", "source_first", "target_first"]]
            | Literal[":replace_all:", ":do_nothing:"] = ":replace_all:",
        not_matched: str | Sequence[str] | Literal[":insert_all:", ":do_nothing:"] = ":insert_all:",
        schema: Literal["auto"] | str | Sequence[dict] = "auto",
        if_source_table_empty: Literal["break", "continue"] | None = "break",
        if_target_table_not_found: Literal["break", "raise"] = "raise",
        cleanup_staging_table: bool = True,
        **kwargs
    ) -> bool:
    """DuckDB 테이블을 스테이징 테이블에 적재한 후, 스테이징 테이블을 BigQuery 테이블에 MERGE 한다.

    **NOTE** WHERE 절에서 타겟 테이블 칼럼은 `T.`, 소스 테이블 칼럼은 `S.`로 참조한다.
    """
    client: BigQueryClient = kwargs["client"]
    return client.merge_table_from_duckdb(
        connection, source_table, target_table, columns,
        where_clause, on_conflict, matched, not_matched, schema,
        if_source_table_empty, if_target_table_not_found, cleanup_staging_table
    )


###################################################################
############################ PostgreSQL ###########################
###################################################################

def postgres_client(func):
    """`PostgresClient`를 생성하여 데이터를 적재하고, 실행이 끝나면 연결을 닫는 데코레이터.

    적재 실패 시 `InternalError` 에러를 발생시킨다.
    """
    @wraps(func)
    def wrapper(*args, postgres_conn_id: str = "postgres", **kwargs):
        from linkmerce.extensions.postgres import PostgresClient

        dsn = _read_postgres_dsn(postgres_conn_id)
        with PostgresClient(dsn) as client:
            if not (success := func(*args, **kwargs, client=client)):
                from psycopg2.errors import InternalError
                raise InternalError("PostgreSQL table load failed due to an unknown error.")
            return success
    return wrapper


def _read_postgres_dsn(conn_id: str) -> str:
    """Postgres 타입의 Connection으로부터 Postgres DSN 문자열을 생성한다."""
    from airflow.sdk import Connection

    airflow_connection: Connection = Connection.get(conn_id)
    return "postgresql://{user}:{password}@{host}:{port}/{dbname}".format(
        user = airflow_connection.login,
        password = airflow_connection.password,
        host = airflow_connection.host,
        port = airflow_connection.port,
        dbname = airflow_connection.schema,
    )


@postgres_client
def load_pg_table_from_duckdb(
        connection: DuckDBConnection,
        source_table: DuckDBTable,
        target_table: PgTable,
        columns: Sequence[str] = list(),
        if_source_table_empty: Literal["break", "continue"] | None = "break",
        if_target_table_not_found: Literal["break", "create", "raise"] = "raise",
        **kwargs
    ) -> bool:
    """DuckDB 테이블 행을 PostgreSQL 테이블에 적재한다."""
    client: PostgresClient = kwargs["client"]
    return client.load_table_from_duckdb(
        connection, source_table, target_table, columns,
        if_source_table_empty, if_target_table_not_found,
        install_extension=True
    )


@postgres_client
def overwrite_pg_table_from_duckdb(
        connection: DuckDBConnection,
        source_table: DuckDBTable,
        target_table: PgTable,
        columns: Sequence[str] = list(),
        where_clause: str | None = None,
        if_source_table_empty: Literal["break", "continue"] | None = "break",
        if_target_table_not_found: Literal["break", "create", "raise"] = "raise",
        cleanup_staging_table: bool = True,
        **kwargs
    ) -> bool:
    """DuckDB 테이블을 스테이징 테이블에 적재한 후,
    PostgreSQL 테이블의 기존 데이터를 삭제하고 스테이징 테이블 행을 덮어쓴다.

    **NOTE** 삭제(DELETE) 및 적재(INSERT) 쿼리 실행 앞뒤로 트랜잭션 시작과 커밋을 실행하며,   
    쿼리가 실패하면 직전 시점으로 롤백한다.
    """
    client: PostgresClient = kwargs["client"]
    return client.overwrite_table_from_duckdb(
        connection, source_table, target_table, columns, where_clause,
        if_source_table_empty, if_target_table_not_found, cleanup_staging_table,
        install_extension=True
    )


@postgres_client
def merge_into_pg_table_from_duckdb(
        connection: DuckDBConnection,
        source_table: DuckDBTable,
        target_table: PgTable,
        columns: Sequence[str] = list(),
        where_clause: str | None = None,
        on_conflict: str | Sequence[str] = list(),
        matched: str
            | dict[str, Literal["replace", "ignore", "greatest", "least", "source_first", "target_first"]]
            | Literal[":replace_all:", ":do_nothing:"] = ":replace_all:",
        not_matched: str | Sequence[str] | Literal[":insert_all:", ":do_nothing:"] = ":insert_all:",
        if_source_table_empty: Literal["break", "continue"] | None = "break",
        if_target_table_not_found: Literal["break", "create", "raise"] = "raise",
        cleanup_staging_table: bool = True,
        **kwargs
    ) -> bool:
    """DuckDB 테이블을 스테이징 테이블에 적재한 후, 스테이징 테이블을 PostgreSQL 테이블에 MERGE 한다.

    **NOTE** WHERE 절에서 타겟 테이블 칼럼은 `T.`, 소스 테이블 칼럼은 `S.`로 참조한다.
    """
    client: PostgresClient = kwargs["client"]
    return client.merge_table_from_duckdb(
        connection, source_table, target_table, columns,
        where_clause, on_conflict, matched, not_matched,
        if_source_table_empty, if_target_table_not_found, cleanup_staging_table,
        install_extension=True
    )


###################################################################
######################## from Google Sheets #######################
###################################################################

def overwrite_table_from_gsheets(
        key: str,
        sheet: str,
        table: str,
        columns: Sequence[str],
        primary_key: Sequence[str] | None = None,
        not_null: Sequence[str] | None = None,
        apply_func: dict[str, Callable[[Any], Any]] | None = None,
        gcp_conn_id: str = "gcp_bigquery",
        postgres_conn_id: str = "postgres",
        head: int = 1,
        numericise_ignore: Sequence[int] | bool = list(),
        **read_options
    ) -> list[dict]:
    """구글시트를 읽어 PostgreSQL 및 BigQuery 테이블에 적재한다. 신규 행이 있다면 반환한다."""
    from linkmerce.extensions.gsheets import WorksheetClient
    from linkmerce.extensions.postgres import PostgresClient
    from linkmerce.extensions.bigquery import BigQueryClient

    account = _read_google_service_account(gcp_conn_id)
    gs = WorksheetClient(account, key, sheet)
    rows, new_rows, unique = list(), list(), set()

    # 1. 구글시트에서 특정 시트를 읽어오면서 PK, NOT NULL 등 제약 조건을 검증한다.
    for record in gs.get_all_records(head=head, numericise_ignore=numericise_ignore, **read_options):
        if primary_key:
            identifier = tuple(record[key] for key in primary_key if record[key])
            if (not identifier) or (identifier in unique):
                continue
            unique.add(identifier)
        if not_null:
            if not [record[key] for key in not_null if record[key]]:
                continue
        if apply_func:
            for key, func in apply_func.items():
                record[key] = func(record[key])
        rows.append({column: record[column] for column in columns} if columns else record)

    # 2. PostgreSQL 테이블에 데이터를 덮어쓰기로 적재한다.
    with PostgresClient(_read_postgres_dsn(postgres_conn_id)) as pg_client:
        with pg_client.conn.cursor() as cursor:
            pg_client.execute("BEGIN;", cursor=cursor)
            try:
                pg_client.execute(f"TRUNCATE TABLE {table};", cursor=cursor)
                pg_client.insert_into_table_from_json(table, rows, cursor=cursor)
                pg_client.execute("COMMIT;", cursor=cursor)
            except:
                pg_client.execute("ROLLBACK;", cursor=cursor)
                raise

    # 3. BigQuery 테이블에 데이터를 덮어쓰기로 적재한다.
    with BigQueryClient(account) as bq_client:
        # 3-1. PK 또는 전체 행을 기준으로 기존 데이터와 비교하여 신규 행을 특정한다.
        criteria = primary_key if primary_key else columns
        query = f"SELECT DISTINCT {', '.join(criteria)} FROM {table};"
        exists = bq_client.fetch_all_to_csv(query, header=False)
        for row in rows:
            identifier = tuple(row[key] for key in criteria if row[key])
            if identifier not in exists:
                new_rows.append(row)

        bq_client.load_table_from_json(table, rows, write="truncate")

    return new_rows
