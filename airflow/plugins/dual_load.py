from __future__ import annotations

from typing import TYPE_CHECKING
from functools import wraps

if TYPE_CHECKING:
    from typing import Literal, Sequence, TypedDict, TypeVar
    from linkmerce.common.load import DuckDBConnection
    from linkmerce.extensions.bigquery import BigQueryClient
    from linkmerce.extensions.postgres import PostgresClient

    class LoadResult(TypedDict):
        src_count: int
        bq_success: bool
        pg_success: bool

    DuckDBTable = TypeVar("DuckDBTable", str)
    BigQueryTable = TypeVar("BigQueryTable", str)
    PgTable = TypeVar("PgTable", str)


def load_table_from_duckdb(
        connection: DuckDBConnection,
        source_table: DuckDBTable,
        target_table: PgTable,
        columns: Sequence[str] = list(),
        **kwargs
    ) -> LoadResult:
    """DuckDB 테이블 행을 BigQuery/PostgreSQL 테이블에 적재한다."""
    common = (connection, source_table, target_table, columns)
    result = {"src_count": connection.count_table(source_table)}

    try: result["bq_success"] = load_bq_table_from_duckdb(*common, **kwargs)
    except: result["bq_success"] = False

    try: result["pg_success"] = load_pg_table_from_duckdb(*common, **kwargs)
    except: result["pg_success"] = False

    return result


def overwrite_table_from_duckdb(
        connection: DuckDBConnection,
        source_table: DuckDBTable,
        target_table: PgTable,
        columns: Sequence[str] = list(),
        where_clause: str | None = None,
        **kwargs
    ) -> LoadResult:
    """DuckDB 테이블을 스테이징 테이블에 적재한 후,
    BigQuery/PostgreSQL 테이블의 기존 데이터를 삭제하고 스테이징 테이블 행을 덮어쓴다.

    **NOTE** 삭제(DELETE) 및 적재(INSERT) 쿼리 실행 앞뒤로 트랜잭션 시작과 커밋을 실행하며,   
    쿼리가 실패하면 직전 시점으로 롤백한다.
    """
    common = (connection, source_table, target_table, columns, where_clause)
    result = {"src_count": connection.count_table(source_table)}

    try: result["bq_success"] = overwrite_bq_table_from_duckdb(*common, **kwargs)
    except: result["bq_success"] = False

    try: result["pg_success"] = overwrite_pg_table_from_duckdb(*common, **kwargs)
    except: result["pg_success"] = False

    return result


def upsert_table_from_duckdb(
        connection: DuckDBConnection,
        source_table: DuckDBTable,
        target_table: PgTable,
        columns: Sequence[str] = list(),
        where_clause: str | None = None,
        on_conflict: str | Sequence[str] = list(),
        do_action: str
            | dict[str, Literal["replace", "ignore", "greatest", "least", "source_first", "target_first"]]
            | Literal[":replace_all:", ":do_nothing:"] = ":replace_all:",
        **kwargs
    ) -> LoadResult:
    """DuckDB 테이블을 스테이징 테이블에 적재한 후, 스테이징 테이블을 BigQuery 테이블에 UPESRT 한다.

    **NOTE** WHERE 절에서 소스 테이블 칼럼은 `S.`로 참조한다.
    """
    common = (connection, source_table, target_table, columns, where_clause, on_conflict, do_action)
    result = {"src_count": connection.count_table(source_table)}

    try: result["bq_success"] = merge_into_bq_table_from_duckdb(*common, **kwargs)
    except: result["bq_success"] = False

    try: result["pg_success"] = upsert_pg_table_from_duckdb(*common, **kwargs)
    except: result["pg_success"] = False

    return result


###################################################################
############################# BigQuery ############################
###################################################################

def bigquery_client(func):
    @wraps(func)
    def wrapper(*args, bigquery_conn_id: str = "bigquery", **kwargs):
        from linkmerce.extensions.bigquery import BigQueryClient

        service_account = _read_google_service_account(bigquery_conn_id)
        with BigQueryClient(service_account) as client:
            return func(*args, **kwargs, client=client)
    return wrapper


def _read_google_service_account(conn_id: str) -> dict:
    from airflow.sdk import Connection

    airflow_connection: Connection = Connection.get(conn_id)
    return airflow_connection.extra_dejson


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
    return client.merge_into_table_from_duckdb(
        connection, source_table, target_table, columns,
        where_clause, on_conflict, matched, not_matched, schema,
        if_source_table_empty, if_target_table_not_found, cleanup_staging_table
    )


###################################################################
############################ PostgreSQL ###########################
###################################################################

def postgres_client(func):
    @wraps(func)
    def wrapper(*args, postgres_conn_id: str = "postgres", **kwargs):
        from linkmerce.extensions.postgres import PostgresClient

        connection_fields = _read_postgres_dsn(postgres_conn_id)
        with PostgresClient("", **connection_fields) as client:
            return func(*args, **kwargs, client=client)
    return wrapper


def _read_postgres_dsn(conn_id: str) -> str:
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


def upsert_pg_table_from_duckdb(
        connection: DuckDBConnection,
        source_table: DuckDBTable,
        target_table: PgTable,
        columns: Sequence[str] = list(),
        where_clause: str | None = None,
        on_conflict: str | Sequence[str] = list(),
        do_action: str
            | dict[str, Literal["replace", "ignore", "greatest", "least", "source_first", "target_first"]]
            | Literal[":replace_all:", ":do_nothing:"] = ":replace_all:",
        if_source_table_empty: Literal["break", "continue"] | None = "break",
        if_target_table_not_found: Literal["break", "create", "raise"] = "raise",
        cleanup_staging_table: bool = True,
        **kwargs
    ) -> bool:
    """DuckDB 테이블을 스테이징 테이블에 적재한 후, 스테이징 테이블을 BigQuery 테이블에 UPESRT 한다.

    **NOTE** WHERE 절에서 소스 테이블 칼럼은 `S.`로 참조한다.
    """
    client: PostgresClient = kwargs["client"]
    return client.upsert_table_from_duckdb(
        connection, source_table, target_table, columns,
        where_clause, on_conflict, do_action,
        if_source_table_empty, if_target_table_not_found, cleanup_staging_table,
        install_extension=True
    )
