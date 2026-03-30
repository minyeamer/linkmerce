from __future__ import annotations

from typing import TYPE_CHECKING
import functools

if TYPE_CHECKING:
    from typing import Any, Literal
    from linkmerce.common.load import DuckDBConnection
    from linkmerce.common.transform import Transformer, DuckDBTransformer


def prepare_extract(
        transformer_cls: type[Transformer],
        extract_options: dict | None = None,
        transform_options: dict | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        **kwargs
    ) -> dict:
    """다음 2가지 경우에 모두 해당된다면 `Transformer` 객체를 생성하고   
    `extract_options`에 `transform` 메서드를 `parser`로 할당한다.
    1. `return_type`이 `"raw"`가 아닌 경우
    2. `extract_options`에 `parser`가 없는 경우

    그 외에 키워드 인자로 전달되는 값을 `extract_options`에 추가한다."""
    from linkmerce.utils.nested import merge
    extract_options = extract_options or dict()

    if (return_type != "raw") and ("parser" not in extract_options):
        transformer = transformer_cls(**(transform_options or dict()))
        extract_options["parser"] = transformer.transform

    return merge(extract_options, kwargs) if kwargs else extract_options


def prepare_duckdb_extract(
        transformer_cls: type[DuckDBTransformer],
        connection: DuckDBConnection,
        extract_options: dict | None = None,
        transform_options: dict | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        **kwargs
    ) -> dict:
    """다음 2가지 경우에 모두 해당된다면 `DuckDBTransformer` 객체를 생성하고   
    `extract_options`에 `transform` 메서드를 `parser`로 할당한다.
    1. `return_type`이 `"raw"`가 아닌 경우
    2. `extract_options`에 `parser`가 없는 경우

    그 외에 키워드 인자로 전달되는 값을 `extract_options`에 추가한다."""
    from linkmerce.utils.nested import merge
    extract_options = extract_options or dict()

    if (return_type != "raw") and ("parser" not in extract_options):
        db_info = {"db_info": {"conn": connection}}
        transformer = transformer_cls(**merge(transform_options or dict(), db_info))
        extract_options["parser"] = transformer.transform

    return merge(extract_options, kwargs) if kwargs else extract_options


def with_duckdb_connection(tables: dict | None = None, table: dict | None = None):
    """DuckDB 연결을 보장한다. `connection`이 없다면 연결을 생성하고, 실행 종료 후 연결을 닫는다.

    함수의 반환값이 있다면 `return_type`에 따라 결과를 형식에 맞게 변환하여 반환한다:
    - `csv`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
    - `json`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다.
    - `parquet`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
    - `raw`: 함수의 반환값을 그대로 반환한다.
    - `none`: 아무것도 반환하지 않는다.

    조회할 대상 테이블(`tables`)을 데코레이터 호출 시점에 지정할 수 있다."""
    tables = {"table": table} if table else tables

    def decorator(func):
        @functools.wraps(func)
        def wrapper(
                *args,
                connection: DuckDBConnection | None = None,
                return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
                **kwargs
            ) -> Any | dict[str, Any] | None:
            if connection is not None:
                result = func(*args, connection=connection, return_type=return_type, **kwargs)
                return result if return_type == "raw" else fetch_all_duckdb_tables(connection, tables, return_type)

            from linkmerce.common.load import DuckDBConnection
            with DuckDBConnection() as conn:
                result = func(*args, connection=conn, return_type=return_type, **kwargs)
                return result if return_type == "raw" else fetch_all_duckdb_tables(conn, tables, return_type)

        return wrapper
    return decorator


def fetch_all_duckdb_tables(
        connection: DuckDBConnection,
        tables: dict | None = None,
        return_type: Literal["csv", "json", "parquet", "none"] = "json",
    ) -> Any | dict[str, Any] | None:
    """DuckDB 연결에서 테이블 데이터를 조회하여 지정된 형식으로 반환한다."""
    exists = connection.show_tables()
    if tables:
        tables = {key: name for key, name in tables.items() if name in exists}
    else:
        tables = {"table": exists}

    if return_type == "none":
        return None
    elif not tables:
        tables = {table: table for table in connection.show_tables()}
        return fetch_all_duckdb_tables(connection, tables, return_type) if tables else None
    elif len(tables) == 1:
        return connection.fetch_all(return_type, f"SELECT * FROM {list(tables.values())[0]}")
    else:
        return {key: connection.fetch_all(return_type, f"SELECT * FROM {name}")
                for key, name in tables.items()}
