from __future__ import annotations

from linkmerce.common.api import run_with_duckdb
from linkmerce.common.api import get_table_from_options as get_table

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection


def get_module(name: str) -> str:
    return (".naver.openapi" + name) if name.startswith('.') else name


def search_blog(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim","date"] = "sim",
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    args = (query, start, display, sort)
    table = get_table(transform_options, "table")
    extract_options = dict(extract_options or dict(), variables=dict(client_id=client_id, client_secret=client_secret))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".search"), "BlogSearch", "BlogSearch", connection, how, table, return_type, args, **options)


def search_news(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim","date"] = "sim",
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    args = (query, start, display, sort)
    table = get_table(transform_options, "table")
    extract_options = dict(extract_options or dict(), variables=dict(client_id=client_id, client_secret=client_secret))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".search"), "NewsSearch", "NewsSearch", connection, how, table, return_type, args, **options)


def search_book(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim","date"] = "sim",
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    args = (query, start, display, sort)
    table = get_table(transform_options, "table")
    extract_options = dict(extract_options or dict(), variables=dict(client_id=client_id, client_secret=client_secret))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".search"), "BookSearch", "BookSearch", connection, how, table, return_type, args, **options)


def search_cafe(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim","date"] = "sim",
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    args = (query, start, display, sort)
    table = get_table(transform_options, "table")
    extract_options = dict(extract_options or dict(), variables=dict(client_id=client_id, client_secret=client_secret))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".search"), "CafeSearch", "CafeSearch", connection, how, table, return_type, args, **options)


def search_kin(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim","date","point"] = "sim",
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    args = (query, start, display, sort)
    table = get_table(transform_options, "table")
    extract_options = dict(extract_options or dict(), variables=dict(client_id=client_id, client_secret=client_secret))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".search"), "KiNSearch", "KiNSearch", connection, how, table, return_type, args, **options)


def search_image(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim","date"] = "sim",
        filter: Literal["all","large","medium","small"] = "all",
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    args = (query, start, display, sort, filter)
    table = get_table(transform_options, "table")
    extract_options = dict(extract_options or dict(), variables=dict(client_id=client_id, client_secret=client_secret))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".search"), "ImageSearch", "ImageSearch", connection, how, table, return_type, args, **options)


def search_shop(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim","date","asc","dsc"] = "sim",
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    args = (query, start, display, sort)
    table = get_table(transform_options, "table")
    extract_options = dict(extract_options or dict(), variables=dict(client_id=client_id, client_secret=client_secret))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".search"), "ShoppingSearch", "ShoppingSearch", connection, how, table, return_type, args, **options)


def rank_shop(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim","date","asc","dsc"] = "sim",
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str,JsonObject]:
    args = (query, start, display, sort)
    table = [get_table(transform_options, "rank_table"), get_table(transform_options, "product_table", "product")]
    extract_options = dict(extract_options or dict(), variables=dict(client_id=client_id, client_secret=client_secret))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".search"), "ShoppingRank", "ShoppingRank", connection, how, table, return_type, args, **options)
