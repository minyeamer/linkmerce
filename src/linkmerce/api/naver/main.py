from __future__ import annotations

from linkmerce.common.api import run_with_duckdb, update_options

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection


def get_module(name: str) -> str:
    return (".naver.main" + name) if name.startswith('.') else name


def get_options(
        retry_count: int = 5,
        request_delay: float | int = 1.01,
        async_limit: int = 3,
    ) -> dict:
    return dict(
        RequestLoop = dict(count=retry_count, ignored_errors=ConnectionError),
        RequestEachLoop = dict(delay=request_delay, limit=async_limit))


def shopping_page(
        query: str | Iterable[str],
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        retry_count: int = 5,
        request_delay: float | int = 1.01,
        async_limit: int = 3,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.naver.main.search.extract import ShoppingProduct
    # from linkmerce.core.naver.main.search.transform import ShoppingProduct
    components = (get_module(".search"), "ShoppingPage", "ShoppingPage")
    extract_options = update_options(extract_options, options=get_options(retry_count, request_delay, async_limit))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, how, return_type, args=(query,), **options)
