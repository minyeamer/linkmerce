from __future__ import annotations

from linkmerce.common.api import run_with_duckdb
from linkmerce.common.api import get_table_from_options as get_table

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection


def get_module(name: str) -> str:
    return (".naver.main" + name) if name.startswith('.') else name


def shopping_page(
        query: str | Iterable[str],
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    args = (query,)
    table = get_table(transform_options, "table")
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".search"), "ShoppingPage", "ShoppingPage", connection, how, table, return_type, args, **options)
