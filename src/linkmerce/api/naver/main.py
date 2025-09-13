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
        max_concurrent: int = 3,
        max_retries: int = 5,
        request_delay: float | int = 1.01,
        progress: bool = True,
    ) -> dict:
    return dict(
        RequestLoop = dict(max_retries=max_retries, ignored_errors=ConnectionError),
        RequestEachLoop = dict(request_delay=request_delay, max_concurrent=max_concurrent, tqdm_options=dict(disable=(not progress))),
    )


def shopping_page(
        query: str | Iterable[str],
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        max_concurrent: int = 3,
        max_retries: int = 5,
        request_delay: float | int = 1.01,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        progress: bool = True,
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.naver.main.search.extract import ShoppingProduct
    # from linkmerce.core.naver.main.search.transform import ShoppingProduct
    return run_with_duckdb(
        module = get_module(".search"),
        extractor = "ShoppingPage",
        transformer = "ShoppingPage",
        connection = connection,
        tables = tables,
        how = how,
        return_type = return_type,
        args = (query,),
        extract_options = update_options(
            extract_options,
            options = get_options(max_concurrent, max_retries, request_delay, progress),
        ),
        transform_options = transform_options,
    )
