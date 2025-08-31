from __future__ import annotations

from linkmerce.common.api import run_with_duckdb, update_options

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection


def get_module(name: str) -> str:
    return (".naver.openapi" + name) if name.startswith('.') else name


def get_options(
        retry_count: int = 5,
        request_delay: float | int = 0.3,
        async_limit: int = 3,
    ) -> dict:
    return dict(
        RequestLoop = dict(count=retry_count),
        RequestEachLoop = dict(delay=request_delay, limit=async_limit))


def get_variables(
        client_id: str,
        client_secret: str,
    ) -> dict:
    return dict(client_id=client_id, client_secret=client_secret)


def _search(
        client_id: str,
        client_secret: str,
        content_type: Literal["Blog", "News", "Book", "Cafe", "KiN", "Image", "Shopping"],
        args: tuple,
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        retry_count: int = 5,
        request_delay: float | int = 0.3,
        async_limit: int = 3,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.naver.openapi.search.extract import _SearchExtractor
    # from linkmerce.core.naver.openapi.search.transform import _SearchTransformer
    components = (get_module(".search"), f"{content_type}Search", f"{content_type}Search")
    extract_options = update_options(extract_options,
        options = get_options(retry_count, request_delay, async_limit),
        variables = get_variables(client_id, client_secret))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, how, return_type, args=args, **options)


def search_blog(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim","date"] = "sim",
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        retry_count: int = 5,
        request_delay: float | int = 0.3,
        async_limit: int = 3,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.naver.openapi.search.extract import BlogSearch
    # from linkmerce.core.naver.openapi.search.transform import BlogSearch
    return _search(
        client_id, client_secret, "Blog", (query, start, display, sort), connection,
        how, retry_count, request_delay, async_limit, return_type, extract_options, transform_options)


def search_news(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim","date"] = "sim",
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        retry_count: int = 5,
        request_delay: float | int = 0.3,
        async_limit: int = 3,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.naver.openapi.search.extract import NewsSearch
    # from linkmerce.core.naver.openapi.search.transform import NewsSearch
    return _search(
        client_id, client_secret, "News", (query, start, display, sort), connection,
        how, retry_count, request_delay, async_limit, return_type, extract_options, transform_options)


def search_book(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim","date"] = "sim",
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        retry_count: int = 5,
        request_delay: float | int = 0.3,
        async_limit: int = 3,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.naver.openapi.search.extract import BookSearch
    # from linkmerce.core.naver.openapi.search.transform import BookSearch
    return _search(
        client_id, client_secret, "Book", (query, start, display, sort), connection,
        how, retry_count, request_delay, async_limit, return_type, extract_options, transform_options)


def search_cafe(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim","date"] = "sim",
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        retry_count: int = 5,
        request_delay: float | int = 0.3,
        async_limit: int = 3,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.naver.openapi.search.extract import CafeSearch
    # from linkmerce.core.naver.openapi.search.transform import CafeSearch
    return _search(
        client_id, client_secret, "Cafe", (query, start, display, sort), connection,
        how, retry_count, request_delay, async_limit, return_type, extract_options, transform_options)


def search_kin(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim","date","point"] = "sim",
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        retry_count: int = 5,
        request_delay: float | int = 0.3,
        async_limit: int = 3,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.naver.openapi.search.extract import KiNSearch
    # from linkmerce.core.naver.openapi.search.transform import KiNSearch
    return _search(
        client_id, client_secret, "KiN", (query, start, display, sort), connection,
        how, retry_count, request_delay, async_limit, return_type, extract_options, transform_options)


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
        retry_count: int = 5,
        request_delay: float | int = 0.3,
        async_limit: int = 3,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.naver.openapi.search.extract import ImageSearch
    # from linkmerce.core.naver.openapi.search.transform import ImageSearch
    return _search(
        client_id, client_secret, "Image", (query, start, display, sort, filter), connection,
        how, retry_count, request_delay, async_limit, return_type, extract_options, transform_options)


def search_shop(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim","date","asc","dsc"] = "sim",
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        retry_count: int = 5,
        request_delay: float | int = 0.3,
        async_limit: int = 3,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.naver.openapi.search.extract import ShoppingSearch
    # from linkmerce.core.naver.openapi.search.transform import ShoppingSearch
    return _search(
        client_id, client_secret, "Shopping", (query, start, display, sort), connection,
        how, retry_count, request_delay, async_limit, return_type, extract_options, transform_options)


def rank_shop(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim","date","asc","dsc"] = "sim",
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        retry_count: int = 5,
        request_delay: float | int = 0.3,
        async_limit: int = 3,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> dict[str,JsonObject]:
    """`tables = {'rank': 'naver_rank_shop', 'product': 'naver_product'}`"""
    # from linkmerce.core.naver.openapi.search.extract import ShoppingRank
    # from linkmerce.core.naver.openapi.search.transform import ShoppingRank
    components = (get_module(".search"), "ShoppingRank", "ShoppingRank")
    extract_options = update_options(extract_options,
        options = get_options(retry_count, request_delay, async_limit),
        variables = get_variables(client_id, client_secret))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, how, return_type, args=(query, start, display, sort), **options)
