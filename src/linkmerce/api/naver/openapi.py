from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection


def _search_config(
        client_id: str,
        client_secret: str,
        max_concurrent: int = 3,
        max_retries: int = 5,
        request_delay: float | int = 0.3,
        progress: bool = True,
    ) -> dict:
    """네이버 Open API 검색 Extractor의 기본 설정을 생성한다."""
    return dict(
        configs = {
            "client_id": client_id,
            "client_secret": client_secret
        },
        options = {
            "RequestLoop": {
                "max_retries": max_retries
            },
            "RequestEachLoop": {
                "request_delay": request_delay,
                "max_concurrent": max_concurrent,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )


@with_duckdb_connection(table="naver_blog")
def search_blog(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim", "date"] = "sim",
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        max_retries: int = 5,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 오픈 API로 블로그 검색 결과를 수집하고 `naver_blog` 테이블에 적재한다."""
    from linkmerce.core.naver.openapi.search.extract import BlogSearch
    from linkmerce.core.naver.openapi.search.transform import BlogSearch as T
    return BlogSearch(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        **_search_config(client_id, client_secret, max_concurrent, max_retries, request_delay, progress),
    )).run(query, start, display, sort, how_to_run=how_to_run)


@with_duckdb_connection(table="naver_news")
def search_news(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim", "date"] = "sim",
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        max_retries: int = 5,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 오픈 API로 뉴스 검색 결과를 수집하고 `naver_news` 테이블에 적재한다."""
    from linkmerce.core.naver.openapi.search.extract import NewsSearch
    from linkmerce.core.naver.openapi.search.transform import NewsSearch as T
    return NewsSearch(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        **_search_config(client_id, client_secret, max_concurrent, max_retries, request_delay, progress),
    )).run(query, start, display, sort, how_to_run=how_to_run)


@with_duckdb_connection(table="naver_book")
def search_book(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim", "date"] = "sim",
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        max_retries: int = 5,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 오픈 API로 책 검색 결과를 수집하고 `naver_book` 테이블에 적재한다."""
    from linkmerce.core.naver.openapi.search.extract import BookSearch
    from linkmerce.core.naver.openapi.search.transform import BookSearch as T
    return BookSearch(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        **_search_config(client_id, client_secret, max_concurrent, max_retries, request_delay, progress),
    )).run(query, start, display, sort, how_to_run=how_to_run)


@with_duckdb_connection(table="naver_cafe")
def search_cafe(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim", "date"] = "sim",
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        max_retries: int = 5,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 오픈 API로 카페 검색 결과를 수집하고 `naver_cafe` 테이블에 적재한다."""
    from linkmerce.core.naver.openapi.search.extract import CafeSearch
    from linkmerce.core.naver.openapi.search.transform import CafeSearch as T
    return CafeSearch(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        **_search_config(client_id, client_secret, max_concurrent, max_retries, request_delay, progress),
    )).run(query, start, display, sort, how_to_run=how_to_run)


@with_duckdb_connection(table="naver_kin")
def search_kin(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim", "date", "point"] = "sim",
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        max_retries: int = 5,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 오픈 API로 지식iN 검색 결과를 수집하고 `naver_kin` 테이블에 적재한다."""
    from linkmerce.core.naver.openapi.search.extract import KiNSearch
    from linkmerce.core.naver.openapi.search.transform import KiNSearch as T
    return KiNSearch(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        **_search_config(client_id, client_secret, max_concurrent, max_retries, request_delay, progress),
    )).run(query, start, display, sort, how_to_run=how_to_run)


@with_duckdb_connection(table="naver_image")
def search_image(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim", "date"] = "sim",
        filter: Literal["all", "large", "medium", "small"] = "all",
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        max_retries: int = 5,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 오픈 API로 이미지 검색 결과를 수집하고 `naver_image` 테이블에 적재한다."""
    from linkmerce.core.naver.openapi.search.extract import ImageSearch
    from linkmerce.core.naver.openapi.search.transform import ImageSearch as T
    return ImageSearch(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        **_search_config(client_id, client_secret, max_concurrent, max_retries, request_delay, progress),
    )).run(query, start, display, sort, filter, how_to_run=how_to_run)


@with_duckdb_connection(table="naver_shop")
def search_shop(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim", "date", "asc", "dsc"] = "sim",
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        max_retries: int = 5,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 오픈 API로 쇼핑 검색 결과를 수집하고 `naver_shop` 테이블에 적재한다."""
    from linkmerce.core.naver.openapi.search.extract import ShoppingSearch
    from linkmerce.core.naver.openapi.search.transform import ShoppingSearch as T
    return ShoppingSearch(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        **_search_config(client_id, client_secret, max_concurrent, max_retries, request_delay, progress),
    )).run(query, start, display, sort, how_to_run=how_to_run)


@with_duckdb_connection(tables={"rank": "naver_shop_rank", "product": "naver_shop_product"})
def rank_shop(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim", "date", "asc", "dsc"] = "sim",
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        max_retries: int = 5,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str, JsonObject]:
    """네이버 오픈 API 쇼핑 검색 결과로부터 상품 순위와 상품 목록을 분리하여 각각의 테이블에 변환 및 적재한다.

    테이블 키 | 테이블명 | 설명
    - `rank` | `naver_shop_rank` | 네이버 쇼핑 상품 순위
    - `product` | `naver_shop_product` | 네이버 쇼핑 상품 목록"""
    from linkmerce.core.naver.openapi.search.extract import ShoppingSearch
    from linkmerce.core.naver.openapi.search.transform import ShoppingRank as T
    return ShoppingSearch(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        **_search_config(client_id, client_secret, max_concurrent, max_retries, request_delay, progress),
    )).run(query, start, display, sort, how_to_run=how_to_run)
