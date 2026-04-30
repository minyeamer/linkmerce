from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    from pathlib import Path
    import datetime as dt


def login(
        userid: str | None = None,
        passwd: str | None = None,
        channel_seq: int | str | None = None,
        cookies: str | None = None,
        save_to: str | Path | None = None,
    ) -> str:
    """네이버 쇼핑파트너센터에 로그인하고 쿠키를 반환한다."""
    from linkmerce.core.smartstore.hcenter.common import PartnerCenterLogin
    from linkmerce.api.common import handle_cookies
    handler = PartnerCenterLogin()
    handler.login(userid, passwd, channel_seq, cookies)
    return handle_cookies(handler, save_to)


@with_duckdb_connection(table="naver_brand_catalog")
def brand_catalog(
        cookies: str,
        brand_ids: str | Iterable[str],
        sort_type: Literal["popular", "recent", "count", "price"] = "recent",
        is_brand_catalog: bool | None = None,
        page: int | list[int] | None = 0,
        page_size: int = 100,
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 브랜드스토어의 카탈로그 목록을 조회하고 `naver_brand_catalog` 테이블에 적재한다."""
    from linkmerce.core.smartstore.hcenter.catalog.extract import BrandCatalog
    from linkmerce.core.smartstore.hcenter.catalog.transform import BrandCatalog as T
    return BrandCatalog(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
        options = {
            "PaginateAll": {
                "max_concurrent": max_concurrent,
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)},
            },
            "RequestEachPages": {
                "max_concurrent": max_concurrent,
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)},
            },
        },
    )).run(brand_ids, sort_type, is_brand_catalog, page, page_size, how_to_run=how_to_run)


@with_duckdb_connection(table="naver_brand_product")
def brand_product(
        cookies: str,
        brand_ids: str | Iterable[str],
        mall_seq: int | str | Iterable[int | str] | None = None,
        sort_type: Literal["popular", "recent", "price"] = "recent",
        is_brand_catalog: bool | None = None,
        page: int | list[int] | None = 0,
        page_size: int = 100,
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 브랜드스토어의 상품 목록을 조회하고 `naver_brand_product` 테이블에 적재한다."""
    from linkmerce.core.smartstore.hcenter.catalog.extract import BrandProduct
    from linkmerce.core.smartstore.hcenter.catalog.transform import BrandProduct as T
    return BrandProduct(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
        options = {
            "PaginateAll": {
                "max_concurrent": max_concurrent,
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)},
            },
            "RequestEachPages": {
                "max_concurrent": max_concurrent,
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)},
            },
        },
    )).run(brand_ids, mall_seq, sort_type, is_brand_catalog, page, page_size, how_to_run=how_to_run)


@with_duckdb_connection(tables={"price": "naver_price_history", "product": "naver_product"})
def brand_price(
        cookies: str,
        brand_ids: str | Iterable[str],
        mall_seq: int | str | Iterable[int | str],
        sort_type: Literal["popular", "recent", "price"] = "recent",
        is_brand_catalog: bool | None = None,
        page: int | list[int] | None = 0,
        page_size: int = 100,
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str, JsonObject]:
    """네이버 브랜드스토어의 상품 조회 결과로부터 상품 가격 및 상품 목록을 각각의 테이블에 적재한다.

    테이블 키 | 테이블명 | 설명
    - `price` | `naver_price_history` | 네이버 브랜드 상품 가격
    - `product` | `naver_product` | 네이버 브랜드 상품 목록"""
    from linkmerce.core.smartstore.hcenter.catalog.extract import BrandProduct
    from linkmerce.core.smartstore.hcenter.catalog.transform import BrandPrice as T
    return BrandProduct(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
        options = {
            "PaginateAll": {
                "max_concurrent": max_concurrent,
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)},
            },
            "RequestEachPages": {
                "max_concurrent": max_concurrent,
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)},
            },
        },
    )).run(brand_ids, mall_seq, sort_type, is_brand_catalog, page, page_size, how_to_run=how_to_run)


@with_duckdb_connection(table="naver_catalog_product")
def product_catalog(
        cookies: str,
        brand_ids: str | Iterable[str],
        mall_seq: int | str | Iterable[int | str],
        sort_type: Literal["popular", "recent", "price"] = "recent",
        is_brand_catalog: bool | None = None,
        page: int | list[int] | None = 0,
        page_size: int = 100,
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 브랜드스토어의 카탈로그-상품 매핑 데이터를 조회하고 `naver_catalog_product` 테이블에 적재한다."""
    from linkmerce.core.smartstore.hcenter.catalog.extract import BrandProduct
    from linkmerce.core.smartstore.hcenter.catalog.transform import ProductCatalog as T
    return BrandProduct(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
        options = {
            "PaginateAll": {
                "max_concurrent": max_concurrent,
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)},
            },
            "RequestEachPages": {
                "max_concurrent": max_concurrent,
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)},
            },
        },
    )).run(brand_ids, mall_seq, sort_type, is_brand_catalog, page, page_size, how_to_run=how_to_run)


def page_view(
        cookies: str,
        aggregate_by: Literal["device", "product", "url"],
        mall_seq: int | str | Iterable[int | str],
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_retries: int = 5,
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str, JsonObject]:
    """네이버 브랜드스토어의 일별 페이지뷰 데이터를 조회해 집계 기준에 대응되는 하나의 테이블에 적재한다.

    집계 기준 | 테이블명 | 설명
    - `device` | `naver_pv_by_device` | 일별/기기별 페이지뷰
    - `product` | `naver_pv_by_product` | 일별/상품별 페이지뷰
    - `url` | `naver_pv_by_url` | 일별/URL별 페이지뷰"""
    if aggregate_by == "device":
        from linkmerce.core.smartstore.hcenter.pageview.extract import PageViewByDevice as E
        from linkmerce.core.smartstore.hcenter.pageview.transform import PageViewByDevice as T
        table = "naver_pv_by_device"
    elif aggregate_by == "product":
        from linkmerce.core.smartstore.hcenter.pageview.extract import PageViewByUrl as E
        from linkmerce.core.smartstore.hcenter.pageview.transform import PageViewByProduct as T
        table = "naver_pv_by_product"
    else:
        from linkmerce.core.smartstore.hcenter.pageview.extract import PageViewByUrl as E
        from linkmerce.core.smartstore.hcenter.pageview.transform import PageViewByUrl as T
        table = "naver_pv_by_url"

    @with_duckdb_connection(table=table)
    def _main(*args, connection: DuckDBConnection | None = None, return_type: str = "json", **kwargs):
        return E(**prepare_duckdb_extract(
            T, connection, extract_options, transform_options, return_type,
            cookies = cookies,
            options = {
                "RequestLoop": {
                    "max_retries": max_retries
                },
                "RequestEachLoop": {
                    "request_delay": request_delay,
                    "max_concurrent": max_concurrent,
                    "tqdm_options": {"disable": (not progress)}
                },
            },
        )).run(mall_seq, start_date, end_date, how_to_run=how_to_run)
    return _main(connection=connection, return_type=return_type)


def store_sales(
        cookies: str,
        sales_type: Literal["store", "category", "product"],
        mall_seq: int | str | Iterable[int | str],
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["daily", "weekly", "monthly"] = "daily",
        page: int | Iterable[int] = 1,
        page_size: int = 1000,
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_retries: int = 5,
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str, JsonObject]:
    """네이버 스토어의 일별 매출 데이터를 조회해 매출 유형에 대응되는 하나의 테이블에 적재한다.

    매출 유형 | 테이블명 | 설명
    - `store` | `naver_store_sales` | 일별/기기별 페이지뷰
    - `category` | `naver_category_sales` | 일별/상품별 페이지뷰
    - `product` | `naver_product_sales` | 일별/URL별 페이지뷰"""
    if sales_type == "store":
        from linkmerce.core.smartstore.hcenter.sales.extract import StoreSales as E
        from linkmerce.core.smartstore.hcenter.sales.transform import StoreSales as T
        table = "naver_store_sales"
    elif sales_type == "category":
        from linkmerce.core.smartstore.hcenter.sales.extract import CategorySales as E
        from linkmerce.core.smartstore.hcenter.sales.transform import CategorySales as T
        table = "naver_category_sales"
    else:
        from linkmerce.core.smartstore.hcenter.sales.extract import ProductSales as E
        from linkmerce.core.smartstore.hcenter.sales.transform import ProductSales as T
        table = "naver_product_sales"

    @with_duckdb_connection(table=table)
    def _main(*args, connection: DuckDBConnection | None = None, return_type: str = "json", **kwargs):
        return E(**prepare_duckdb_extract(
            T, connection, extract_options, transform_options, return_type,
            cookies = cookies,
            options = {
                "RequestLoop": {
                    "max_retries": max_retries
                },
                "RequestEachLoop": {
                    "request_delay": request_delay,
                    "max_concurrent": max_concurrent,
                    "tqdm_options": {"disable": (not progress)}
                },
            },
        )).run(mall_seq, start_date, end_date, date_type, page, page_size, how_to_run=how_to_run)
    return _main(connection=connection, return_type=return_type)


@with_duckdb_connection(tables={"sales": "naver_sales", "product": "naver_product"})
def aggregated_sales(
        cookies: str,
        mall_seq: int | str | Iterable[int | str],
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["daily", "weekly", "monthly"] = "daily",
        page: int | Iterable[int] = 1,
        page_size: int = 1000,
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_retries: int = 5,
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str, JsonObject]:
    """네이버 스토어의 일별/상품별 매출 데이터로부터 상품별 매출 및 상품 목록을 각각의 테이블에 적재한다.

    테이블 키 | 테이블명 | 설명
    - `sales` | `naver_sales` | 네이버 스토어 상품별 매출
    - `product` | `naver_product` | 네이버 스토어 상품 목록"""
    from linkmerce.core.smartstore.hcenter.sales.extract import ProductSales
    from linkmerce.core.smartstore.hcenter.sales.transform import AggregatedSales as T
    return ProductSales(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
        options = {
            "RequestLoop": {
                "max_retries": max_retries
            },
            "RequestEachLoop": {
                "request_delay": request_delay,
                "max_concurrent": max_concurrent,
                "tqdm_options": {"disable": (not progress)}
            },
        },
    )).run(mall_seq, start_date, end_date, date_type, page, page_size, how_to_run=how_to_run)
