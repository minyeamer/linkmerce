from __future__ import annotations

from linkmerce.common.api import run_with_duckdb, update_options

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    from pathlib import Path
    import datetime as dt


def get_module(name: str) -> str:
    return (".smartstore.brand" + name) if name.startswith('.') else name


def login(
        userid: str | None = None,
        passwd: str | None = None,
        channel_seq: int | str | None = None,
        cookies: str | None = None,
        save_to: str | Path | None = None,
    ) -> str:
    from linkmerce.core.smartstore.brand.common import PartnerCenterLogin
    handler = PartnerCenterLogin()
    handler.login(userid, passwd, channel_seq, cookies)
    cookies = handler.get_cookies()
    if cookies and save_to:
        with open(save_to, 'w', encoding="utf-8") as file:
            file.write(cookies)
    return cookies


def get_catalog_options(
        request_delay: float | int = 1,
        async_limit: int = 3,
    ) -> dict:
    return dict(
        PaginateAll = dict(delay=request_delay, limit=async_limit),
        RequestEachPages = dict(delay=request_delay, limit=async_limit))


def get_sales_options(
        request_delay: float | int = 1,
        async_limit: int = 3,
    ) -> dict:
    return dict(RequestEach = dict(delay=request_delay, limit=async_limit))


def brand_catalog(
        cookies: str,
        brand_ids: str | Iterable[str],
        sort_type: Literal["popular","recent","price"] = "poular",
        is_brand_catalog: bool | None = None,
        page: int | list[int] | None = 0,
        page_size: int = 100,
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        request_delay: float | int = 1,
        async_limit: int = 3,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.brand.catalog.extract import BrandCatalog
    # from linkmerce.core.smartstore.brand.catalog.transform import BrandCatalog
    components = (get_module(".catalog"), "BrandCatalog", "BrandCatalog")
    args = (brand_ids, sort_type, is_brand_catalog, page, page_size)
    extract_options = update_options(extract_options,
        headers = dict(cookies=cookies),
        options = get_catalog_options(request_delay, async_limit))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, how, return_type, args, **options)


def brand_product(
        cookies: str,
        brand_ids: str | Iterable[str],
        mall_seq: int | str | Iterable[int | str] | None = None,
        sort_type: Literal["popular","recent","price"] = "poular",
        is_brand_catalog: bool | None = None,
        page: int | list[int] | None = 0,
        page_size: int = 100,
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        request_delay: float | int = 1,
        async_limit: int = 3,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.brand.catalog.extract import BrandProduct
    # from linkmerce.core.smartstore.brand.catalog.transform import BrandProduct
    components = (get_module(".catalog"), "BrandProduct", "BrandProduct")
    args = (brand_ids, mall_seq, sort_type, is_brand_catalog, page, page_size)
    extract_options = update_options(extract_options,
        headers = dict(cookies=cookies),
        options = get_catalog_options(request_delay, async_limit))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, how, return_type, args, **options)


def brand_price(
        cookies: str,
        brand_ids: str | Iterable[str],
        mall_seq: int | str | Iterable[int | str],
        sort_type: Literal["popular","recent","price"] = "recent",
        is_brand_catalog: bool | None = None,
        page: int | list[int] | None = 0,
        page_size: int = 100,
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        request_delay: float | int = 1,
        async_limit: int = 3,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> dict[str,JsonObject]:
    """`tables = {'price': 'naver_brand_price', 'product': 'naver_brand_product'}`"""
    # from linkmerce.core.smartstore.brand.catalog.extract import BrandPrice
    # from linkmerce.core.smartstore.brand.catalog.transform import BrandPrice
    components = (get_module(".catalog"), "BrandPrice", "BrandPrice")
    args = (brand_ids, mall_seq, sort_type, is_brand_catalog, page, page_size)
    extract_options = update_options(extract_options,
        headers = dict(cookies=cookies),
        options = get_catalog_options(request_delay, async_limit))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, how, return_type, args, **options)


def product_catalog(
        cookies: str,
        brand_ids: str | Iterable[str],
        mall_seq: int | str | Iterable[int | str],
        sort_type: Literal["popular","recent","price"] = "recent",
        is_brand_catalog: bool | None = None,
        page: int | list[int] | None = 0,
        page_size: int = 100,
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        request_delay: float | int = 1,
        async_limit: int = 3,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.brand.catalog.extract import ProductCatalog
    # from linkmerce.core.smartstore.brand.catalog.transform import ProductCatalog
    components = (get_module(".catalog"), "ProductCatalog", "ProductCatalog")
    args = (brand_ids, mall_seq, sort_type, is_brand_catalog, page, page_size)
    extract_options = update_options(extract_options,
        headers = dict(cookies=cookies),
        options = get_catalog_options(request_delay, async_limit))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, how, return_type, args, **options)


def store_sales(
        cookies: str,
        mall_seq: int | str | Iterable[int | str],
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["daily","weekly","monthly"] = "daily",
        page: int | Iterable[int] = 1,
        page_size: int = 1000,
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        request_delay: float | int = 1,
        async_limit: int = 3,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.brand.sales.extract import StoreSales
    # from linkmerce.core.smartstore.brand.sales.transform import StoreSales
    components = (get_module(".sales"), "StoreSales", "StoreSales")
    args = (mall_seq, start_date, end_date, date_type, page, page_size)
    extract_options = update_options(extract_options,
        headers = dict(cookies=cookies),
        options = get_sales_options(request_delay, async_limit))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, how, return_type, args, **options)


def category_sales(
        cookies: str,
        mall_seq: int | str | Iterable[int | str],
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["daily","weekly","monthly"] = "daily",
        page: int | Iterable[int] = 1,
        page_size: int = 1000,
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        request_delay: float | int = 1,
        async_limit: int = 3,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.brand.sales.extract import CategorySales
    # from linkmerce.core.smartstore.brand.sales.transform import CategorySales
    components = (get_module(".sales"), "CategorySales", "CategorySales")
    args = (mall_seq, start_date, end_date, date_type, page, page_size)
    extract_options = update_options(extract_options,
        headers = dict(cookies=cookies),
        options = get_sales_options(request_delay, async_limit))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, how, return_type, args, **options)


def product_sales(
        cookies: str,
        mall_seq: int | str | Iterable[int | str],
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["daily","weekly","monthly"] = "daily",
        page: int | Iterable[int] = 1,
        page_size: int = 1000,
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        request_delay: float | int = 1,
        async_limit: int = 3,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.brand.sales.extract import ProductSales
    # from linkmerce.core.smartstore.brand.sales.transform import ProductSales
    components = (get_module(".sales"), "ProductSales", "ProductSales")
    args = (mall_seq, start_date, end_date, date_type, page, page_size)
    extract_options = update_options(extract_options,
        headers = dict(cookies=cookies),
        options = get_sales_options(request_delay, async_limit))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, how, return_type, args, **options)


def aggregated_sales(
        cookies: str,
        mall_seq: int | str | Iterable[int | str],
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["daily","weekly","monthly"] = "daily",
        page: int | Iterable[int] = 1,
        page_size: int = 1000,
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        request_delay: float | int = 1,
        async_limit: int = 3,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> dict[str,JsonObject]:
    """`tables = {'sales': 'naver_brand_sales', 'product': 'naver_brand_product'}`"""
    # from linkmerce.core.smartstore.brand.sales.extract import AggregatedSales
    # from linkmerce.core.smartstore.brand.sales.transform import AggregatedSales
    components = (get_module(".sales"), "AggregatedSales", "AggregatedSales")
    args = (mall_seq, start_date, end_date, date_type, page, page_size)
    extract_options = update_options(extract_options,
        headers = dict(cookies=cookies),
        options = get_sales_options(request_delay, async_limit))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, how, return_type, args, **options)
