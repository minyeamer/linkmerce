from __future__ import annotations

from linkmerce.common.api import run_with_duckdb
from linkmerce.common.api import get_table_from_options as get_table

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


def get_module(name: str) -> str:
    return (".smartstore.brand" + name) if name.startswith('.') else name


def brand_catalog(
        cookies: str,
        brand_ids: str | Iterable[str],
        sort_type: Literal["popular","recent","price"] = "poular",
        is_brand_catalog: bool | None = None,
        page: int | list[int] | None = 0,
        page_size: int = 100,
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    args = (brand_ids, sort_type, is_brand_catalog, page, page_size)
    table = get_table(transform_options, "table")
    extract_options = dict(extract_options or dict(), headers=dict(cookies=cookies))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".catalog"), "BrandCatalog", "BrandCatalog", connection, how, table, return_type, args, **options)


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
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    args = (brand_ids, mall_seq, sort_type, is_brand_catalog, page, page_size)
    table = get_table(transform_options, "table")
    extract_options = dict(extract_options or dict(), headers=dict(cookies=cookies))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".catalog"), "BrandProduct", "BrandProduct", connection, how, table, return_type, args, **options)


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
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    args = (brand_ids, mall_seq, sort_type, is_brand_catalog, page, page_size)
    table = [get_table(transform_options, "price_table"), get_table(transform_options, "product_table", "product")]
    extract_options = dict(extract_options or dict(), headers=dict(cookies=cookies))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".catalog"), "BrandPrice", "BrandPrice", connection, how, table, return_type, args, **options)


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
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    args = (brand_ids, mall_seq, sort_type, is_brand_catalog, page, page_size)
    table = get_table(transform_options, "table")
    extract_options = dict(extract_options or dict(), headers=dict(cookies=cookies))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".catalog"), "ProductCatalog", "ProductCatalog", connection, how, table, return_type, args, **options)


def store_sales(
        cookies: str,
        mall_seq: int | str | Iterable[int | str],
        start_date: dt.date | str,
        end_date: dt.date | str,
        date_type: Literal["daily","weekly","monthly"] = "daily",
        page: int | Iterable[int] = 1,
        page_size: int = 1000,
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    args = (mall_seq, start_date, end_date, date_type, page, page_size)
    table = get_table(transform_options, "table")
    extract_options = dict(extract_options or dict(), headers=dict(cookies=cookies))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".sales"), "StoreSales", "StoreSales", connection, how, table, return_type, args, **options)


def category_sales(
        cookies: str,
        mall_seq: int | str | Iterable[int | str],
        start_date: dt.date | str,
        end_date: dt.date | str,
        date_type: Literal["daily","weekly","monthly"] = "daily",
        page: int | Iterable[int] = 1,
        page_size: int = 1000,
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    args = (mall_seq, start_date, end_date, date_type, page, page_size)
    table = get_table(transform_options, "table")
    extract_options = dict(extract_options or dict(), headers=dict(cookies=cookies))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".sales"), "CategorySales", "CategorySales", connection, how, table, return_type, args, **options)


def product_sales(
        cookies: str,
        mall_seq: int | str | Iterable[int | str],
        start_date: dt.date | str,
        end_date: dt.date | str,
        date_type: Literal["daily","weekly","monthly"] = "daily",
        page: int | Iterable[int] = 1,
        page_size: int = 1000,
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    args = (mall_seq, start_date, end_date, date_type, page, page_size)
    table = get_table(transform_options, "table")
    extract_options = dict(extract_options or dict(), headers=dict(cookies=cookies))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".sales"), "ProductSales", "ProductSales", connection, how, table, return_type, args, **options)


def aggregated_sales(
        cookies: str,
        mall_seq: int | str | Iterable[int | str],
        start_date: dt.date | str,
        end_date: dt.date | str,
        date_type: Literal["daily","weekly","monthly"] = "daily",
        page: int | Iterable[int] = 1,
        page_size: int = 1000,
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    args = (mall_seq, start_date, end_date, date_type, page, page_size)
    table = [get_table(transform_options, "sales_table"), get_table(transform_options, "product_table", "product")]
    extract_options = dict(extract_options or dict(), headers=dict(cookies=cookies))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".sales"), "AggregatedSales", "AggregatedSales", connection, how, table, return_type, args, **options)
