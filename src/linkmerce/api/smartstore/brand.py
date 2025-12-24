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


def each_page_options(
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
    ) -> dict:
    return dict(
        PaginateAll = dict(max_concurrent=max_concurrent, request_delay=request_delay, tqdm_options=dict(disable=(not progress))),
        RequestEachPages = dict(max_concurrent=max_concurrent, request_delay=request_delay, tqdm_options=dict(disable=(not progress))),
    )


def each_request_options(
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
    ) -> dict:
    return dict(
        RequestEach = dict(request_delay=request_delay, max_concurrent=max_concurrent, tqdm_options=dict(disable=(not progress))),
    )


def each_request_loop_options(
        max_retries: int = 5,
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
    ) -> dict:
    return dict(
        RequestLoop = dict(max_retries=max_retries),
        RequestEachLoop = dict(request_delay=request_delay, max_concurrent=max_concurrent, tqdm_options=dict(disable=(not progress))),
    )


def brand_catalog(
        cookies: str,
        brand_ids: str | Iterable[str],
        sort_type: Literal["popular","recent","price"] = "recent",
        is_brand_catalog: bool | None = None,
        page: int | list[int] | None = 0,
        page_size: int = 100,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.brand.catalog.extract import BrandCatalog
    # from linkmerce.core.smartstore.brand.catalog.transform import BrandCatalog
    return run_with_duckdb(
        module = get_module(".catalog"),
        extractor = "BrandCatalog",
        transformer = "BrandCatalog",
        connection = connection,
        tables = tables,
        how = how,
        return_type = return_type,
        args = (brand_ids, sort_type, is_brand_catalog, page, page_size),
        extract_options = update_options(
            extract_options,
            headers = dict(cookies=cookies),
            options = each_page_options(max_concurrent, request_delay, progress),
        ),
        transform_options = transform_options,
    )


def brand_product(
        cookies: str,
        brand_ids: str | Iterable[str],
        mall_seq: int | str | Iterable[int | str] | None = None,
        sort_type: Literal["popular","recent","price"] = "recent",
        is_brand_catalog: bool | None = None,
        page: int | list[int] | None = 0,
        page_size: int = 100,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.brand.catalog.extract import BrandProduct
    # from linkmerce.core.smartstore.brand.catalog.transform import BrandProduct
    return run_with_duckdb(
        module = get_module(".catalog"),
        extractor = "BrandProduct",
        transformer = "BrandProduct",
        connection = connection,
        tables = tables,
        how = how,
        return_type = return_type,
        args = (brand_ids, mall_seq, sort_type, is_brand_catalog, page, page_size),
        extract_options = update_options(
            extract_options,
            headers = dict(cookies=cookies),
            options = each_page_options(max_concurrent, request_delay, progress),
        ),
        transform_options = transform_options,
    )


def brand_price(
        cookies: str,
        brand_ids: str | Iterable[str],
        mall_seq: int | str | Iterable[int | str],
        sort_type: Literal["popular","recent","price"] = "recent",
        is_brand_catalog: bool | None = None,
        page: int | list[int] | None = 0,
        page_size: int = 100,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> dict[str,JsonObject]:
    """`tables = {'price': 'naver_brand_price', 'product': 'naver_brand_product'}`"""
    # from linkmerce.core.smartstore.brand.catalog.extract import BrandProduct
    # from linkmerce.core.smartstore.brand.catalog.transform import BrandPrice
    return run_with_duckdb(
        module = get_module(".catalog"),
        extractor = "BrandProduct",
        transformer = "BrandPrice",
        connection = connection,
        tables = tables,
        how = how,
        return_type = return_type,
        args = (brand_ids, mall_seq, sort_type, is_brand_catalog, page, page_size),
        extract_options = update_options(
            extract_options,
            headers = dict(cookies=cookies),
            options = each_page_options(max_concurrent, request_delay, progress),
        ),
        transform_options = transform_options,
    )


def product_catalog(
        cookies: str,
        brand_ids: str | Iterable[str],
        mall_seq: int | str | Iterable[int | str],
        sort_type: Literal["popular","recent","price"] = "recent",
        is_brand_catalog: bool | None = None,
        page: int | list[int] | None = 0,
        page_size: int = 100,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.brand.catalog.extract import BrandProduct
    # from linkmerce.core.smartstore.brand.catalog.transform import ProductCatalog
    return run_with_duckdb(
        module = get_module(".catalog"),
        extractor = "BrandProduct",
        transformer = "ProductCatalog",
        connection = connection,
        tables = tables,
        how = how,
        return_type = return_type,
        args = (brand_ids, mall_seq, sort_type, is_brand_catalog, page, page_size),
        extract_options = update_options(
            extract_options,
            headers = dict(cookies=cookies),
            options = each_page_options(max_concurrent, request_delay, progress),
        ),
        transform_options = transform_options,
    )


def page_view(
        cookies: str,
        aggregate_by: Literal["device","product","url"],
        mall_seq: int | str | Iterable[int | str],
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        max_retries: int = 5,
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> dict[str,JsonObject]:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.brand.pageview.extract import PageViewByDevice, PageViewByUrl
    # from linkmerce.core.smartstore.brand.pageview.transform import PageViewByDevice, PageViewByProduct, PageViewByUrl
    return run_with_duckdb(
        module = get_module(".pageview"),
        extractor = "PageViewBy{}".format("Device" if aggregate_by == "device" else "Url"),
        transformer = "PageViewBy{}".format(aggregate_by.capitalize()),
        connection = connection,
        tables = tables,
        how = how,
        return_type = return_type,
        args = (mall_seq, start_date, end_date),
        extract_options = update_options(
            extract_options,
            headers = dict(cookies=cookies),
            options = each_request_loop_options(max_retries, max_concurrent, request_delay, progress),
        ),
        transform_options = transform_options,
    )


def store_sales(
        cookies: str,
        mall_seq: int | str | Iterable[int | str],
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["daily","weekly","monthly"] = "daily",
        page: int | Iterable[int] = 1,
        page_size: int = 1000,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.brand.sales.extract import StoreSales
    # from linkmerce.core.smartstore.brand.sales.transform import StoreSales
    return run_with_duckdb(
        module = get_module(".sales"),
        extractor = "StoreSales",
        transformer = "StoreSales",
        connection = connection,
        tables = tables,
        how = how,
        return_type = return_type,
        args = (mall_seq, start_date, end_date, date_type, page, page_size),
        extract_options = update_options(
            extract_options,
            headers = dict(cookies=cookies),
            options = each_request_options(max_concurrent, request_delay, progress),
        ),
        transform_options = transform_options,
    )


def category_sales(
        cookies: str,
        mall_seq: int | str | Iterable[int | str],
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["daily","weekly","monthly"] = "daily",
        page: int | Iterable[int] = 1,
        page_size: int = 1000,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.brand.sales.extract import CategorySales
    # from linkmerce.core.smartstore.brand.sales.transform import CategorySales
    return run_with_duckdb(
        module = get_module(".sales"),
        extractor = "CategorySales",
        transformer = "CategorySales",
        connection = connection,
        tables = tables,
        how = how,
        return_type = return_type,
        args = (mall_seq, start_date, end_date, date_type, page, page_size),
        extract_options = update_options(
            extract_options,
            headers = dict(cookies=cookies),
            options = each_request_options(max_concurrent, request_delay, progress),
        ),
        transform_options = transform_options,
    )


def product_sales(
        cookies: str,
        mall_seq: int | str | Iterable[int | str],
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["daily","weekly","monthly"] = "daily",
        page: int | Iterable[int] = 1,
        page_size: int = 1000,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.brand.sales.extract import ProductSales
    # from linkmerce.core.smartstore.brand.sales.transform import ProductSales
    return run_with_duckdb(
        module = get_module(".sales"),
        extractor = "ProductSales",
        transformer = "ProductSales",
        connection = connection,
        tables = tables,
        how = how,
        return_type = return_type,
        args = (mall_seq, start_date, end_date, date_type, page, page_size),
        extract_options = update_options(
            extract_options,
            headers = dict(cookies=cookies),
            options = each_request_options(max_concurrent, request_delay, progress),
        ),
        transform_options = transform_options,
    )


def aggregated_sales(
        cookies: str,
        mall_seq: int | str | Iterable[int | str],
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["daily","weekly","monthly"] = "daily",
        page: int | Iterable[int] = 1,
        page_size: int = 1000,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> dict[str,JsonObject]:
    """`tables = {'sales': 'naver_brand_sales', 'product': 'naver_brand_product'}`"""
    # from linkmerce.core.smartstore.brand.sales.extract import AggregatedSales
    # from linkmerce.core.smartstore.brand.sales.transform import AggregatedSales
    return run_with_duckdb(
        module = get_module(".sales"),
        extractor = "AggregatedSales",
        transformer = "AggregatedSales",
        connection = connection,
        tables = tables,
        how = how,
        return_type = return_type,
        args = (mall_seq, start_date, end_date, date_type, page, page_size),
        extract_options = update_options(
            extract_options,
            headers = dict(cookies=cookies),
            options = each_request_options(max_concurrent, request_delay, progress),
        ),
        transform_options = transform_options,
    )
