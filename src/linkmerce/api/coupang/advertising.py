from __future__ import annotations

from linkmerce.common.api import run_with_duckdb, update_options

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


def get_module(name: str) -> str:
    return (".coupang.advertising" + name) if name.startswith('.') else name


def campaign(
        cookies: str,
        goal_type: Literal["SALES","NCA","REACH"] = "SALES",
        is_deleted: bool = False,
        vendor_id: str | None = None,
        domain: Literal["advertising","domain","wing"] = "advertising",
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'campagin': 'coupang_campaign', 'adgroup': 'coupang_adgroup'}`"""
    # from linkmerce.core.coupang.advertising.adreport.extract import Campaign
    # from linkmerce.core.coupang.advertising.adreport.transform import Campaign
    return run_with_duckdb(
        module = get_module(".adreport"),
        extractor = "Campaign",
        transformer = "Campaign",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (goal_type, is_deleted, vendor_id),
        extract_options = update_options(
            extract_options,
            headers = dict(cookies=cookies),
            variables = dict(domain=domain),
            options = dict(PaginateAll = dict(request_delay=request_delay, tqdm_options=dict(disable=(not progress)))),
        ),
        transform_options = transform_options,
    )


def creative(
        cookies: str,
        campaign_ids: Sequence[int | str],
        vendor_id: str | None = None,
        domain: Literal["advertising","domain","wing"] = "advertising",
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.coupang.advertising.adreport.extract import Creative
    # from linkmerce.core.coupang.advertising.adreport.transform import Creative
    return run_with_duckdb(
        module = get_module(".adreport"),
        extractor = "Creative",
        transformer = "Creative",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (campaign_ids, vendor_id),
        extract_options = update_options(
            extract_options,
            headers = dict(cookies=cookies),
            variables = dict(domain=domain),
            options = dict(RequestEach = dict(request_delay=request_delay, tqdm_options=dict(disable=(not progress)))),
        ),
        transform_options = transform_options,
    )


def adreport(
        cookies: str,
        start_date: dt.date | str, 
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        report_type: Literal["pa","nca"] = "pa",
        date_type: Literal["total","daily"] = "daily",
        report_level: Literal["campaign","adGroup","ad","vendorItem","keyword","creative"] = "vendorItem",
        campaign_ids: Sequence[int | str] = list(),
        vendor_id: str | None = None,
        wait_seconds: int = 60,
        wait_interval: int = 1,
        domain: Literal["advertising","domain","wing"] = "advertising",
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ):
    """`tables = {'default': 'data'}`"""
    args = (
        cookies, start_date, end_date, date_type, report_level, campaign_ids, vendor_id,
        wait_seconds, wait_interval, domain, connection, tables, return_type, extract_options, transform_options)
    if report_type == "pa":
        return product_adreport(*args)
    elif report_type == "nca":
        return new_customer_adreport(*args)
    else:
        raise ValueError(f"Invalid report_type: '{report_type}'")


def product_adreport(
        cookies: str,
        start_date: dt.date | str, 
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["total","daily"] = "daily",
        report_level: Literal["campaign","adGroup","vendorItem","keyword"] = "vendorItem",
        campaign_ids: Sequence[int | str] = list(),
        vendor_id: str | None = None,
        wait_seconds: int = 60,
        wait_interval: int = 1,
        domain: Literal["advertising","domain","wing"] = "advertising",
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.coupang.advertising.adreport.extract import ProductAdReport
    # from linkmerce.core.coupang.advertising.adreport.transform import ProductAdReport
    return run_with_duckdb(
        module = get_module(".adreport"),
        extractor = "ProductAdReport",
        transformer = "ProductAdReport",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (start_date, end_date, date_type, report_level, campaign_ids, vendor_id, wait_seconds, wait_interval),
        extract_options = update_options(
            extract_options,
            headers = dict(cookies=cookies),
            variables = dict(domain=domain),
        ),
        transform_options = transform_options,
    )


def new_customer_adreport(
        cookies: str,
        start_date: dt.date | str, 
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["total","daily"] = "daily",
        report_level: Literal["campaign","ad","keyword","creative"] = "creative",
        campaign_ids: Sequence[int | str] = list(),
        vendor_id: str | None = None,
        wait_seconds: int = 60,
        wait_interval: int = 1,
        domain: Literal["advertising","domain","wing"] = "advertising",
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.coupang.advertising.adreport.extract import NewCustomerAdReport
    # from linkmerce.core.coupang.advertising.adreport.transform import NewCustomerAdReport
    return run_with_duckdb(
        module = get_module(".adreport"),
        extractor = "NewCustomerAdReport",
        transformer = "NewCustomerAdReport",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (start_date, end_date, date_type, report_level, campaign_ids, vendor_id, wait_seconds, wait_interval),
        extract_options = update_options(
            extract_options,
            headers = dict(cookies=cookies),
            variables = dict(domain=domain),
        ),
        transform_options = transform_options,
    )
