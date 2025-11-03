from __future__ import annotations

from linkmerce.common.api import run_with_duckdb

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


def get_module(name: str) -> str:
    return (".coupang.advertising" + name) if name.startswith('.') else name


def marketing_report(
        cookies: str,
        start_date: dt.date | str, 
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["total","daily"] = "daily",
        report_type: Literal["campaign","adGroup","vendorItem","keyword"] = "vendorItem",
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
    # from linkmerce.core.coupang.advertising.adreport.extract import MarketingReport
    # from linkmerce.core.coupang.advertising.adreport.transform import MarketingReport
    return run_with_duckdb(
        module = get_module(".adreport"),
        extractor = "MarketingReport",
        transformer = "MarketingReport",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (start_date, end_date, date_type, report_type, campaign_ids, vendor_id, wait_seconds, wait_interval),
        extract_options = dict(
            extract_options,
            headers = dict(cookies=cookies),
            variables = dict(domain=domain),
        ),
        transform_options = transform_options,
    )
