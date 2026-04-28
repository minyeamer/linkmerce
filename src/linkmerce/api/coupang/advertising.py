from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    from pathlib import Path
    import datetime as dt


def login(
        userid: str,
        passwd: str,
        domain: Literal["wing", "supplier"] = "wing",
        save_to: str | Path | None = None,
    ) -> str:
    """쿠팡 광고 로그인 후 쿠키를 반환한다."""
    from linkmerce.core.coupang.advertising.common import CoupangLogin
    from linkmerce.api.common import handle_cookies
    handler = CoupangLogin()
    handler.login(userid, passwd, domain)
    return handle_cookies(handler, save_to)


@with_duckdb_connection(tables={"campaign": "coupang_campaign", "adgroup": "coupang_adgroup"})
def campaign(
        cookies: str,
        goal_type: Literal["SALES", "NCA", "REACH"] = "SALES",
        is_deleted: bool = False,
        vendor_id: str | None = None,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """쿠팡 광고 캠페인 및 광고그룹 목록을 수집하고 각각의 테이블에 변환 및 적재한다.

    테이블 키 | 테이블명 | 설명
    - `campaign` | `coupang_campaign` | 쿠팡 광고 캠페인 목록
    - `adgroup` | `coupang_adgroup` | 쿠팡 광고그룹 목록"""
    from linkmerce.core.coupang.advertising.adreport.extract import Campaign
    from linkmerce.core.coupang.advertising.adreport.transform import Campaign as T
    return Campaign(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
        options = {
            "PaginateAll": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(goal_type, is_deleted, vendor_id)


@with_duckdb_connection(table="coupang_creative")
def creative(
        cookies: str,
        campaign_id: int | str | Sequence[int | str],
        vendor_id: str | None = None,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """쿠팡 광고 동영상 소재 목록을 수집하고 `coupang_creative` 테이블에 적재한다."""
    from linkmerce.core.coupang.advertising.adreport.extract import Creative
    from linkmerce.core.coupang.advertising.adreport.transform import Creative as T
    return Creative(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(campaign_id, vendor_id)


def adreport(
        cookies: str,
        start_date: dt.date | str, 
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        report_type: Literal["pa", "nca"] = "pa",
        date_type: Literal["total", "daily"] = "daily",
        report_level: Literal["campaign", "adGroup", "ad", "vendorItem", "keyword", "creative"] = "vendorItem",
        campaign_ids: Sequence[int | str] = list(),
        vendor_id: str | None = None,
        wait_seconds: int = 60,
        wait_interval: int = 1,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ):
    """`report_type`에 따라 PA(`coupang_adreport_pa`) 또는 NCA(`coupang_adreport_nca`) 성과 보고서를 다운로드한다."""
    args = (
        cookies, start_date, end_date, date_type, report_level,
        campaign_ids, vendor_id, wait_seconds, wait_interval)
    kwargs = {"connection": connection, "return_type": return_type, "extract_options": extract_options, "transform_options": transform_options}

    if report_type == "pa":
        return product_adreport(*args, **kwargs)
    elif report_type == "nca":
        return new_customer_adreport(*args, **kwargs)
    else:
        raise ValueError(f"Invalid report_type: '{report_type}'")


@with_duckdb_connection(table="coupang_adreport_pa")
def product_adreport(
        cookies: str,
        start_date: dt.date | str, 
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["total", "daily"] = "daily",
        report_level: Literal["campaign", "adGroup", "vendorItem", "keyword"] = "vendorItem",
        campaign_ids: Sequence[int | str] = list(),
        vendor_id: str | None = None,
        wait_seconds: int = 60,
        wait_interval: int = 1,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """쿠팡 PA(Product Ad) 광고 성과 보고서(Excel)를 다운로드하고 `coupang_adreport_pa` 테이블에 적재한다."""
    from linkmerce.core.coupang.advertising.adreport.extract import ProductAdReport
    from linkmerce.core.coupang.advertising.adreport.transform import ProductAdReport as T
    return ProductAdReport(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
    )).extract(start_date, end_date, date_type, report_level, campaign_ids, vendor_id, wait_seconds, wait_interval)


@with_duckdb_connection(table="coupang_adreport_nca")
def new_customer_adreport(
        cookies: str,
        start_date: dt.date | str, 
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["total", "daily"] = "daily",
        report_level: Literal["campaign", "ad", "keyword", "creative"] = "creative",
        campaign_ids: Sequence[int | str] = list(),
        vendor_id: str | None = None,
        wait_seconds: int = 60,
        wait_interval: int = 1,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """쿠팡 신규고객광고(NCA) 성과 보고서(Excel)를 다운로드하고 `coupang_adreport_nca` 테이블에 적재한다."""
    from linkmerce.core.coupang.advertising.adreport.extract import NewCustomerAdReport
    from linkmerce.core.coupang.advertising.adreport.transform import NewCustomerAdReport as T
    return NewCustomerAdReport(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
    )).extract(start_date, end_date, date_type, report_level, campaign_ids, vendor_id, wait_seconds, wait_interval)
