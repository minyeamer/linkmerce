from __future__ import annotations

from linkmerce.common.api import run, run_with_duckdb, update_options

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal, Sequence
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


def get_module(name: str) -> str:
    return (".searchad.manage" + name) if name.startswith('.') else name


def get_options(
        retry_count: int = 5,
        request_delay: float | int = 1.01,
    ) -> dict:
    return dict(
        RequestLoop = dict(count=retry_count, ignored_errors=ConnectionError),
        RequestEachLoop = dict(delay=request_delay))


def validate_cookies(cookies: str) -> bool:
    from linkmerce.utils.headers import build_headers
    import requests
    url = "https://gw.searchad.naver.com/auth/local/naver-cookie/exist"
    origin = "https://searchad.naver.com"
    referer = f"{origin}/membership/select-account?redirectUrl=https:%2F%2Fmanage.searchad.naver.com"
    headers = build_headers(cookies=cookies, referer=referer, origin=origin)
    with requests.get(url, headers=headers) as response:
        return (response.text == "true")


def adreport(
        customer_id: int | str,
        cookies: str,
        report_id: str,
        report_name: str,
        userid: str,
        attributes: list[str],
        fields: list[str],
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> Sequence:
    # from linkmerce.core.searchad.manage.adreport.extract import AdvancedReport
    # from linkmerce.core.searchad.manage.adreport.transform import AdvancedReport
    components = (get_module(".adreport"), "AdvancedReport", "AdvancedReport")
    args = (report_id, report_name, userid, attributes, fields, start_date, end_date)
    extract_options = dict(extract_options, headers = dict(cookies=cookies), variables = dict(customer_id=customer_id))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run(*components, "sync", args, **options)


def daily_report(
        customer_id: int | str,
        cookies: str,
        report_id: str,
        report_name: str,
        userid: str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.searchad.manage.adreport.extract import DailyReport
    # from linkmerce.core.searchad.manage.adreport.transform import DailyReport
    components = (get_module(".adreport"), "DailyReport", "DailyReport")
    args = (report_id, report_name, userid, start_date, end_date)
    extract_options = dict(extract_options, headers=dict(cookies=cookies), variables=dict(customer_id=customer_id))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, "sync", return_type, args, **options)


def diagnose_exposure(
        customer_id: int | str,
        cookies: str,
        keyword: str | Iterable[str],
        domain: Literal["search","shopping"] = "search",
        mobile: bool = True,
        is_own: bool | None = None,
        connection: DuckDBConnection | None = None,
        retry_count: int = 5,
        request_delay: float | int = 1.01,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.searchad.manage.exposure.extract import ExposureDiagnosis
    # from linkmerce.core.searchad.manage.exposure.transform import ExposureDiagnosis
    components = (get_module(".exposure"), "ExposureDiagnosis", "ExposureDiagnosis")
    extract_options = update_options(extract_options,
        headers = dict(cookies=cookies),
        options = get_options(retry_count, request_delay),
        variables = dict(customer_id=customer_id))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, "sync", return_type, args=(keyword, domain, mobile, is_own), **options)


def rank_exposure(
        customer_id: int | str,
        cookies: str,
        keyword: str | Iterable[str],
        domain: Literal["search","shopping"] = "search",
        mobile: bool = True,
        is_own: bool | None = None,
        connection: DuckDBConnection | None = None,
        retry_count: int = 5,
        request_delay: float | int = 1.01,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> dict[str,JsonObject]:
    """`tables = {'rank': 'naver_rank_ad', 'product': 'naver_product'}`"""
    # from linkmerce.core.searchad.manage.exposure.extract import ExposureRank
    # from linkmerce.core.searchad.manage.exposure.transform import ExposureRank
    components = (get_module(".exposure"), "ExposureRank", "ExposureRank")
    extract_options = update_options(extract_options,
        headers = dict(cookies=cookies),
        options = get_options(retry_count, request_delay),
        variables = dict(customer_id=customer_id))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, "sync", return_type, args=(keyword, domain, mobile, is_own), **options)
