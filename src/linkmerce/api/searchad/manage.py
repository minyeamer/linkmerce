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
        max_retries: int = 5,
        request_delay: float | int = 1.01,
        progress: bool = True,
    ) -> dict:
    return dict(
        RequestLoop = dict(max_retries=max_retries, raise_errors=RuntimeError, ignored_errors=Exception),
        RequestEachLoop = dict(request_delay=request_delay, tqdm_options=dict(disable=(not progress))),
    )


def has_cookies(cookies: str, **kwargs) -> bool:
    from linkmerce.core.searchad.manage.common import has_cookies
    import requests
    with requests.Session() as session:
        return has_cookies(session, cookies)


def has_permission(customer_id: int | str, cookies: str, **kwargs) -> bool:
    from linkmerce.core.searchad.manage.common import has_permission
    import requests
    with requests.Session() as session:
        return has_permission(session, customer_id, cookies)


def whoami(customer_id: int | str, cookies: str, **kwargs) -> dict | None:
    from linkmerce.core.searchad.manage.common import whoami
    import requests
    with requests.Session() as session:
        return whoami(session, customer_id, cookies)


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
    return run(
        module = get_module(".adreport"),
        extractor = "AdvancedReport",
        transformer = "AdvancedReport",
        how = "sync",
        args = (report_id, report_name, userid, attributes, fields, start_date, end_date),
        extract_options = dict(
            extract_options,
            headers = dict(cookies=cookies),
            variables = dict(customer_id=customer_id),
        ),
        transform_options = transform_options,
    )


def daily_report(
        customer_id: int | str,
        cookies: str,
        report_id: str,
        report_name: str,
        userid: str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.searchad.manage.adreport.extract import DailyReport
    # from linkmerce.core.searchad.manage.adreport.transform import DailyReport
    return run_with_duckdb(
        module = get_module(".adreport"),
        extractor = "DailyReport",
        transformer = "DailyReport",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (report_id, report_name, userid, start_date, end_date),
        extract_options = dict(
            extract_options,
            headers = dict(cookies=cookies),
            variables = dict(customer_id=customer_id),
        ),
        transform_options = transform_options,
    )


def diagnose_exposure(
        customer_id: int | str,
        cookies: str,
        keyword: str | Iterable[str],
        domain: Literal["search","shopping"] = "search",
        mobile: bool = True,
        is_own: bool | None = None,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        max_retries: int = 5,
        request_delay: float | int = 1.01,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.searchad.manage.exposure.extract import ExposureDiagnosis
    # from linkmerce.core.searchad.manage.exposure.transform import ExposureDiagnosis
    return run_with_duckdb(
        module = get_module(".exposure"),
        extractor = "ExposureDiagnosis",
        transformer = "ExposureDiagnosis",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (keyword, domain, mobile, is_own),
        extract_options = update_options(
            extract_options,
            headers = dict(cookies=cookies),
            options = get_options(max_retries, request_delay, progress),
            variables = dict(customer_id=customer_id),
        ),
        transform_options=transform_options, 
    )


def rank_exposure(
        customer_id: int | str,
        cookies: str,
        keyword: str | Iterable[str],
        domain: Literal["search","shopping"] = "search",
        mobile: bool = True,
        is_own: bool | None = None,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        max_retries: int = 5,
        request_delay: float | int = 1.01,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> dict[str,JsonObject]:
    """`tables = {'rank': 'naver_rank_ad', 'product': 'naver_product'}`"""
    # from linkmerce.core.searchad.manage.exposure.extract import ExposureDiagnosis
    # from linkmerce.core.searchad.manage.exposure.transform import ExposureRank
    return run_with_duckdb(
        module = get_module(".exposure"),
        extractor = "ExposureDiagnosis",
        transformer = "ExposureRank",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (keyword, domain, mobile, is_own),
        extract_options = update_options(
            extract_options,
            headers = dict(cookies=cookies),
            options = get_options(max_retries, request_delay, progress),
            variables = dict(customer_id=customer_id),
        ),
        transform_options=transform_options, 
    )
