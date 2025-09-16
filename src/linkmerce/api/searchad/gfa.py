from __future__ import annotations

from linkmerce.common.api import run_with_duckdb, update_options

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


def get_module(name: str) -> str:
    return (".searchad.gfa" + name) if name.startswith('.') else name


def get_options(request_delay: float | int = 0.3, progress: bool = True) -> dict:
    return dict(
        PaginateAll = dict(request_delay=request_delay, tqdm_options=dict(disable=(not progress))),
        RequestEachPages = dict(request_delay=request_delay, tqdm_options=dict(disable=(not progress))),
    )


def logged_in(cookies: str, **kwargs) -> bool:
    from linkmerce.core.searchad.gfa.common import logged_in
    import requests
    with requests.Session() as session:
        return logged_in(session, cookies)


def whoami(cookies: str, **kwargs) -> str | None:
    from linkmerce.core.searchad.gfa.common import whoami
    import requests
    with requests.Session() as session:
        return whoami(session, cookies)


def _master_report(
        account_no: int | str,
        cookies: str,
        report_type: Literal["Campaign", "AdSet", "Creative"],
        status: Sequence[str],
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.searchad.api.adreport.extract import _MasterReport
    return run_with_duckdb(
        module = get_module(".adreport"),
        extractor = report_type,
        transformer = report_type,
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (status,),
        extract_options = update_options(
            extract_options,
            headers = dict(cookies=cookies),
            options = get_options(request_delay, progress),
            variables = dict(account_no=account_no),
        ),
        transform_options = transform_options,
    )


def campaign(
        account_no: int | str,
        cookies: str,
        status: Sequence[Literal["RUNNABLE","DELETED"]] = ["RUNNABLE","DELETED"],
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.searchad.gfa.adreport.extract import Campaign
    # from linkmerce.core.searchad.gfa.adreport.transform import Campaign
    return _master_report(
        account_no, cookies, "Campaign", status, connection, tables, request_delay, progress,
        return_type, extract_options, transform_options)


def adset(
        account_no: int | str,
        cookies: str,
        status: Sequence[Literal["ALL","RUNNABLE","BEFORE_STARTING","TERMINATED","DELETED"]] = ["ALL","DELETED"],
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.searchad.gfa.adreport.extract import AdSet
    # from linkmerce.core.searchad.gfa.adreport.transform import AdSet
    return _master_report(
        account_no, cookies, "AdSet", status, connection, tables, request_delay, progress,
        return_type, extract_options, transform_options)


def creative(
        account_no: int | str,
        cookies: str,
        status: Sequence[Literal["ALL","PENDING","REJECT","ACCEPT","PENDING_IN_OPERATION","REJECT_IN_OPERATION","DELETED"]] = ["ALL","DELETED"],
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.searchad.gfa.adreport.extract import Creative
    # from linkmerce.core.searchad.gfa.adreport.transform import Creative
    return _master_report(
        account_no, cookies, "Creative", status, connection, tables, request_delay, progress,
        return_type, extract_options, transform_options)


def performance_report(
        account_no: int | str,
        cookies: str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["TOTAL","DAY","WEEK","MONTH","HOUR"] = "DAY",
        columns: list[str] | Literal[":default:"] = ":default:",
        wait_seconds: int = 60,
        wait_interval: int = 1,
        progress: bool = True,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.searchad.gfa.adreport.extract import PerformanceReport
    # from linkmerce.core.searchad.gfa.adreport.transform import PerformanceReport
    return run_with_duckdb(
        module = get_module(".adreport"),
        extractor = "PerformanceReport",
        transformer = "PerformanceReport",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (start_date, end_date, date_type, columns, wait_seconds, wait_interval, progress),
        extract_options = dict(
            extract_options,
            headers = dict(cookies=cookies),
            variables = dict(account_no=account_no),
        ),
        transform_options = transform_options,
    )
