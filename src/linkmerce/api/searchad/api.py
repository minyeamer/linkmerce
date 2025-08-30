from __future__ import annotations

from linkmerce.common.api import run_with_duckdb

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


def get_module(name: str) -> str:
    return (".searchad.api" + name) if name.startswith('.') else name


def get_variables(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
    ) -> dict:
    return dict(api_key=api_key, secret_key=secret_key, customer_id=customer_id)


def _master_report(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        report_type: Literal["Campaign", "Adgroup", "Ad"],
        from_date: dt.date | str | None = None,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.searchad.api.adreport.extract import _MasterReport
    components = (get_module(".adreport"), report_type, report_type)
    extract_options = dict(extract_options, variables=get_variables(api_key, secret_key, customer_id))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, "sync", return_type, args=(from_date,), **options)


def campaign(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.searchad.api.adreport.extract import Campaign
    # from linkmerce.core.searchad.api.adreport.transform import Campaign
    return _master_report(
        api_key, secret_key, customer_id, "Campaign", from_date,
        connection, return_type, extract_options, transform_options)


def adgroup(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.searchad.api.adreport.extract import Adgroup
    # from linkmerce.core.searchad.api.adreport.transform import Adgroup
    return _master_report(
        api_key, secret_key, customer_id, "Adgroup", from_date,
        connection, return_type, extract_options, transform_options)


def ad(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.searchad.api.adreport.extract import Ad
    # from linkmerce.core.searchad.api.adreport.transform import Ad
    return _master_report(
        api_key, secret_key, customer_id, "Ad", from_date,
        connection, return_type, extract_options, transform_options)
