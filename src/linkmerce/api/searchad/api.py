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
        tables: dict | None = None,
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
        args = (from_date,),
        extract_options = dict(
            extract_options,
            variables = get_variables(api_key, secret_key, customer_id),
        ),
        transform_options = transform_options,
    )


def campaign(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.searchad.api.adreport.extract import Campaign
    # from linkmerce.core.searchad.api.adreport.transform import Campaign
    return _master_report(
        api_key, secret_key, customer_id, "Campaign", from_date,
        connection, tables, return_type, extract_options, transform_options)


def adgroup(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.searchad.api.adreport.extract import Adgroup
    # from linkmerce.core.searchad.api.adreport.transform import Adgroup
    return _master_report(
        api_key, secret_key, customer_id, "Adgroup", from_date,
        connection, tables, return_type, extract_options, transform_options)


def ad(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.searchad.api.adreport.extract import Ad
    # from linkmerce.core.searchad.api.adreport.transform import Ad
    return _master_report(
        api_key, secret_key, customer_id, "Ad", from_date,
        connection, tables, return_type, extract_options, transform_options)


def contract(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> dict[str,JsonObject]:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.searchad.api.contract.extract import TimeContract, BrandNewContract
    # from linkmerce.core.searchad.api.contract.transform import TimeContract, BrandNewContract
    args = (api_key, secret_key, customer_id, connection, tables, return_type, extract_options, transform_options)
    return dict(
        TimeContract = time_contract(*args),
        BrandNewContract = brand_new_contract(*args),
    )


def time_contract(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.searchad.api.contract.extract import TimeContract
    # from linkmerce.core.searchad.api.contract.transform import TimeContract
    return run_with_duckdb(
        module = get_module(".contract"),
        extractor = "TimeContract",
        transformer = "TimeContract",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        extract_options = dict(
            extract_options,
            variables = get_variables(api_key, secret_key, customer_id),
        ),
        transform_options = transform_options,
    )


def brand_new_contract(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.searchad.api.contract.extract import BrandNewContract
    # from linkmerce.core.searchad.api.contract.transform import BrandNewContract
    return run_with_duckdb(
        module = get_module(".contract"),
        extractor = "BrandNewContract",
        transformer = "BrandNewContract",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        extract_options = dict(
            extract_options,
            variables = get_variables(api_key, secret_key, customer_id),
        ),
        transform_options = transform_options,
    )
