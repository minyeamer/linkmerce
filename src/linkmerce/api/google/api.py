from __future__ import annotations

from linkmerce.common.api import run_with_duckdb, update_options

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    from pathlib import Path
    import datetime as dt


def get_module(name: str) -> str:
    return (".google.api" + name) if name.startswith('.') else name


def get_options(
        request_delay: float | int = 1,
        progress: bool = True,
    ) -> dict:
    return dict(
        RequestEach = dict(request_delay=request_delay, tqdm_options=dict(disable=(not progress))),
    )


def _ad_stream(
        object_type: Literal["Campaign","AdGroup","Ad","Asset","AssetView"],
        customer_id: int | str,
        manager_id: int | str,
        developer_token: str,
        service_account: str | Path | dict[str,str],
        start_date: dt.date | str | None = None,
        end_date: dt.date | str | None = None,
        date_range: Literal[
            "TODAY", "YESTERDAY", "LAST_7_DAYS", "LAST_14_DAYS", "LAST_30_DAYS", "LAST_BUSINESS_WEEK",
            "THIS_MONTH", "LAST_MONTH", "THIS_WEEK_SUN_TODAY", "THIS_WEEK_MON_TODAY",
            "LAST_WEEK_SUN_SAT", "LAST_WEEK_MON_SUN"] | None = None,
        fields: Sequence[str] = list(),
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.google.api.ads.extract import GoogleAds
    # from linkmerce.core.google.api.ads.transform import _AdTransformer
    return run_with_duckdb(
        module = get_module(".ads"),
        extractor = object_type,
        transformer = object_type,
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (start_date, end_date, date_range, fields),
        extract_options = update_options(
            extract_options,
            variables = dict(
                customer_id = customer_id,
                manager_id = manager_id,
                developer_token = developer_token,
                service_account = service_account,
            ),
        ),
        transform_options = transform_options,
    )


def campaign(
        customer_id: int | str,
        manager_id: int | str,
        developer_token: str,
        service_account: str | Path | dict[str,str],
        start_date: dt.date | str | None = None,
        end_date: dt.date | str | None = None,
        date_range: Literal[
            "TODAY", "YESTERDAY", "LAST_7_DAYS", "LAST_14_DAYS", "LAST_30_DAYS", "LAST_BUSINESS_WEEK",
            "THIS_MONTH", "LAST_MONTH", "THIS_WEEK_SUN_TODAY", "THIS_WEEK_MON_TODAY",
            "LAST_WEEK_SUN_SAT", "LAST_WEEK_MON_SUN"] | None = "LAST_30_DAYS",
        fields: Sequence[str] = list(),
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.google.api.ads.extract import Campaign
    # from linkmerce.core.google.api.ads.transform import Campaign
    return _ad_stream(
        "Campaign", customer_id, manager_id, developer_token, service_account, start_date, end_date,
        date_range, fields, connection, tables, return_type, extract_options, transform_options)


def adgroup(
        customer_id: int | str,
        manager_id: int | str,
        developer_token: str,
        service_account: str | Path | dict[str,str],
        start_date: dt.date | str | None = None,
        end_date: dt.date | str | None = None,
        date_range: Literal[
            "TODAY", "YESTERDAY", "LAST_7_DAYS", "LAST_14_DAYS", "LAST_30_DAYS", "LAST_BUSINESS_WEEK",
            "THIS_MONTH", "LAST_MONTH", "THIS_WEEK_SUN_TODAY", "THIS_WEEK_MON_TODAY",
            "LAST_WEEK_SUN_SAT", "LAST_WEEK_MON_SUN"] | None = "LAST_30_DAYS",
        fields: Sequence[str] = list(),
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.google.api.ads.extract import AdGroup
    # from linkmerce.core.google.api.ads.transform import AdGroup
    return _ad_stream(
        "AdGroup", customer_id, manager_id, developer_token, service_account, start_date, end_date,
        date_range, fields, connection, tables, return_type, extract_options, transform_options)


def ad(
        customer_id: int | str,
        manager_id: int | str,
        developer_token: str,
        service_account: str | Path | dict[str,str],
        start_date: dt.date | str | None = None,
        end_date: dt.date | str | None = None,
        date_range: Literal[
            "TODAY", "YESTERDAY", "LAST_7_DAYS", "LAST_14_DAYS", "LAST_30_DAYS", "LAST_BUSINESS_WEEK",
            "THIS_MONTH", "LAST_MONTH", "THIS_WEEK_SUN_TODAY", "THIS_WEEK_MON_TODAY",
            "LAST_WEEK_SUN_SAT", "LAST_WEEK_MON_SUN"] | None = "LAST_30_DAYS",
        fields: Sequence[str] = list(),
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.google.api.ads.extract import Ad
    # from linkmerce.core.google.api.ads.transform import Ad
    return _ad_stream(
        "Ad", customer_id, manager_id, developer_token, service_account, start_date, end_date,
        date_range, fields, connection, tables, return_type, extract_options, transform_options)


def insight(
        customer_id: int | str,
        manager_id: int | str,
        developer_token: str,
        service_account: str | Path | dict[str,str],
        start_date: dt.date | str | None = None,
        end_date: dt.date | str | None = None,
        date_freq: Literal['D', 'W', 'M'] = 'D',
        date_range: Literal[
            "TODAY", "YESTERDAY", "LAST_7_DAYS", "LAST_14_DAYS", "LAST_30_DAYS", "LAST_BUSINESS_WEEK",
            "THIS_MONTH", "LAST_MONTH", "THIS_WEEK_SUN_TODAY", "THIS_WEEK_MON_TODAY",
            "LAST_WEEK_SUN_SAT", "LAST_WEEK_MON_SUN"] | None = "YESTERDAY",
        fields: Sequence[str] = list(),
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.google.api.ads.extract import Insight
    # from linkmerce.core.google.api.ads.transform import Insight
    return run_with_duckdb(
        module = get_module(".ads"),
        extractor = "Insight",
        transformer = "Insight",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (start_date, end_date, date_freq, date_range, fields),
        extract_options = update_options(
            extract_options,
            variables = dict(
                customer_id = customer_id,
                manager_id = manager_id,
                developer_token = developer_token,
                service_account = service_account,
            ),
            options = get_options(request_delay, progress),
        ),
        transform_options = transform_options,
    )


def asset(
        customer_id: int | str,
        manager_id: int | str,
        developer_token: str,
        service_account: str | Path | dict[str,str],
        fields: Sequence[str] = list(),
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.google.api.ads.extract import Asset
    # from linkmerce.core.google.api.ads.transform import Asset
    return run_with_duckdb(
        module = get_module(".ads"),
        extractor = "Asset",
        transformer = "Asset",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (fields,),
        extract_options = update_options(
            extract_options,
            variables = dict(
                customer_id = customer_id,
                manager_id = manager_id,
                developer_token = developer_token,
                service_account = service_account,
            ),
        ),
        transform_options = transform_options,
    )


def asset_view(
        customer_id: int | str,
        manager_id: int | str,
        developer_token: str,
        service_account: str | Path | dict[str,str],
        start_date: dt.date | str | None = None,
        end_date: dt.date | str | None = None,
        date_freq: Literal['D', 'W', 'M'] = 'D',
        date_range: Literal[
            "TODAY", "YESTERDAY", "LAST_7_DAYS", "LAST_14_DAYS", "LAST_30_DAYS", "LAST_BUSINESS_WEEK",
            "THIS_MONTH", "LAST_MONTH", "THIS_WEEK_SUN_TODAY", "THIS_WEEK_MON_TODAY",
            "LAST_WEEK_SUN_SAT", "LAST_WEEK_MON_SUN"] | None = "YESTERDAY",
        fields: Sequence[str] = list(),
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.google.api.ads.extract import AssetView
    # from linkmerce.core.google.api.ads.transform import AssetView
    return run_with_duckdb(
        module = get_module(".ads"),
        extractor = "AssetView",
        transformer = "AssetView",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (start_date, end_date, date_freq, date_range, fields),
        extract_options = update_options(
            extract_options,
            variables = dict(
                customer_id = customer_id,
                manager_id = manager_id,
                developer_token = developer_token,
                service_account = service_account,
            ),
            options = get_options(request_delay, progress),
        ),
        transform_options = transform_options,
    )
