from __future__ import annotations

from linkmerce.common.api import run, run_with_duckdb
from linkmerce.common.api import get_table_from_options as get_table

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


def get_module(name: str) -> str:
    return (".searchad.api" + name) if name.startswith('.') else name


def campaign(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    table = get_table(transform_options, "table")
    extract_options = dict(extract_options or dict(), variables=dict(api_key=api_key, secret_key=secret_key, customer_id=customer_id))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".adreport"), "Campaign", "Campaign", connection, "sync", table, return_type, (from_date,), **options)


def adgroup(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    table = get_table(transform_options, "table")
    extract_options = dict(extract_options or dict(), variables=dict(api_key=api_key, secret_key=secret_key, customer_id=customer_id))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".adreport"), "Adgroup", "Adgroup", connection, "sync", table, return_type, (from_date,), **options)


def power_link_ad(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> Sequence:
    extract_options = dict(extract_options or dict(), variables=dict(api_key=api_key, secret_key=secret_key, customer_id=customer_id))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run(get_module(".adreport"), "PowerLinkAd", "PowerLinkAd", "sync", (from_date,), **options)


def power_contents_ad(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> Sequence:
    extract_options = dict(extract_options or dict(), variables=dict(api_key=api_key, secret_key=secret_key, customer_id=customer_id))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run(get_module(".adreport"), "PowerContentsAd", "PowerContentsAd", "sync", (from_date,), **options)


def shopping_product_ad(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> Sequence:
    extract_options = dict(extract_options or dict(), variables=dict(api_key=api_key, secret_key=secret_key, customer_id=customer_id))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run(get_module(".adreport"), "ShoppingProductAd", "ShoppingProductAd", "sync", (from_date,), **options)


def product_group(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> Sequence:
    extract_options = dict(extract_options or dict(), variables=dict(api_key=api_key, secret_key=secret_key, customer_id=customer_id))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run(get_module(".adreport"), "ProductGroup", "ProductGroup", "sync", (from_date,), **options)


def product_group_rel(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> Sequence:
    extract_options = dict(extract_options or dict(), variables=dict(api_key=api_key, secret_key=secret_key, customer_id=customer_id))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run(get_module(".adreport"), "ProductGroupRel", "ProductGroupRel", "sync", (from_date,), **options)


def brand_thumbnail_ad(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> Sequence:
    extract_options = dict(extract_options or dict(), variables=dict(api_key=api_key, secret_key=secret_key, customer_id=customer_id))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run(get_module(".adreport"), "BrandThumbnailAd", "BrandThumbnailAd", "sync", (from_date,), **options)


def brand_banner_ad(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> Sequence:
    extract_options = dict(extract_options or dict(), variables=dict(api_key=api_key, secret_key=secret_key, customer_id=customer_id))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run(get_module(".adreport"), "BrandBannerAd", "BrandBannerAd", "sync", (from_date,), **options)


def brand_ad(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> Sequence:
    extract_options = dict(extract_options or dict(), variables=dict(api_key=api_key, secret_key=secret_key, customer_id=customer_id))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run(get_module(".adreport"), "BrandAd", "BrandAd", "sync", (from_date,), **options)


def ad(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    table = get_table(transform_options, "table")
    extract_options = dict(extract_options or dict(), variables=dict(api_key=api_key, secret_key=secret_key, customer_id=customer_id))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".adreport"), "Ad", "Ad", connection, "sync", table, return_type, (from_date,), **options)
