from __future__ import annotations

from linkmerce.common.api import run_with_duckdb
from linkmerce.common.api import get_table_from_options as get_table

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection


def get_module(name: str) -> str:
    return (".searchad.manage" + name) if name.startswith('.') else name


def diagnose_exposure(
        customer_id: str,
        cookies: str,
        keyword: str | Iterable[str],
        domain: Literal["search","shopping"] = "search",
        mobile: bool = True,
        is_own: bool | None = None,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    args = (keyword, domain, mobile, is_own)
    table = get_table(transform_options, "table")
    extract_options = dict(extract_options or dict(), headers=dict(cookies=cookies), variables=dict(customer_id=customer_id))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".exposure"), "ExposureDiagnosis", "ExposureDiagnosis", connection, "sync", table, return_type, args, **options)


def rank_exposure(
        customer_id: str,
        cookies: str,
        keyword: str | Iterable[str],
        domain: Literal["search","shopping"] = "search",
        mobile: bool = True,
        is_own: bool | None = None,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    args = (keyword, domain, mobile, is_own)
    table = [get_table(transform_options, "rank_table"), get_table(transform_options, "product_table", "product")]
    extract_options = dict(extract_options or dict(), headers=dict(cookies=cookies), variables=dict(customer_id=customer_id))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".exposure"), "ExposureRank", "ExposureRank", connection, "sync", table, return_type, args, **options)
