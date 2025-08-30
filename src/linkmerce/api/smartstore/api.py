from __future__ import annotations

from linkmerce.common.api import run_with_duckdb
from linkmerce.common.api import get_table_from_options as get_table

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


def get_module(name: str) -> str:
    return (".smartstore.api" + name) if name.startswith('.') else name


def order(
        client_id: str,
        client_secret: str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        range_type: Literal["PAYED_DATETIME","ORDERING_CONFIRM","DELIVERY_OPERATED","DELIVERY_COMPLETED","PURCHASE_DECISION_COMPLETED"] = "PAYED_DATETIME",
        product_order_status: Iterable[str] = list(),
        claim_status: Iterable[str] = list(),
        place_order_status: str = list(),
        page_start: int = 1,
        retry_count: int = 5,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    args = (start_date, end_date, range_type, product_order_status, claim_status, place_order_status, page_start, retry_count)
    table = get_table(transform_options, "table")
    extract_options = dict(extract_options or dict(), variables=dict(client_id=client_id, client_secret=client_secret))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".order"), "Order", "Order", connection, "sync", table, return_type, args, **options)


def product_order(
        client_id: str,
        client_secret: str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        range_type: Literal["PAYED_DATETIME","ORDERING_CONFIRM","DELIVERY_OPERATED","DELIVERY_COMPLETED","PURCHASE_DECISION_COMPLETED"] = "PAYED_DATETIME",
        product_order_status: Iterable[str] = list(),
        claim_status: Iterable[str] = list(),
        place_order_status: str = list(),
        page_start: int = 1,
        retry_count: int = 5,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    args = (start_date, end_date, range_type, product_order_status, claim_status, place_order_status, page_start, retry_count)
    table = [get_table(transform_options, "order_table"), get_table(transform_options, "option_table", "option")]
    extract_options = dict(extract_options or dict(), variables=dict(client_id=client_id, client_secret=client_secret))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".order"), "ProductOrder", "ProductOrder", connection, "sync", table, return_type, args, **options)


def order_status(
        client_id: str,
        client_secret: str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        last_changed_type: str | None = None,
        retry_count: int = 5,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    args = (start_date, end_date, last_changed_type, retry_count)
    table = get_table(transform_options, "table")
    extract_options = dict(extract_options or dict(), variables=dict(client_id=client_id, client_secret=client_secret))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(get_module(".order"), "OrderStatus", "OrderStatus", connection, "sync", table, return_type, args, **options)
