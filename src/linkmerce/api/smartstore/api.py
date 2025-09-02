from __future__ import annotations

from linkmerce.common.api import run_with_duckdb, update_options

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal, Sequence
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


def get_module(name: str) -> str:
    return (".smartstore.api" + name) if name.startswith('.') else name


def get_options(request_delay: float | int = 1) -> dict:
    return dict(CursorAll = dict(delay=request_delay))


def get_variables(
        client_id: str,
        client_secret: str,
    ) -> dict:
    return dict(client_id=client_id, client_secret=client_secret)


def product(
        client_id: str,
        client_secret: str,
        search_keyword: Sequence[int] = list(),
        keyword_type: Literal["CHANNEL_PRODUCT_NO","PRODUCT_NO","GROUP_PRODUCT_NO"] = "CHANNEL_PRODUCT_NO",
        status_type: Sequence[Literal["ALL","WAIT","SALE","OUTOFSTOCK","UNADMISSION","REJECTION","SUSPENSION","CLOSE","PROHIBITION"]] = ["SALE"],
        period_type: Literal["PROD_REG_DAY","SALE_START_DAY","SALE_END_DAY","PROD_MOD_DAY"] = "PROD_REG_DAY",
        from_date: dt.date | str | None = None,
        to_date: dt.date | str | None = None,
        channel_seq: int | str | None = None,
        retry_count: int = 5,
        request_delay: float | int = 1,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.api.product.extract import Product
    # from linkmerce.core.smartstore.api.product.transform import Product
    components = (get_module(".product"), "Product", "Product")
    args = (search_keyword, keyword_type, status_type, period_type, from_date, to_date, channel_seq, retry_count)
    extract_options = update_options(extract_options,
        options = get_options(request_delay),
        variables = get_variables(client_id, client_secret))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, "sync", return_type, args, **options)


def order(
        client_id: str,
        client_secret: str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        range_type: str = "PAYED_DATETIME",
        product_order_status: Iterable[str] = list(),
        claim_status: Iterable[str] = list(),
        place_order_status: str = list(),
        page_start: int = 1,
        retry_count: int = 5,
        request_delay: float | int = 1,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.api.order.extract import Order
    # from linkmerce.core.smartstore.api.order.transform import Order
    components = (get_module(".order"), "Order", "Order")
    args = (start_date, end_date, range_type, product_order_status, claim_status, place_order_status, page_start, retry_count)
    extract_options = update_options(extract_options,
        options = get_options(request_delay),
        variables = get_variables(client_id, client_secret))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, "sync", return_type, args, **options)


def product_order(
        client_id: str,
        client_secret: str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        range_type: str = "PAYED_DATETIME",
        product_order_status: Iterable[str] = list(),
        claim_status: Iterable[str] = list(),
        place_order_status: str = list(),
        page_start: int = 1,
        retry_count: int = 5,
        request_delay: float | int = 1,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> dict[str,JsonObject]:
    """`tables = {'order': 'smartstore_order', 'option': 'smartstore_option'}`"""
    # from linkmerce.core.smartstore.api.order.extract import ProductOrder
    # from linkmerce.core.smartstore.api.order.transform import ProductOrder
    components = (get_module(".order"), "ProductOrder", "ProductOrder")
    args = (start_date, end_date, range_type, product_order_status, claim_status, place_order_status, page_start, retry_count)
    extract_options = update_options(extract_options,
        options = get_options(request_delay),
        variables = get_variables(client_id, client_secret))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, "sync", return_type, args, **options)


def order_status(
        client_id: str,
        client_secret: str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        last_changed_type: str | None = None,
        retry_count: int = 5,
        request_delay: float | int = 1,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.api.order.extract import OrderStatus
    # from linkmerce.core.smartstore.api.order.transform import OrderStatus
    components = (get_module(".order"), "OrderStatus", "OrderStatus")
    args = (start_date, end_date, last_changed_type, retry_count)
    extract_options = update_options(extract_options,
        options = get_options(request_delay),
        variables = get_variables(client_id, client_secret))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, "sync", return_type, args, **options)


def aggregated_order_status(
        client_id: str,
        client_secret: str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        retry_count: int = 5,
        request_delay: float | int = 1,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> dict[str,JsonObject]:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.api.order.extract import OrderTime
    # from linkmerce.core.smartstore.api.order.transform import OrderTime
    components = (get_module(".order"), "OrderTime", "OrderTime")
    extract_options = update_options(extract_options,
        options = get_options(request_delay),
        variables = get_variables(client_id, client_secret))
    options = dict(kwargs=dict(retry_count=retry_count), extract_options=extract_options, transform_options=transform_options)
    return dict(
        order_status = run_with_duckdb(
            get_module(".order"), "OrderStatus", "OrderStatus", connection, "sync", return_type, (start_date, end_date), **options),
        purchase_decided = run_with_duckdb(
            *components, connection, "sync", return_type, (start_date, end_date, "PURCHASE_DECIDED_DATETIME"), **options),
        claim_completed = run_with_duckdb(
            *components, connection, "sync", return_type, (start_date, end_date, "CLAIM_COMPLETED_DATETIME"), **options),
    )
