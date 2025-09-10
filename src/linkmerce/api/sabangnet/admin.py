from __future__ import annotations

from linkmerce.common.api import run_with_duckdb, update_options

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


def get_module(name: str) -> str:
    return (".sabangnet.admin" + name) if name.startswith('.') else name


def get_options(request_delay: float | int = 1) -> dict:
    return dict(PaginateAll = dict(delay=request_delay), RequestEachPages = dict(delay=request_delay))


def order(
        userid: str,
        passwd: str,
        domain: int,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: str = "ord_dt",
        order_status_div: str = str(),
        order_status: Sequence[str] = list(),
        sort_type: str = "ord_no_asc",
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    # from linkmerce.core.sabangnet.admin.order.extract import Order
    # from linkmerce.core.sabangnet.admin.order.transform import Order
    components = (get_module(".order"), "Order", "Order")
    args = (start_date, end_date, date_type, order_status_div, order_status, sort_type)
    extract_options = update_options(extract_options,
        options = get_options(request_delay),
        variables = dict(userid=userid, passwd=passwd, domain=domain))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, "sync", return_type, args, **options)


def order_download(
        userid: str,
        passwd: str,
        domain: int,
        excel_form: int,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: str = "ord_dt",
        order_seq: list[int] = list(),
        order_status_div: str = str(),
        order_status: Sequence[str] = list(),
        sort_type: str = "ord_no_asc",
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> dict[str,bytes]:
    # from linkmerce.core.sabangnet.admin.order.extract import OrderDownload
    # from linkmerce.core.sabangnet.admin.order.transform import OrderDownload
    components = (get_module(".order"), "OrderDownload", "OrderDownload")
    args = (excel_form, start_date, end_date, date_type, order_seq, order_status_div, order_status, sort_type)
    extract_options = dict(extract_options, variables = dict(userid=userid, passwd=passwd, domain=domain))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, "sync", return_type, args, **options)


def order_status(
        userid: str,
        passwd: str,
        domain: int,
        excel_form: int,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: list[str] = ["delivery_confirm_date", "cancel_dt", "rtn_dt", "chng_dt"],
        order_seq: list[int] = list(),
        order_status_div: str = str(),
        order_status: Sequence[str] = list(),
        sort_type: str = "ord_no_asc",
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    # from linkmerce.core.sabangnet.admin.order.extract import OrderStatus
    # from linkmerce.core.sabangnet.admin.order.transform import OrderStatus
    components = (get_module(".order"), "OrderStatus", "OrderStatus")
    args = (excel_form, start_date, end_date, date_type, order_seq, order_status_div, order_status, sort_type)
    extract_options = update_options(extract_options,
        options = get_options(request_delay),
        variables = dict(userid=userid, passwd=passwd, domain=domain))
    options = dict(extract_options=extract_options, transform_options=transform_options)
    return run_with_duckdb(*components, connection, "sync", return_type, args, **options)
