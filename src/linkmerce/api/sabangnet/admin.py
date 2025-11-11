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


def get_options(request_delay: float | int = 1, progress: bool = True) -> dict:
    return dict(
        PaginateAll = dict(request_delay=request_delay, tqdm_options=dict(disable=(not progress))),
    )


def login(userid: str, passwd: str) -> dict[str,str]:
    from linkmerce.core.sabangnet.admin.common import SabangnetLogin
    auth = SabangnetLogin()
    return auth.login(userid, passwd)


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
        tables: dict | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.sabangnet.admin.order.extract import Order
    # from linkmerce.core.sabangnet.admin.order.transform import Order
    return run_with_duckdb(
        module = get_module(".order"),
        extractor = "Order",
        transformer = "Order",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (start_date, end_date, date_type, order_status_div, order_status, sort_type),
        extract_options = update_options(
            extract_options,
            options = get_options(request_delay, progress),
            variables = dict(userid=userid, passwd=passwd, domain=domain)),
        transform_options = transform_options,
    )


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
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> dict[str,bytes]:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.sabangnet.admin.order.extract import OrderDownload
    # from linkmerce.core.sabangnet.admin.order.transform import OrderDownload
    return run_with_duckdb(
        module = get_module(".order"),
        extractor = "OrderDownload",
        transformer = "OrderDownload",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type =  return_type,
        args = (excel_form, start_date, end_date, date_type, order_seq, order_status_div, order_status, sort_type),
        extract_options = dict(
            extract_options,
            variables = dict(userid=userid, passwd=passwd, domain=domain),
        ),
        transform_options = transform_options,
    )


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
        tables: dict | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.sabangnet.admin.order.extract import OrderStatus
    # from linkmerce.core.sabangnet.admin.order.transform import OrderStatus
    return run_with_duckdb(
        module = get_module(".order"),
        extractor = "OrderStatus",
        transformer = "OrderStatus",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type =  return_type,
        args = (excel_form, start_date, end_date, date_type, order_seq, order_status_div, order_status, sort_type),
        extract_options = update_options(
            extract_options,
            options = get_options(request_delay, progress),
            variables = dict(userid=userid, passwd=passwd, domain=domain)),
        transform_options = transform_options,
    )


def product(
        userid: str,
        passwd: str,
        domain: int,
        start_date: dt.date | str | Literal[":base_date:",":today:"] = ":base_date:",
        end_date: dt.date | str | Literal[":start_date:",":today:"] = ":today:",
        date_type: str = "001",
        sort_type: str = "001",
        sort_asc: bool = True,
        is_deleted: bool = False,
        product_status: str | None = None,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.sabangnet.admin.product.extract import Product
    # from linkmerce.core.sabangnet.admin.product.transform import Product
    return run_with_duckdb(
        module = get_module(".product"),
        extractor = "Product",
        transformer = "Product",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (start_date, end_date, date_type, sort_type, sort_asc, is_deleted, product_status),
        extract_options = update_options(
            extract_options,
            options = get_options(request_delay, progress),
            variables = dict(userid=userid, passwd=passwd, domain=domain)),
        transform_options = transform_options,
    )


def option(
        userid: str,
        passwd: str,
        domain: int,
        product_id: Sequence[str],
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.sabangnet.admin.product.extract import Option
    # from linkmerce.core.sabangnet.admin.product.transform import Option
    return run_with_duckdb(
        module = get_module(".product"),
        extractor = "Option",
        transformer = "Option",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (product_id,),
        extract_options = update_options(
            extract_options,
            options = get_options(request_delay, progress),
            variables = dict(userid=userid, passwd=passwd, domain=domain)),
        transform_options = transform_options,
    )


def option_download(
        userid: str,
        passwd: str,
        domain: int,
        start_date: dt.date | str | Literal[":base_date:",":today:"] = ":base_date:",
        end_date: dt.date | str | Literal[":start_date:",":today:"] = ":today:",
        date_type: str = "prdFstRegsDt",
        sort_type: str = "prdNo",
        sort_asc: bool = True,
        is_deleted: bool = False,
        product_status: list[str] = list(),
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.sabangnet.admin.product.extract import OptionDownload
    # from linkmerce.core.sabangnet.admin.product.transform import OptionDownload
    return run_with_duckdb(
        module = get_module(".product"),
        extractor = "OptionDownload",
        transformer = "OptionDownload",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (start_date, end_date, date_type, sort_type, sort_asc, is_deleted, product_status),
        extract_options = update_options(
            extract_options,
            variables = dict(userid=userid, passwd=passwd, domain=domain)),
        transform_options = transform_options,
    )
