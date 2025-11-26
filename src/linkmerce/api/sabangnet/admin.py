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


def get_paginate_options(request_delay: float | int = 1, progress: bool = True) -> dict:
    return dict(
        PaginateAll = dict(request_delay=request_delay, tqdm_options=dict(disable=(not progress))),
    )


def get_request_options(request_delay: float | int = 1, progress: bool = True) -> dict:
    return dict(
        RequestEach = dict(request_delay=request_delay, tqdm_options=dict(disable=(not progress))),
    )


def login(userid: str, passwd: str) -> dict[str,str]:
    from linkmerce.core.sabangnet.admin.common import SabangnetLogin
    auth = SabangnetLogin()
    return auth.login(userid, passwd)


def order(
        userid: str,
        passwd: str,
        domain: int,
        start_date: dt.datetime | dt.date | str | Literal[":today:"] = ":today:",
        end_date: dt.datetime | dt.date | str | Literal[":start_date:",":now:"] = ":start_date:",
        date_type: str = "reg_dm",
        order_status_div: str = str(),
        order_status: Sequence[str] = list(),
        shop_id: str = str(),
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
        args = (start_date, end_date, date_type, order_status_div, order_status, shop_id, sort_type),
        extract_options = update_options(
            extract_options,
            options = get_paginate_options(request_delay, progress),
            variables = dict(userid=userid, passwd=passwd, domain=domain)),
        transform_options = transform_options,
    )


def order_download(
        userid: str,
        passwd: str,
        domain: int,
        download_no: int,
        download_type: Literal["order","option","invoice","dispatch"],
        start_date: dt.datetime | dt.date | str | Literal[":today:"] = ":today:",
        end_date: dt.datetime | dt.date | str | Literal[":start_date:",":now:"] = ":start_date:",
        date_type: str = "reg_dm",
        order_seq: list[int] = list(),
        order_status_div: str = str(),
        order_status: Sequence[str] = list(),
        shop_id: str = str(),
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
        args = (download_no, start_date, end_date, date_type, order_seq, order_status_div, order_status, shop_id, sort_type),
        extract_options = dict(
            extract_options,
            variables = dict(userid=userid, passwd=passwd, domain=domain),
        ),
        transform_options = dict(transform_options, download_type=download_type),
    )


def order_status(
        userid: str,
        passwd: str,
        domain: int,
        excel_form: int,
        start_date: dt.datetime | dt.date | str | Literal[":today:"] = ":today:",
        end_date: dt.datetime | dt.date | str | Literal[":start_date:",":now:"] = ":start_date:",
        date_type: list[str] = ["delivery_confirm_date", "cancel_dt", "rtn_dt", "chng_dt"],
        order_seq: list[int] = list(),
        order_status_div: str = str(),
        order_status: Sequence[str] = list(),
        shop_id: str = str(),
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
        args = (excel_form, start_date, end_date, date_type, order_seq, order_status_div, order_status, shop_id, sort_type),
        extract_options = update_options(
            extract_options,
            options = get_paginate_options(request_delay, progress),
            variables = dict(userid=userid, passwd=passwd, domain=domain)),
        transform_options = transform_options,
    )


def product_mapping(
        userid: str,
        passwd: str,
        domain: int,
        start_date: dt.date | str | Literal[":base_date:",":today:"] = ":base_date:",
        end_date: dt.date | str | Literal[":start_date:",":today:"] = ":today:",
        shop_id: str = str(),
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.sabangnet.admin.order.extract import ProductMapping
    # from linkmerce.core.sabangnet.admin.order.transform import ProductMapping
    return run_with_duckdb(
        module = get_module(".order"),
        extractor = "ProductMapping",
        transformer = "ProductMapping",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (start_date, end_date, shop_id),
        extract_options = update_options(
            extract_options,
            options = get_paginate_options(request_delay, progress),
            variables = dict(userid=userid, passwd=passwd, domain=domain)),
        transform_options = transform_options,
    )


def sku_mapping(
        userid: str,
        passwd: str,
        domain: int,
        query: Sequence[dict],
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """```python
    query = [{
        'product_id_shop': str,
        'shop_id': str,
        'product_id': str
    }]
    tables = {'default': 'data'}"""
    # from linkmerce.core.sabangnet.admin.order.extract import SkuMapping
    # from linkmerce.core.sabangnet.admin.order.transform import SkuMapping
    return run_with_duckdb(
        module = get_module(".order"),
        extractor = "SkuMapping",
        transformer = "SkuMapping",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (query,),
        extract_options = update_options(
            extract_options,
            options = get_request_options(request_delay, progress),
            variables = dict(userid=userid, passwd=passwd, domain=domain)),
        transform_options = transform_options,
    )


def option_mapping(
        userid: str,
        passwd: str,
        domain: int,
        connection: DuckDBConnection,
        start_date: dt.date | str | Literal[":base_date:",":today:"] = ":base_date:",
        end_date: dt.date | str | Literal[":start_date:",":today:"] = ":today:",
        shop_id: str = str(),
        tables: dict | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'product': 'product_mapping', 'sku': 'sku_mapping'}`"""
    # from linkmerce.core.sabangnet.admin.order.extract import ProductMapping, SkuMapping
    # from linkmerce.core.sabangnet.admin.order.transform import ProductMapping, SkuMapping
    from copy import deepcopy
    results = dict()
    common = (request_delay, progress, return_type)
    product_table = (tables or dict()).get("product", "product_mapping")
    sku_table = (tables or dict()).get("sku", "sku_mapping")

    args = (start_date, end_date, shop_id)
    options = (deepcopy(extract_options), deepcopy(transform_options))
    results["product"] = product_mapping(userid, passwd, domain, *args, connection, dict(default=product_table), *common, *options)

    query = "SELECT DISTINCT product_id_shop, shop_id, product_id FROM {} WHERE mapping_count > 0;".format(product_table)
    args = (connection.fetch_all_to_json(query),)
    options = (deepcopy(extract_options), deepcopy(transform_options))
    results["sku"] = sku_mapping(userid, passwd, domain, *args, connection, dict(default=sku_table), *common, *options)

    return results


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
            options = get_paginate_options(request_delay, progress),
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
            options = get_request_options(request_delay, progress),
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


def add_product(
        userid: str,
        passwd: str,
        domain: int,
        group_id: Sequence[str] = list(),
        start_date: dt.date | str | Literal[":base_date:",":today:"] = ":base_date:",
        end_date: dt.date | str | Literal[":start_date:",":today:"] = ":today:",
        shop_id: str = str(),
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.sabangnet.admin.product.extract import AddProductGroup, AddProduct
    # from linkmerce.core.sabangnet.admin.product.transform import AddProductGroup, AddProduct
    from copy import deepcopy

    if not group_id:
        groups = run_with_duckdb(
            module = get_module(".product"),
            extractor = "AddProductGroup",
            transformer = "AddProductGroup",
            connection = connection,
            tables = {"default": "temp_product"},
            how = "sync",
            return_type = "json",
            args = (start_date, end_date, shop_id),
            extract_options = update_options(
                deepcopy(extract_options),
                options = get_paginate_options(request_delay, progress),
                variables = dict(userid=userid, passwd=passwd, domain=domain)),
            transform_options = deepcopy(transform_options),
        )
        if connection is not None:
            connection.execute("DROP TABLE temp_product;")
        group_id = [group["group_id"] for group in groups]

    return run_with_duckdb(
        module = get_module(".product"),
        extractor = "AddProduct",
        transformer = "AddProduct",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (group_id,),
        extract_options = update_options(
            deepcopy(extract_options),
            options = get_request_options(request_delay, progress),
            variables = dict(userid=userid, passwd=passwd, domain=domain)),
        transform_options = deepcopy(transform_options),
    )
