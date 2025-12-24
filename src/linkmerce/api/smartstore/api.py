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


def get_order_options(request_delay: float | int = 1, progress: bool = True) -> dict:
    return dict(
        CursorAll = dict(request_delay=request_delay),
        RequestEachCursor = dict(tqdm_options=dict(disable=(not progress))),
    )


def request(
        client_id: str,
        client_secret: str,
        method: str,
        path: str,
        version: str | None = None,
        params: dict | list[tuple] | bytes | None = None,
        data: dict | list[tuple] | bytes | None = None,
        json: JsonObject | None = None,
        headers: dict[str,str] = None,
        extract_options: dict = dict(),
    ) -> JsonObject:
    from linkmerce.core.smartstore.api.common import SmartstoreTestAPI
    extractor = SmartstoreTestAPI(**update_options(
        extract_options,
        variables = dict(client_id=client_id, client_secret=client_secret),
    ))
    return extractor.extract(method, path, version, params, data, json, headers)


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
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        max_retries: int = 5,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.api.product.extract import Product
    # from linkmerce.core.smartstore.api.product.transform import Product
    return run_with_duckdb(
        module = get_module(".product"),
        extractor = "Product",
        transformer = "Product",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (search_keyword, keyword_type, status_type, period_type, from_date, to_date, channel_seq, max_retries),
        extract_options = update_options(
            extract_options,
            options = dict(PaginateAll = dict(request_delay=request_delay, tqdm_options=dict(disable=(not progress)))),
            variables = dict(client_id=client_id, client_secret=client_secret),
        ),
        transform_options = transform_options,
    )


def option(
        client_id: str,
        client_secret: str,
        product_id: Sequence[int | str],
        channel_seq: int | str | None = None,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        max_retries: int = 5,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.api.product.extract import Option
    # from linkmerce.core.smartstore.api.product.transform import Option
    return run_with_duckdb(
        module = get_module(".product"),
        extractor = "Option",
        transformer = "Option",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (product_id, channel_seq, max_retries),
        extract_options = update_options(
            extract_options,
            options = dict(RequestEach = dict(request_delay=request_delay, tqdm_options=dict(disable=(not progress)))),
            variables = dict(client_id=client_id, client_secret=client_secret),
        ),
        transform_options = transform_options,
    )


def product_option(
        client_id: str,
        client_secret: str,
        connection: DuckDBConnection,
        search_keyword: Sequence[int] = list(),
        keyword_type: Literal["CHANNEL_PRODUCT_NO","PRODUCT_NO","GROUP_PRODUCT_NO"] = "CHANNEL_PRODUCT_NO",
        status_type: Sequence[Literal["ALL","WAIT","SALE","OUTOFSTOCK","UNADMISSION","REJECTION","SUSPENSION","CLOSE","PROHIBITION"]] = ["SALE"],
        period_type: Literal["PROD_REG_DAY","SALE_START_DAY","SALE_END_DAY","PROD_MOD_DAY"] = "PROD_REG_DAY",
        from_date: dt.date | str | None = None,
        to_date: dt.date | str | None = None,
        channel_seq: int | str | None = None,
        tables: dict | None = None,
        max_retries: int = 5,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
        if_merged_table_exists: Literal["insert","replace"] = "replace",
    ) -> dict[str,JsonObject]:
    """```python
    tables = {
        'product': 'smartstore_product',
        'option': 'smartstore_option',
        'merged': 'data'
    }"""
    # from linkmerce.core.smartstore.api.product.extract import Product, Option
    # from linkmerce.core.smartstore.api.product.transform import Product, Option
    from copy import deepcopy
    results = dict()
    common = (max_retries, request_delay, progress, return_type)
    product_table = (tables or dict()).get("product", "smartstore_product")
    option_table = (tables or dict()).get("option", "smartstore_option")
    merged_table = (tables or dict()).get("merged", "data")

    args = (search_keyword, keyword_type, status_type, period_type, from_date, to_date, channel_seq)
    options = (deepcopy(extract_options), deepcopy(transform_options))
    results["product"] = product(client_id, client_secret, *args, connection, dict(default=product_table), *common, *options)

    where_clause = f" WHERE channel_seq = {channel_seq}" if channel_seq else str()
    query = "SELECT DISTINCT product_id FROM {}{};".format(product_table, where_clause)
    product_id = [row[0] for row in connection.execute(query).fetchall()]
    options = (deepcopy(extract_options), deepcopy(transform_options))
    results["option"] = option(client_id, client_secret, product_id, channel_seq, connection, dict(default=option_table), *common, *options)

    if return_type == "raw":
        return results

    columns = [
        "L.product_id"
        , "COALESCE(R.option_id, L.product_id) AS option_id"
        , "L.channel_seq"
        , ("(CASE WHEN R.product_type = 0 THEN '옵션상품(단독형)' "
            + "WHEN R.product_type = 1 THEN '옵션상품(조합형)' "
            + "WHEN R.product_type = 2 THEN '추가상품' "
            + "ELSE '단품상품' END) AS product_type")
        , "L.product_name"
        , "R.register_order"
        , *[("R.option_"+x+str(i)) for i in range(1,3+1) for x in ["group","name"]]
        , "L.management_code AS seller_product_code"
        , "R.management_code AS seller_option_code"
        , "L.model_name"
        , "L.brand_name"
        , "L.category_id"
        , ("(CASE WHEN L.status_type = 'WAIT' THEN '판매대기' "
            + "WHEN L.status_type = 'SALE' THEN '판매중' "
            + "WHEN L.status_type = 'OUTOFSTOCK' THEN '품절' "
            + "WHEN L.status_type = 'UNADMISSION' THEN '승인대기' "
            + "WHEN L.status_type = 'REJECTION' THEN '승인거부' "
            + "WHEN L.status_type = 'SUSPENSION' THEN '판매중지' "
            + "WHEN L.status_type = 'CLOSE' THEN '판매종료' "
            + "WHEN L.status_type = 'PROHIBITION' THEN '판매금지' "
            + "ELSE NULL END) AS product_status")
        , ("(CASE WHEN L.display_type = 'WAIT' THEN '전시대기' "
            + "WHEN L.display_type = 'ON' THEN '전시중' "
            + "WHEN L.display_type = 'SUSPENSION' THEN '전시중지' "
            + "ELSE NULL END) AS display_type")
        , "IF(R.usable, '사용', '사용안함') AS option_status"
        , "L.price"
        , "L.sales_price"
        , "L.delivery_fee"
        , "R.option_price"
        , "R.stock_quantity"
        , "L.register_dt"
        , "L.modify_dt"
    ]
    if if_merged_table_exists == "replace":
        keyword = f"CREATE OR REPLACE TABLE {merged_table} AS "
    elif connection.table_exists(merged_table):
        keyword = f"INSERT INTO {merged_table} "
    else:
        keyword = f"CREATE TABLE {merged_table} AS "
    connection.execute(
        keyword
        + f"SELECT {', '.join(columns)} "
        + f"FROM {product_table} AS L "
        + f"LEFT JOIN {option_table} AS R "
            + "ON L.product_id = R.product_id;")

    if return_type == "none":
        results["merged"] = None
    else:
        results["merged"] = connection.fetch_all(return_type, f"SELECT * FROM {merged_table}")

    return results


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
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        max_retries: int = 5,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """```python
    tables = {
        'order': 'smartstore_order',
        'product_order': 'smartstore_product_order',
        'delivery': 'smartstore_delivery',
        'option': 'smartstore_option'
    }"""
    # from linkmerce.core.smartstore.api.order.extract import Order
    # from linkmerce.core.smartstore.api.order.transform import Order
    return run_with_duckdb(
        module = get_module(".order"),
        extractor = "Order",
        transformer = "Order",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (start_date, end_date, range_type, product_order_status, claim_status, place_order_status, page_start, max_retries),
        extract_options = update_options(
            extract_options,
            options = get_order_options(request_delay, progress),
            variables = dict(client_id=client_id, client_secret=client_secret)),
        transform_options = transform_options,
    )


def order_status(
        client_id: str,
        client_secret: str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        last_changed_type: str | None = None,
        channel_seq: int | str | None = None,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        max_retries: int = 5,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.api.order.extract import OrderStatus
    # from linkmerce.core.smartstore.api.order.transform import OrderStatus
    return run_with_duckdb(
        module = get_module(".order"),
        extractor = "OrderStatus",
        transformer = "OrderStatus",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (start_date, end_date, last_changed_type, channel_seq, max_retries),
        extract_options = update_options(
            extract_options,
            options = get_order_options(request_delay, progress),
            variables = dict(client_id=client_id, client_secret=client_secret),
        ),
        transform_options = transform_options,
    )


def aggregated_order_status(
        client_id: str,
        client_secret: str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        channel_seq: int | str | None = None,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        max_retries: int = 5,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> dict[str,JsonObject]:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.api.order.extract import Order
    # from linkmerce.core.smartstore.api.order.transform import OrderTime
    common = dict(
        module = get_module(".order"),
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        kwargs = dict(channel_seq=channel_seq, max_retries=max_retries),
        extract_options = update_options(
            extract_options,
            options = get_order_options(request_delay, progress),
            variables = dict(client_id=client_id, client_secret=client_secret),
        ),
        transform_options = transform_options,
    )

    return dict(
        order_status = run_with_duckdb(
            **common,
            extractor = "OrderStatus",
            transformer = "OrderStatus",
            args = (start_date, end_date),
        ),
        purchase_decided = run_with_duckdb(
            **common,
            extractor = "Order",
            transformer = "OrderTime",
            args = (start_date, end_date, "PURCHASE_DECIDED_DATETIME"),
        ),
        claim_completed = run_with_duckdb(
            **common,
            extractor = "Order",
            transformer = "OrderTime",
            args = (start_date, end_date, "CLAIM_COMPLETED_DATETIME"),
        ),
    )


def marketing_channel(
        client_id: str,
        client_secret: str,
        channel_seq: int | str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        max_retries: int = 5,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.smartstore.api.bizdata.extract import MarketingChannel
    # from linkmerce.core.smartstore.api.bizdata.transform import MarketingChannel
    return run_with_duckdb(
        module = get_module(".bizdata"),
        extractor = "MarketingChannel",
        transformer = "MarketingChannel",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (channel_seq, start_date, end_date, max_retries),
        extract_options = update_options(
            extract_options,
            options = dict(RequestEach = dict(request_delay=request_delay, tqdm_options=dict(disable=(not progress)))),
            variables = dict(client_id=client_id, client_secret=client_secret),
        ),
        transform_options = transform_options,
    )
