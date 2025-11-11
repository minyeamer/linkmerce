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
        args = (start_date, end_date, last_changed_type, max_retries),
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
    # from linkmerce.core.smartstore.api.order.extract import OrderTime
    # from linkmerce.core.smartstore.api.order.transform import OrderTime
    common = dict(
        module = get_module(".order"),
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        kwargs = dict(max_retries=max_retries),
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
            extractor = "OrderTime",
            transformer = "OrderTime",
            args = (start_date, end_date, "PURCHASE_DECIDED_DATETIME"),
        ),
        claim_completed = run_with_duckdb(
            **common,
            extractor = "OrderTime",
            transformer = "OrderTime",
            args = (start_date, end_date, "CLAIM_COMPLETED_DATETIME"),
        ),
    )
