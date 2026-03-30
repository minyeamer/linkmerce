from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal, Sequence
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


def _get_api_configs(client_id: str, client_secret: str) -> dict:
    """커머스 API 인증에 필요한 설정을 구성한다."""
    return _get_api_configs(client_id, client_secret)


def request(
        client_id: str,
        client_secret: str,
        method: str,
        path: str,
        version: str | None = None,
        params: dict | list[tuple] | bytes | None = None,
        data: dict | list[tuple] | bytes | None = None,
        json: JsonObject | None = None,
        headers: dict[str, str] = None,
        extract_options: dict = dict(),
    ) -> JsonObject:
    """커머스 API에 임의의 HTTP 요청을 보낸다."""
    from linkmerce.core.smartstore.api.common import SmartstoreTestAPI
    from linkmerce.utils.nested import merge
    extractor = SmartstoreTestAPI(**merge(
        extract_options or dict(),
        configs = _get_api_configs(client_id, client_secret),
    ))
    return extractor.extract(method, path, version, params, data, json, headers)


@with_duckdb_connection(table="smartstore_product")
def product(
        client_id: str,
        client_secret: str,
        search_keyword: Sequence[int] = list(),
        keyword_type: Literal["CHANNEL_PRODUCT_NO", "PRODUCT_NO", "GROUP_PRODUCT_NO"] = "CHANNEL_PRODUCT_NO",
        status_type: Sequence[
            Literal["ALL", "WAIT", "SALE", "OUTOFSTOCK", "UNADMISSION", "REJECTION",
                    "SUSPENSION", "CLOSE", "PROHIBITION"]] = ["SALE"],
        period_type: Literal["PROD_REG_DAY", "SALE_START_DAY", "SALE_END_DAY", "PROD_MOD_DAY"] = "PROD_REG_DAY",
        from_date: dt.date | str | None = None,
        to_date: dt.date | str | None = None,
        channel_seq: int | str | None = None,
        max_retries: int = 5,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 커머스 API로 상품 목록을 조회하고 `smartstore_product` 테이블에 적재한다."""
    from linkmerce.core.smartstore.api.product.extract import Product
    from linkmerce.core.smartstore.api.product.transform import Product as T
    return Product(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(client_id, client_secret),
        options = {
            "PaginateAll": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(search_keyword, keyword_type, status_type, period_type, from_date, to_date, channel_seq, max_retries)


@with_duckdb_connection(table="smartstore_option")
def option(
        client_id: str,
        client_secret: str,
        product_id: Sequence[int | str],
        channel_seq: int | str | None = None,
        max_retries: int = 5,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 커머스 API로 채널 상품에 등록된 옵션 목록을 조회하고 `smartstore_option` 테이블에 적재한다."""
    from linkmerce.core.smartstore.api.product.extract import Option
    from linkmerce.core.smartstore.api.product.transform import Option as T
    return Option(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(client_id, client_secret),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(product_id, channel_seq, max_retries)


@with_duckdb_connection(tables={
    "product": "smartstore_product",
    "option": "smartstore_option",
    "merged": "smartstore_product_option",
})
def product_option(
        client_id: str,
        client_secret: str,
        search_keyword: Sequence[int] = list(),
        keyword_type: Literal["CHANNEL_PRODUCT_NO", "PRODUCT_NO", "GROUP_PRODUCT_NO"] = "CHANNEL_PRODUCT_NO",
        status_type: Sequence[Literal["ALL", "WAIT", "SALE", "OUTOFSTOCK", "UNADMISSION", "REJECTION", "SUSPENSION", "CLOSE", "PROHIBITION"]] = ["SALE"],
        period_type: Literal["PROD_REG_DAY", "SALE_START_DAY", "SALE_END_DAY", "PROD_MOD_DAY"] = "PROD_REG_DAY",
        from_date: dt.date | str | None = None,
        to_date: dt.date | str | None = None,
        channel_seq: int | str | None = None,
        max_retries: int = 5,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: tuple[dict | None, dict | None] = (None, None),
        transform_options: tuple[dict | None, dict | None] = (None, None),
    ) -> dict[str, JsonObject]:
    """네이버 커머스 API로 상품 목록 및 옵션 목록을 조회하고, 상품과 옵션을 통합한 결과를 생성한다.

    테이블 키 | 테이블명 | 설명
    - `product` | `smartstore_product` | 스마트스토어 상품 목록
    - `option` | `smartstore_option` | 스마트스토어 옵션 목록
    - `merged` | `smartstore_product_option` | 상품 목록과 옵션 목록을 병합"""
    PRODUCT, OPTION = 0, 1
    results = dict()
    return_type = return_type if return_type == "raw" else "none"

    args = (search_keyword, keyword_type, status_type, period_type, from_date, to_date, channel_seq, max_retries)
    results["product"] = product(
        client_id, client_secret, *args,
        connection=connection, request_delay=request_delay, progress=progress, return_type=return_type,
        extract_options=extract_options[PRODUCT], transform_options=transform_options[PRODUCT])

    where_clause = f" WHERE channel_seq = {channel_seq}" if channel_seq else str()
    query = f"SELECT DISTINCT product_id FROM smartstore_product{where_clause}"
    product_id = [row[0] for row in connection.execute(query)[0].fetchall()]
    results["option"] = option(
        client_id, client_secret, product_id, channel_seq, max_retries,
        connection=connection, request_delay=request_delay, progress=progress, return_type=return_type,
        extract_options=extract_options[OPTION], transform_options=transform_options[OPTION])

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
        , *[("R.option_"+x+str(i)) for i in range(1, 3+1) for x in ["group", "name"]]
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
    if connection.table_exists("smartstore_product_option"):
        keyword = f"INSERT INTO smartstore_product_option "
    else:
        keyword = f"CREATE TABLE smartstore_product_option AS "
    connection.execute(
        keyword
        + f"SELECT {', '.join(columns)} "
        + f"FROM smartstore_product AS L "
        + f"LEFT JOIN smartstore_option AS R "
            + "ON L.product_id = R.product_id")
    return results


@with_duckdb_connection(tables={
    "order": "smartstore_order",
    "product_order": "smartstore_product_order",
    "delivery": "smartstore_delivery",
    "option": "smartstore_option",
})
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
        max_retries: int = 5,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 커머스 API로 조건형 상품 주문 상세 내역 조회 결과를 수집해 각각의 테이블에 적재한다.

    테이블 키 | 테이블명 | 설명
    - `order` | `smartstore_order` | 주문 정보
    - `product_order` | `smartstore_product_order` | 상품 주문 정보
    - `delivery` | `smartstore_delivery` | 주문 배송 정보
    - `option` | `smartstore_option` | 주문 옵션 정보"""
    from linkmerce.core.smartstore.api.order.extract import Order
    from linkmerce.core.smartstore.api.order.transform import Order as T
    return Order(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(client_id, client_secret),
        options = {
            "CursorAll": {
                "request_delay": request_delay
            },
            "RequestEachCursor": {
                "tqdm_options": {"disable": (not progress)}
            },
        },
    )).extract(start_date, end_date, range_type, product_order_status, claim_status, place_order_status, page_start, max_retries)


@with_duckdb_connection(table="smartstore_order_time")
def order_status(
        client_id: str,
        client_secret: str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        last_changed_type: str | None = None,
        channel_seq: int | str | None = None,
        max_retries: int = 5,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 커머스 API로 변경 상품 주문 내역 조회 결과를 수집해 `smartstore_order_time` 테이블에 적재한다."""
    from linkmerce.core.smartstore.api.order.extract import OrderStatus
    from linkmerce.core.smartstore.api.order.transform import OrderStatus as T
    return OrderStatus(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(client_id, client_secret),
        options = {
            "CursorAll": {
                "request_delay": request_delay
            },
            "RequestEachCursor": {
                "tqdm_options": {"disable": (not progress)}
            },
        },
    )).extract(start_date, end_date, last_changed_type, channel_seq, max_retries)


@with_duckdb_connection(table="smartstore_order_time")
def aggregated_order_status(
        client_id: str,
        client_secret: str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        channel_seq: int | str | None = None,
        max_retries: int = 5,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: tuple[dict | None, dict | None, dict | None] = (None, None, None),
        transform_options: tuple[dict | None, dict | None, dict | None] = (None, None, None),
    ) -> dict[str, JsonObject]:
    """네이버 커머스 API로 변경 상품 주문 내역과 주문 상태에 따른 변경 날짜를 수집해
    `smartstore_order_time` 테이블에 적재한다."""
    from linkmerce.core.smartstore.api.order.extract import OrderStatus
    from linkmerce.core.smartstore.api.order.transform import OrderStatus as T
    STATUS, PURCHASE, CLAIM = 0, 1, 2
    common = {
        "configs": _get_api_configs(client_id, client_secret),
        "options": {
            "CursorAll": {
                "request_delay": request_delay
            },
            "RequestEachCursor": {
                "tqdm_options": {"disable": (not progress)}
            },
        },
    }
    kwargs = {"channel_seq": channel_seq, "max_retries": max_retries}
    return_type = return_type if return_type == "raw" else "none"

    results = {
        "order_status": OrderStatus(**prepare_duckdb_extract(
            T, connection, extract_options[STATUS], transform_options[STATUS], return_type, **common,
        )).extract(start_date, end_date, **kwargs),
    }

    from linkmerce.core.smartstore.api.order.extract import Order
    from linkmerce.core.smartstore.api.order.transform import OrderTime as T

    results.update({
        "purchase_decided": Order(**prepare_duckdb_extract(
            T, connection, extract_options[PURCHASE], transform_options[PURCHASE], return_type, **common,
        )).extract(start_date, end_date, "PURCHASE_DECIDED_DATETIME", **kwargs),
        "claim_completed": Order(**prepare_duckdb_extract(
            T, connection, extract_options[CLAIM], transform_options[CLAIM], return_type, **common,
        )).extract(start_date, end_date, "CLAIM_COMPLETED_DATETIME", **kwargs),
    })
    return results


@with_duckdb_connection(table="marketing_channel")
def marketing_channel(
        client_id: str,
        client_secret: str,
        channel_seq: int | str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        max_retries: int = 5,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 커머스 API로 상품/마케팅 채널 데이터를 조회하고 `marketing_channel` 테이블에 적재한다."""
    from linkmerce.core.smartstore.api.bizdata.extract import MarketingChannel
    from linkmerce.core.smartstore.api.bizdata.transform import MarketingChannel as T
    return MarketingChannel(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(client_id, client_secret),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(channel_seq, start_date, end_date, max_retries)
