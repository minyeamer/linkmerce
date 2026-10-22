from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence


class OrderParser(JsonTransformer):
    """카페24 주문 목록 조회 결과를 파싱하는 클래스."""

    dtype = dict
    scope = "orders"
    fields = [
        "order_id", "member_id", "billing_name", "payment_method", "order_date",
        "first_order", "payment_date", "order_place_name",
    ]


class OrderItemParser(OrderParser):
    """카페24 주문 내역의 상품 목록을 파싱하는 클래스."""

    dtype = dict
    scope = "orders"
    fields = [
        "order_id", "order_date", {"receivers.0.zipcode": None},
        {"item": [
            "order_item_code", "variant_code", "product_no", "custom_product_code",
            "option_value", "product_name", "product_price", "option_price",
            "additional_discount_price", "coupon_discount_price", "app_item_discount_amount",
            "quantity", "supplier_product_name", "tracking_no", "shipping_code",
            "individual_shipping_fee", "shipped_date", "delivered_date", # "purchaseconfirmation_date",
            "option_type", "supply_price",
        ]},
    ]

    def parse(self, obj: dict | list[dict], **kwargs) -> list[dict]:
        orders = [obj] if isinstance(obj, dict) else obj
        return [(order | {"item": item}) for order in orders for item in order["items"]]


class Order(DuckDBTransformer):
    """카페24 주문 목록 조회 결과를 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `Order`

    - **Parsers** ( *parser_class: input_type -> output_type* ):
        1. `OrderParser: dict -> list[dict]`
        2. `OrderItemParser: dict -> list[dict]`

    - **Tables** ( *table_key: table_name (description)* ):
        1. `order: cafe24_order` (주문 정보)
        2. `delivery: cafe24_order_delivery` (주문 배송 정보)
        3. `detail: cafe24_order_detail` (주문 결제 정보)
        4. `option: cafe24_order_option` (주문 품목 정보)

    Parameters
    ----------
    mall_id: str
        카페24 쇼핑몰 아이디
    """

    extractor = "Order"
    tables = {
        "order": "cafe24_order",
        "delivery": "cafe24_order_delivery",
        "detail": "cafe24_order_detail",
        "option": "cafe24_order_option",
    }
    parser = {"order": OrderParser, "order_item": OrderItemParser}
    params = {"mall_id": "$mall_id"}


class OrderTime(DuckDBTransformer):
    """카페24 주문 품목별 상태 변경 일시를 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `Order`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `OrderItemParser: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: cafe24_order_time`

    Parameters
    ----------
    mall_id: str
        카페24 쇼핑몰 아이디
    """

    extractor = "Order"
    tables = {"table": "cafe24_order_time"}
    parser = OrderItemParser
    parser_config = dict(
        fields = [
            "order_id", "order_date", "payment_date",
            {"item": [
                "order_item_code", "shipped_date", "delivered_date",
                "purchaseconfirmation_date", "cancel_date", "refund_date", "exchange_date",
            ]},
        ],
    )
    params = {"mall_id": "$mall_id"}


class OrderDetail(Order):
    """카페24 주문 상세 조회 결과를 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `OrderDetail`

    - **Parsers** ( *parser_class: input_type -> output_type* ):
        1. `OrderParser: dict -> list[dict]`
        2. `OrderItemParser: dict -> list[dict]`

    - **Tables** ( *table_key: table_name (description)* ):
        1. `order: cafe24_order` (주문 정보)
        2. `delivery: cafe24_order_delivery` (주문 배송 정보)
        3. `detail: cafe24_order_detail` (주문 결제 정보)
        4. `option: cafe24_order_option` (주문 품목 정보)

    Parameters
    ----------
    mall_id: str
        카페24 쇼핑몰 아이디
    """

    extractor = "OrderDetail"
    tables = {
        "order": "cafe24_order",
        "delivery": "cafe24_order_delivery",
        "detail": "cafe24_order_detail",
        "option": "cafe24_order_option",
    }
    parser = {"order": OrderParser, "order_item": OrderItemParser}
    parser_config = {"scope": "order"}
    params = {"mall_id": "$mall_id"}

    def set_queries(self, name: Literal["self"] | str = "self", keys: Sequence[str] | None = None):
        """`models.sql` 파일에서 쿼리를 불러온다. `name = "self"` -> `Order` 클래스명을 키로 사용한다."""
        super().set_queries(Order.__name__ if name == "self" else name, keys)


class OrderChange(DuckDBTransformer):
    """카페24 품목별 상태 변경 주문 목록을 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `OrderChange`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: cafe24_order_change`

    Parameters
    ----------
    mall_id: str
        카페24 쇼핑몰 아이디
    """

    extractor = "OrderChange"
    tables = {"table": "cafe24_order_change"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "changes",
        fields = ["order_id", "order_item_code", "order_status"],
    )
    params = {"mall_id": "$mall_id", "start_datetime": "$start_datetime", "end_datetime": "$end_datetime"}
