from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence


class OrderParser(JsonTransformer):
    """쿠팡 마켓플레이스 발주서 목록 조회 결과를 파싱하는 클래스."""

    dtype = dict
    scope = "data"
    fields = [
        "shipmentBoxId", "orderId", "orderedAt", "paidAt", "status", "seller.sellerId",
        {"orderer": ["name", "safeNumber"]}, "shippingPrice.units", "receiver.postCode",
        "invoiceNumber", "inTrasitDateTime", "deliveredDate", "refer", "shipmentType",
    ]


class OrderItemParser(JsonTransformer):
    """쿠팡 마켓플레이스 발주서의 상품 목록을 파싱하는 클래스."""

    dtype = dict
    scope = "data"
    fields = [
        "shipmentBoxId", "orderId", "orderedAt", "seller.sellerId",
        {"item": [
            "sequenceNo", {"productId": None}, "vendorItemId", "sellerProductId", "shippingCount",
            "salesPrice.units", "orderPrice.units", "instantCouponDiscount.units",
            "downloadableCouponDiscount.units", "coupangDiscount.units", "sellerProductName",
            "sellerProductItemName", "vendorItemName",
        ]},
    ]

    def parse(self, obj: list[dict], **kwargs) -> list[dict]:
        return [(order | {"item": item}) for order in obj for item in order["orderItems"]]


class Order(DuckDBTransformer):
    """쿠팡 마켓플레이스 발주서 목록 조회 결과를 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `Order`

    - **Parsers** ( *parser_class: input_type -> output_type* ):
        1. `OrderParser: dict -> list[dict]`
        2. `OrderItemParser: dict -> list[dict]`

    - **Tables** ( *table_key: table_name (description)* ):
        1. `order: coupang_order` (발주서 정보)
        2. `delivery: coupang_order_delivery` (발주서 배송 정보)
        3. `detail: coupang_order_detail` (발주서 결제 정보)
        4. `item: coupang_order_item` (발주서 상품 정보)
    """

    extractor = "Order"
    tables = {
        "order": "coupang_order",
        "delivery": "coupang_order_delivery",
        "detail": "coupang_order_detail",
        "item": "coupang_order_item",
    }
    parser = {"order": OrderParser, "order_item": OrderItemParser}


class OrderDetail(Order):
    """쿠팡 주문번호별 또는 배송번호별 마켓플레이스 발주서 조회 결과를 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**
        - `OrderDetail`

    - **Parsers**
        - `OrderParser: dict -> list[dict]`
        - `OrderItemParser: dict -> list[dict]`

    - **Tables** ( *table_key: table_name (description)* ):
        1. `order: coupang_order` (발주서 정보)
        2. `delivery: coupang_order_delivery` (발주서 배송 정보)
        3. `detail: coupang_order_detail` (발주서 결제 정보)
        4. `item: coupang_order_item` (발주서 상품 정보)
    """

    extractor = "OrderDetail"
    tables = {
        "order": "coupang_order",
        "delivery": "coupang_order_delivery",
        "detail": "coupang_order_detail",
        "item": "coupang_order_item",
    }
    parser = {"order": OrderParser, "order_item": OrderItemParser}

    def set_queries(self, name: Literal["self"] | str = "self", keys: Sequence[str] | None = None):
        """`models.sql` 파일에서 쿼리를 불러온다. `name = "self"` -> `Order` 클래스명을 키로 사용한다."""
        super().set_queries(Order.__name__ if name == "self" else name, keys)
