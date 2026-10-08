from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


class Inventory(DuckDBTransformer):
    """쿠팡 로켓창고 재고를 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `Inventory`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: coupang_rocket_inventory`
    """

    extractor = "Inventory"
    tables = {"table": "coupang_rocket_inventory"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data",
        fields = [
            "vendorItemId", "vendorId", "salesCountMap.SALES_COUNT_LAST_THIRTY_DAYS",
            "inventoryDetails.totalOrderableQuantity", "externalSkuId"
        ],
    )


class OrderParser(JsonTransformer):
    """쿠팡 로켓그로스 주문 목록을 파싱하는 클래스."""

    dtype = dict
    scope = "data"
    fields = [
            "orderId", "vendorId", "paidAt",
            {"orderItem": ["vendorItemId", "productName", "salesQuantity", "unitSalesPrice"]}
        ]

    def parse(self, obj: list[dict], **kwargs) -> list[dict]:
        """각 주문 정보의 옵션 목록을 평탄화한 주문 목록을 반환한다."""
        return [(order | {"orderItem": item})
            for order in obj for item in (order.get("orderItems") or list())]


class Order(DuckDBTransformer):
    """쿠팡 로켓그로스 주문 목록을 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `Order`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `OrderParser: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: coupang_rocket_order`
    """

    extractor = "Order"
    tables = {"table": "coupang_rocket_order"}
    parser = OrderParser
