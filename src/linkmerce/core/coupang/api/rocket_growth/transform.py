from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence


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
    """쿠팡 로켓그로스 주문 단건 또는 목록을 파싱하는 클래스."""

    dtype = dict
    scope = "data"
    fields = [
            "orderId", "vendorId", "paidAt",
            {"orderItem": ["vendorItemId", "productName", "salesQuantity", "unitSalesPrice"]}
        ]

    def parse(self, obj: dict | list[dict], **kwargs) -> list[dict]:
        """각 주문 정보의 옵션 목록을 평탄화한 주문 목록을 반환한다."""
        orders = [obj] if isinstance(obj, dict) else obj
        return [(order | {"orderItem": item})
            for order in orders for item in (order.get("orderItems") or list())]


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


class OrderDetail(Order):
    """쿠팡 주문번호별 로켓그로스 주문 조회 결과를 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `OrderDetail`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `OrderDetailParser: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: coupang_rocket_order`
    """

    extractor = "OrderDetail"
    tables = {"table": "coupang_rocket_order"}
    parser = OrderParser

    def set_queries(self, name: Literal["self"] | str = "self", keys: Sequence[str] | None = None):
        """`models.sql` 파일에서 쿼리를 불러온다. `name = "self"` -> `Order` 클래스명을 키로 사용한다."""
        super().set_queries(Order.__name__ if name == "self" else name, keys)
