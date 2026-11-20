from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


class RevenueHistoryParser(JsonTransformer):
    """쿠팡 마켓플레이스 매출내역의 상품 목록을 파싱하는 클래스."""

    dtype = dict
    scope = "data"
    fields = [
        "orderId", "saleType", "saleDate", "recognitionDate", "settlementDate",
        {"item": [
            "productId", "vendorItemId", "salePrice", "quantity", "coupangDiscountCoupon",
            "saleAmount", "sellerDiscountCoupon", "downloadableCoupon",
            "serviceFee", "serviceFeeVat", "settlementAmount"
        ]}
    ]

    def parse(self, obj: list[dict], **kwargs) -> list[dict]:
        return [(order | {"item": item}) for order in obj for item in order["items"]]


class RevenueHistory(DuckDBTransformer):
    """쿠팡 마켓플레이스 매출내역을 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `RevenueHistory`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `RevenueHistoryParser: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: coupang_revenue_history`

    Parameters
    ----------
    vendor_id: str
        업체 코드
    """

    extractor = "RevenueHistory"
    tables = {"table": "coupang_revenue_history"}
    parser = RevenueHistoryParser
    params = {"vendor_id": "$vendor_id"}
