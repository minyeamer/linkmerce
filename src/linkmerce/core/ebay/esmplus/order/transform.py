from linkmerce.common.transform import DuckDBTransformer


class Order(DuckDBTransformer):
    """ESM PLUS 주문내역을 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `Order`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Tables** ( *table_key: table_name (description)* ):
        1. `order: esmplus_order` (주문 정보)
        2. `item: esmplus_order_item` (주문 상품 정보)
    """

    extractor = "Order"
    tables = {"order": "esmplus_order", "item": "esmplus_order_item"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data.list",
        fields = [
            "siteId", "orderNo", "siteOrderNo", "goodsNo", "goodsName",
            "sellerId", "buyerCustNo", "tradeAmnt", "orderQty",
            "depositConfirmDate", "buyDecisionDate", "deliveryFinishDate",
            "sttlDate", "refundApprDate", "deliveryStatus", "claimType",
            "imgFileName", "deliveryStatusCode"
        ],
    )
