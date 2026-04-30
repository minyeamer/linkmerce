from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class Order(DuckDBTransformer):
    """스마트스토어 상품 주문 내역 조회 결과를 변환 및 적재하는 클래스.

    - **Extractor**: `Order`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Tables** ( *table_key: table_name (description)* ):
        1. `order: smartstore_order` (주문 정보)
        2. `product_order: smartstore_product_order` (상품 주문 정보)
        3. `delivery: smartstore_delivery` (주문 배송 정보)
        4. `option: smartstore_option` (주문 옵션 정보)
    """

    extractor = "Order"
    tables = {table: f"smartstore_{table}" for table in ["order", "product_order", "delivery", "option"]}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data.contents",
        fields = {
            ".": ["productOrderId"],
            "content": {
                "order": ["orderId", "ordererNo", "payLocationType", "orderDate", {"paymentDate": None}],
                "productOrder": [
                    "merchantChannelId", "productId", "optionCode", "productClass", "deliveryAttributeType",
                    {"deliveryTagType": None}, "inflowPath", {"inflowPathAdd": None}, "quantity", "unitPrice", "optionPrice",
                    "totalProductAmount", "productDiscountAmount", {"sellerBurdenDiscountAmount": None},
                    {"sellerBurdenStoreDiscountAmount": None}, "totalPaymentAmount", "expectedSettlementAmount",
                    "deliveryFeeAmount", {"shippingAddress": [{key: None} for key in ["zipCode", "latitude", "longitude"]]},
                    {"sellerProductCode": None}, {"optionManageCode": None}, "productName",
                    {"productOption": None}, {"decisionDate": None}
                ],
                "delivery": [
                    {key: None} for key in ["trackingNumber", "deliveryCompany", "deliveryMethod", "pickupDate", "sendDate"]
                ],
            }
        },
    )


class OrderTime(Order):
    """스마트스토어 상품 주문 내역 조회 결과를 변환 및 적재하는 클래스.

    **NOTE** 변경 상품 주문 내역을 다루는 `OrderStatus` 클래스와 동일한 구성의 테이블을 가진다.

    - **Extractor**: `Order`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table`: `smartstore_order_time`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    channel_seq: int | str
        채널 번호
    """

    extractor = "Order"
    tables = {"table": "smartstore_order_time"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data.contents",
        fields = {
            ".": ["productOrderId"],
            "content": {
                "order": ["orderId", {"paymentDate": None}],
                "productOrder": [{"decisionDate": None}],
                "delivery": [{"sendDate": None}, {"deliveredDate": None}],
                "completedClaims.0": [{"claimType": None}, {"claimRequestAdmissionDate": None}]
            }
        },
    )
    params = {"channel_seq": "$channel_seq"}


class OrderStatus(DuckDBTransformer):
    """스마트스토어 변경 상품 주문 내역 조회 결과를 변환 및 적재하는 클래스.

    - **Extractor**: `OrderStatus`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: smartstore_order_time`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    channel_seq: int | str
        채널 번호
    """

    extractor = "OrderStatus"
    tables = {"table": "smartstore_order_time"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data.lastChangeStatuses",
        fields = [
            "productOrderId", "orderId", "lastChangedType", "productOrderStatus",
            {"claimType": None}, {"claimStatus": None}, "receiverAddressChanged",
            {"giftReceivingStatus": None}, {"paymentDate": None}, "lastChangedDate"
        ],
    )
    params = {"channel_seq": "$channel_seq"}

    def parse(self, obj: dict, **kwargs) -> list[dict]:
        if isinstance(obj, dict) and ("data" not in obj):
            return list()
        return super().parse(obj, **kwargs)
