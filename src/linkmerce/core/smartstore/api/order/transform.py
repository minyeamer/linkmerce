from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


class OrderParser(JsonTransformer):
    """스마트스토어 조건형 상품 주문 상세 내역 조회 API 응답 데이터에서 변환 대상 필드 구성을 보장하는 파서 클래스."""

    dtype = dict
    scope = "data.contents"

    def select_and_extend(self, item: dict, extends: dict | None = None, **kwargs) -> dict:
        """항목 내 필수 키 존재 여부를 확인하고 보정한 뒤 필드 추출을 수행한다."""
        content: dict = item["content"]
        for key in ["order", "productOrder", "delivery"]:
            if not isinstance(content.get(key), dict):
                content[key] = dict()
        if not isinstance(content.get("completedClaims"), list):
            content["completedClaims"] = [dict()]
        return super().select_and_extend(item, extends=extends, **kwargs)


class Order(DuckDBTransformer):
    """스마트스토어 조건형 상품 주문 상세 내역 조회 API 응답 데이터를 각각의 테이블에 변환 및 적재하는 클래스.

    테이블 키 | 테이블명 | 설명
    - `order` | `smartstore_order` | 주문 정보
    - `product_order` | `smartstore_product_order` | 상품 주문 정보
    - `delivery` | `smartstore_delivery` | 주문 배송 정보
    - `option` | `smartstore_option` | 주문 옵션 정보"""

    extractor = "Order"
    tables = {table: f"smartstore_{table}" for table in ["order", "product_order", "delivery", "option"]}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data.contents",
        fields = {
            ".": ["productOrderId"],
            "content": {
                "order": ["orderId", "ordererNo", "payLocationType", "orderDate", "paymentDate"],
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
    """스마트스토어 조건형 상품 주문 상세 내역 조회 API 응답 데이터로부터   
    주문 상태에 따른 변경 날짜를 파싱해 `smartstore_order_time` 테이블에 적재하는 클래스."""

    extractor = "Order"
    tables = {"table": "smartstore_order_time"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data.contents",
        fields = {
            ".": ["productOrderId"],
            "content": {
                "order": ["orderId", "paymentDate"],
                "productOrder": [{"decisionDate": None}],
                "delivery": [{"sendDate": None}, {"deliveredDate": None}],
                "completedClaims.0": [{"claimType": None}, {"claimRequestAdmissionDate": None}]
            }
        },
    )
    params = {"channel_seq": "$channel_seq"}


class OrderStatus(DuckDBTransformer):
    """스마트스토어 변경 상품 주문 내역 조회 API 응답 데이터를 `smartstore_order_time` 테이블에 적재하는 클래스."""

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
