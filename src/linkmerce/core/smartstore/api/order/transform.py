from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class Order(DuckDBTransformer):
    """스마트스토어 조건형 상품 주문 상세 내역 조회 API 응답 데이터를 각각의 테이블에 변환 및 적재하는 클래스."""

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
                    "deliveryTagType", "inflowPath", "inflowPathAdd", "quantity", "unitPrice", "optionPrice",
                    "totalProductAmount", "productDiscountAmount", {"sellerBurdenDiscountAmount": None},
                    {"sellerBurdenStoreDiscountAmount": None}, "totalPaymentAmount", "expectedSettlementAmount",
                    "deliveryFeeAmount", {"shippingAddress": ["zipCode", "latitude", "longitude"]},
                    "sellerProductCode", "optionManageCode", "productName", "productOption", "decisionDate"
                ],
                "delivery": ["trackingNumber", "deliveryCompany", "deliveryMethod", "pickupDate", "sendDate"],
            }
        },
    )


class OrderTime(Order):
    """스마트스토어 조건형 상품 주문 상세 내역 조회 API 응답 데이터로부터   
    주문 상태에 따른 변경 날짜를 파싱해 `smartstore_order_time` 테이블에 적재하는 클래스."""

    tables = {"table": "smartstore_order_time"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data.contents",
        fields = {
            ".": ["productOrderId"],
            "content": {
                "order": ["orderId", "paymentDate"],
                "productOrder": ["decisionDate"],
                "delivery": ["sendDate", "deliveredDate"],
                "completedClaims.0": ["claimType", "claimRequestAdmissionDate"]
            }
        },
    )
    params = {"channel_seq": "$channel_seq"}


class OrderStatus(DuckDBTransformer):
    """스마트스토어 변경 상품 주문 내역 조회 API 응답 데이터를 `smartstore_order_time` 테이블에 적재하는 클래스."""

    tables = {"table": "smartstore_order_time"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data.lastChangeStatuses",
        fields = [
            "productOrderId", "orderId", "lastChangedType", "productOrderStatus",
            "claimType", "claimStatus", "receiverAddressChanged", "giftReceivingStatus",
            "paymentDate", "lastChangedDate"
        ],
    )
    params = {"channel_seq": "$channel_seq"}
