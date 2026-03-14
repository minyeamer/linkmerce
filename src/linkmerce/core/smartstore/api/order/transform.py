from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class Order(DuckDBTransformer):
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
                    "totalProductAmount", "productDiscountAmount", "sellerBurdenDiscountAmount", "totalPaymentAmount",
                    "expectedSettlementAmount", "deliveryFeeAmount", {"shippingAddress": ["zipCode", "latitude", "longitude"]},
                    "sellerProductCode", "optionManageCode", "productName", "productOption", "decisionDate"
                ],
                "delivery": ["trackingNumber", "deliveryCompany", "deliveryMethod", "pickupDate", "sendDate"],
            }
        },
    )

class OrderTime(DuckDBTransformer):
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
        defaults = {"channelSeq": "$channel_seq"},
    )


class OrderStatus(DuckDBTransformer):
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
        defaults = {"channelSeq": "$channel_seq"},
    )
