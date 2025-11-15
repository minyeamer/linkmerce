from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject


class OrderList(JsonTransformer):
    dtype = dict
    path = ["data","contents"]

    def transform(self, obj: JsonObject, **kwargs):
        orders = super().transform(obj, **kwargs)
        if orders:
            self.validate_content(orders[0]["content"])
        return orders

    def validate_content(self, content: dict):
        from linkmerce.utils.map import hier_get
        order = self.validate_order(content.get("order") or dict())
        product_order = self.validate_product_order(content.get("productOrder") or dict())
        delivery = self.validate_delivery(content.get("delivery") or dict())
        completed_claim = self.validate_completed_claim(hier_get(content, ["completedClaims",0]) or dict())
        content.update(order=order, productOrder=product_order, delivery=delivery, completedClaims=[completed_claim])

    def validate_order(self, order: dict) -> dict:
        for key in ["orderId", "ordererNo", "ordererId", "ordererName", "payLocationType", "orderDate", "paymentDate"]:
            if key not in order:
                order[key] = None
        return order

    def validate_product_order(self, product_order: dict) -> dict:
        keys = ["merchantChannelId", "productId", "optionCode", "sellerProductCode", "optionManageCode", "productOrderStatus",
                "claimStatus", "productClass", "productName", "productOption", "inflowPath", "inflowPathAdd", "inflowPathAdd",
                "deliveryAttributeType", "deliveryTagType", "quantity", "unitPrice", "optionPrice", "deliveryFeeAmount",
                "totalPaymentAmount", "paymentCommission", "expectedSettlementAmount", "decisionDate"]
        for key in keys:
            if key not in product_order:
                product_order[key] = None
        product_order["shippingAddress"] = dict(
            (product_order.get("shippingAddress") or dict()), zipCode=None, longitude=None, latitude=None)
        return product_order

    def validate_delivery(self, delivery: dict) -> dict:
        for key in ["trackingNumber", "deliveryCompany", "deliveryMethod", "pickupDate", "sendDate", "deliveredDate"]:
            if key not in delivery:
                delivery[key] = None
        return delivery

    def validate_completed_claim(self, completed_claim: dict) -> dict:
        for key in ["claimType", "claimRequestAdmissionDate"]:
            if key not in completed_claim:
                completed_claim[key] = None
        return completed_claim


ORDER_TABLES = ["order", "product_order", "delivery", "option"]

class Order(DuckDBTransformer):
    queries = [f"{keyword}_{table}"
        for table in ORDER_TABLES
        for keyword in ["create", "select", "insert"]
    ]

    def set_tables(self, tables: dict | None = None):
        base = {table: f"smartstore_{table}" for table in ORDER_TABLES}
        super().set_tables(dict(base, **(tables or dict())))

    def create_table(self, **kwargs):
        for table in ORDER_TABLES:
            super().create_table(key=f"create_{table}", table=f":{table}:")

    def transform(self, obj: JsonObject, **kwargs):
        orders = OrderList().transform(obj)
        if orders:
            for table in ORDER_TABLES:
                self.insert_into_table(orders, key=f"insert_{table}", table=f":{table}:", values=f":select_{table}:")


class OrderTime(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, channel_seq: int | str | None = None, **kwargs):
        orders = OrderList().transform(obj)
        if orders:
            self.insert_into_table(orders, params=dict(channel_seq=channel_seq))


class OrderStatusList(JsonTransformer):
    dtype = dict
    path = ["data","lastChangeStatuses"]


class OrderStatus(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, channel_seq: int | str | None = None, **kwargs):
        status = OrderStatusList().transform(obj)
        if status:
            status[0] = self.validate_change_status(status[0])
            self.insert_into_table(status, params=dict(channel_seq=channel_seq))

    def validate_change_status(self, change_status: dict) -> dict:
        keys = ["productOrderId", "orderId", "lastChangedType", "productOrderStatus", "claimType", "claimStatus",
                "receiverAddressChanged", "giftReceivingStatus", "paymentDate", "lastChangedDate"]
        for key in keys:
            if key not in change_status:
                change_status[key] = None
        return change_status
