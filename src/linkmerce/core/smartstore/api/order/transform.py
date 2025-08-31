from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject


class OrderList(JsonTransformer):
    dtype = dict
    path = ["data","contents"]


class Order(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, **kwargs):
        orders = OrderList().transform(obj)
        if orders:
            self.validate_content(orders[0]["content"])
            self.insert_into_table(orders)

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
                order[key] = order.get(key)
        return order

    def validate_product_order(self, product_order: dict) -> dict:
        keys = ["merchantChannelId", "productId", "optionCode", "sellerProductCode", "optionManageCode", "productOrderStatus",
                "claimStatus", "productClass", "productName", "productOption", "inflowPath", "inflowPathAdd", "inflowPathAdd",
                "deliveryAttributeType", "quantity", "unitPrice", "optionPrice", "deliveryFeeAmount",
                "totalPaymentAmount", "expectedSettlementAmount", "decisionDate"]
        for key in keys:
            if key not in product_order:
                product_order[key] = product_order.get(key)
        return product_order

    def validate_delivery(self, delivery: dict) -> dict:
        for key in ["sendDate", "deliveredDate"]:
            if key not in delivery:
                delivery[key] = delivery.get(key)
        return delivery

    def validate_completed_claim(self, completed_claim: dict) -> dict:
        for key in ["claimType", "claimRequestAdmissionDate"]:
            if key not in completed_claim:
                completed_claim[key] = completed_claim.get(key)
        return completed_claim


class ProductOrder(Order):
    queries = ["create_order", "select_order", "insert_order", "create_option", "select_option", "upsert_option"]

    def set_tables(self, tables: dict | None = None):
        base = dict(order="smartstore_order", option="smartstore_option")
        super().set_tables(dict(base, **(tables or dict())))

    def create_table(self, **kwargs):
        super().create_table(key="create_order", table=":order:")
        super().create_table(key="create_option", table=":option:")

    def insert_into_table(self, obj: list[dict], **kwargs):
        super().insert_into_table(obj, key="insert_order", table=":order:", values=":select_order:")
        super().insert_into_table(obj, key="upsert_option", table=":option:", values=":select_option:")


class OrderTime(Order):
    queries = ["create", "select", "insert"]


class OrderStatusList(JsonTransformer):
    dtype = dict
    path = ["data","lastChangeStatuses"]


class OrderStatus(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, **kwargs):
        status = OrderStatusList().transform(obj)
        if status:
            status[0] = self.validate_change_status(status[0])
            self.insert_into_table(status)

    def validate_change_status(self, change_status: dict) -> dict:
        keys = ["productOrderId", "orderId", "lastChangedType", "productOrderStatus", "claimType", "claimStatus",
                "receiverAddressChanged", "giftReceivingStatus", "paymentDate", "lastChangedDate"]
        for key in keys:
            if key not in change_status:
                change_status[key] = change_status.get(key)
        return change_status
