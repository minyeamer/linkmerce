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
        productOrder = content["productOrder"] or dict()
        for key in ["sellerProductCode","optionManageCode","claimStatus","productOption","inflowPathAdd","decisionDate"]:
            if key not in productOrder:
                productOrder[key] = productOrder.get(key)
        delivery = content["delivery"] or dict()
        for key in ["sendDate","deliveredDate"]:
            if key not in delivery:
                delivery[key] = delivery.get(key)
        content.update(productOrder=productOrder, delivery=delivery)


class ProductOrder(Order):
    queries = ["create_order", "select_order", "insert_order", "create_option", "select_option", "upsert_option"]

    def create_table(
            self,
            order_table: str = ":default:",
            option_table: str = "option",
            **kwargs
        ):
        super().create_table(key="create_order", table=order_table)
        super().create_table(key="create_option", table=option_table)

    def insert_into_table(
            self,
            obj: list[dict],
            order_table: str = ":default:",
            option_table: str = "option",
            **kwargs
        ):
        super().insert_into_table(obj, key="insert_order", table=order_table, values=":select_order:")
        super().insert_into_table(obj, key="upsert_option", table=option_table, values=":select_option:")


class OrderStatusList(JsonTransformer):
    dtype = dict
    path = ["data","lastChangeStatuses"]


class OrderStatus(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, **kwargs):
        status = OrderStatusList().transform(obj)
        if status:
            self.insert_into_table(status)
