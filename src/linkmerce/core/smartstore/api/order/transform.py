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
            return self.insert_into_table(orders)

    def validate_content(self, content: dict):
        productOrder = content["productOrder"] or dict()
        for key in ["sellerProductCode","optionManageCode","claimStatus","productOption","decisionDate"]:
            if key not in productOrder:
                productOrder[key] = productOrder.get(key)
        delivery = content["delivery"] or dict()
        for key in ["sendDate","deliveredDate"]:
            if key not in delivery:
                delivery[key] = delivery.get(key)
        content.update(productOrder=productOrder, delivery=delivery)
