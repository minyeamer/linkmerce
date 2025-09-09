from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject


class OrderList(JsonTransformer):
    dtype = dict
    path = ["data", "orderList"]


class Order(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, **kwargs):
        orders = OrderList().transform(obj)
        if orders:
            return self.insert_into_table(orders)


class OrderDownload(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: bytes, **kwargs):
        from linkmerce.utils.openpyxl import excel2json
        orders = excel2json(obj, warnings=False)
        if orders:
            return self.insert_into_table(orders)
