from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject


class StockList(JsonTransformer):
    dtype = dict
    path = ["dsRealTime"]


class Stock(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, **kwargs):
        items = StockList().transform(obj)
        if items:
            return self.insert_into_table(items)
