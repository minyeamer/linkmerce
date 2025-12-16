from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject


class InventoryList(JsonTransformer):
    dtype = dict
    path = ["Data","Result"]


class Inventory(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, **kwargs):
        results = InventoryList().transform(obj)
        if results:
            self.insert_into_table(results)
