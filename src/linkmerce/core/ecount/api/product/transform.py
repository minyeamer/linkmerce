from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject


class ProductList(JsonTransformer):
    dtype = dict
    path = ["Data","Result"]


class Product(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, **kwargs):
        products = ProductList().transform(obj)
        if products:
            self.insert_into_table(products)
