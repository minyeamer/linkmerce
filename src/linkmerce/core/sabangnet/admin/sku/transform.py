from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any
    from linkmerce.common.transform import JsonObject


class SearchList(JsonTransformer):
    dtype = dict
    path = ["data", "list"]


class MappingSearch(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, sku_yn: bool | None = None, **kwargs):
        products = SearchList().transform(obj)
        if products:
            return self.insert_into_table(products, params=dict(sku_yn=sku_yn))


class SkuList(JsonTransformer):
    dtype = dict
    path = ["data"]


class MappingList(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, query: dict, **kwargs):
        lists = SkuList().transform(obj)
        if lists:
            return self.insert_into_table(lists, params=dict(shop_id=query["shop_id"]))
