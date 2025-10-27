from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject


class CatalogItems(JsonTransformer):
    dtype = dict

    def is_valid_response(self, obj: dict) -> bool:
        if obj.get("errors"):
            from linkmerce.utils.map import hier_get
            msg = hier_get(obj, ["errors",0,"message"]) or "null"
            self.raise_request_error(f"An error occurred during the request: {msg}")
        return True


class BrandCatalog(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, **kwargs):
        items = CatalogItems(path=["items"]).transform(obj)
        if items:
            self.insert_into_table(items)


class BrandProduct(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, mall_seq: int | str | None = None, **kwargs):
        items = CatalogItems(path=["items"]).transform(obj)
        if items:
            self.insert_into_table(items, params=dict(mall_seq=mall_seq))


class BrandPrice(BrandProduct):
    queries = ["create_price", "select_price", "insert_price", "create_product", "select_product", "upsert_product"]

    def set_tables(self, tables: dict | None = None):
        base = dict(price="naver_brand_price", product="naver_brand_product")
        super().set_tables(dict(base, **(tables or dict())))

    def create_table(self, **kwargs):
        super().create_table(key="create_price", table=":price:")
        super().create_table(key="create_product", table=":product:")

    def insert_into_table(self, obj: list[dict], params: dict = dict(), **kwargs):
        super().insert_into_table(obj, key="insert_price", table=":price:", values=":select_price:", params=params)
        super().insert_into_table(obj, key="upsert_product", table=":product:", values=":select_product:", params=params)


class ProductCatalog(BrandProduct):
    queries = ["create", "select", "insert"]

    def insert_into_table(self, obj: list[dict], table: str = ":default:", **kwargs):
        super().insert_into_table(obj, key="insert", table=table, values=":select:")
