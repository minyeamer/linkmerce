from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.transform import JsonObject


class CatalogItems(JsonTransformer):
    dtype = dict

    def is_valid_response(self, obj: dict) -> bool:
        if obj.get("errors"):
            from linkmerce.utils.map import hier_get
            msg = hier_get(obj, ["errors",0,"message"]) or "null"
            self.raise_request_error(f"An error occurred during the request: {msg}")
        return True


class _CatalogTransformer(DuckDBTransformer):
    object_type: Literal["catalogs","products"]
    queries: list[str] = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, mall_seq: int | str | None = None, **kwargs):
        items = CatalogItems(path=["data",self.object_type,"items"]).transform(obj)
        if items:
            params = dict(mall_seq=mall_seq) if self.object_type == "products" else None
            self.insert_into_table(items, params=params)


class BrandCatalog(_CatalogTransformer):
    object_type = "catalogs"
    queries = ["create", "select", "insert"]


class BrandProduct(_CatalogTransformer):
    object_type = "products"
    queries = ["create", "select", "insert"]


class BrandPrice(BrandProduct):
    object_type = "products"
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
    object_type = "products"
    queries = ["create", "select", "insert"]

    def insert_into_table(self, obj: list[dict], table: str = ":default:", **kwargs):
        super().insert_into_table(obj, key="insert", table=table, values=":select:")
