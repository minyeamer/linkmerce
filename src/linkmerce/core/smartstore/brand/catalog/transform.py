from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import JsonObject
    from duckdb import DuckDBPyRelation


class _CatalogTransformer(DuckDBTransformer):
    object_type: Literal["catalogs","products"]
    queries: list[str] = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, mall_seq: int | str | None = None, **kwargs):
        if isinstance(obj, dict):
            if not obj.get("errors"):
                items = obj["data"][self.object_type]["items"]
                params = dict(mall_seq=mall_seq) if self.object_type == "products" else None
                return self.insert_into_table(items, params=params) if items else None
            else:
                from linkmerce.utils.map import hier_get
                msg = hier_get(obj, ["errors",0,"message"]) or "null"
                self.raise_request_error(f"An error occurred during the request: {msg}")
        else:
            self.raise_parse_error()


class BrandCatalog(_CatalogTransformer):
    object_type = "catalogs"
    queries = ["create", "select", "insert"]


class BrandProduct(_CatalogTransformer):
    object_type = "products"
    queries = ["create", "select", "insert"]


class BrandPrice(BrandProduct):
    object_type = "products"
    queries = ["create_price", "select_price", "insert_price", "create_product", "select_product", "upsert_product"]

    def create_table(
            self,
            price_table: str = ":default:",
            product_table: str = "product",
            **kwargs
        ) -> tuple[DuckDBPyRelation,DuckDBPyRelation]:
        price = super().create_table(key="create_price", table=price_table)
        product = super().create_table(key="create_product", table=product_table)
        return price, product

    def insert_into_table(
            self,
            obj: list,
            price_table: str = ":default:",
            product_table: str = "product",
            params: dict = dict(),
            **kwargs
        ) -> tuple[DuckDBPyRelation,DuckDBPyRelation]:
        price = super().insert_into_table(obj, key="insert_price", table=price_table, values=":select_price:", params=params)
        product = super().insert_into_table(obj, key="upsert_product", table=product_table, values=":select_product:", params=params)
        return price, product


class MatchCatalog(_CatalogTransformer):
    object_type = "products"
    queries = ["create", "select", "insert"]

    def insert_into_table(
            self,
            obj: list,
            table: str = ":default:",
            **kwargs
        ) -> tuple[DuckDBPyRelation,DuckDBPyRelation]:
        return super().insert_into_table(obj, key="insert", table=table, values=":select:")
