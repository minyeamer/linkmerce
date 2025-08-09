from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import JsonObject
    from duckdb import DuckDBPyRelation


class _SearchTransformer(DuckDBTransformer):
    content_type: Literal["blog","news","book","adult","encyc","cafearticle","kin","local","errata","webkr","image","shop","doc"]
    queries: list[str] = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, query: str, start: int = 1, **kwargs):
        if isinstance(obj, dict):
            if "errorMessage" not in obj:
                params = dict(keyword=query, start=(start-1))
                return self.insert_into_table(obj["items"], params=params) if obj["items"] else None
            else:
                self.raise_request_error(obj.get("errorMessage") or str())
        else:
            self.raise_parse_error()


class BlogSearch(_SearchTransformer):
    content_type = "blog"
    queries = ["create", "select", "insert"]


class NewsSearch(_SearchTransformer):
    content_type = "news"
    queries = ["create", "select", "insert"]


class BookSearch(_SearchTransformer):
    content_type = "book"
    queries = ["create", "select", "insert"]


class CafeSearch(_SearchTransformer):
    content_type = "cafe"
    queries = ["create", "select", "insert"]


class KiNSearch(_SearchTransformer):
    content_type = "kin"
    queries = ["create", "select", "insert"]


class ImageSearch(_SearchTransformer):
    content_type = "image"
    queries = ["create", "select", "insert"]


class ShoppingSearch(_SearchTransformer):
    content_type = "shop"
    queries = ["create", "select", "insert"]


class ShoppingRank(_SearchTransformer):
    content_type = "shop"
    queries = ["create_rank", "select_rank", "insert_rank", "create_product", "select_product", "upsert_product"]

    def create_table(
            self,
            rank_table: str = ":default:",
            product_table: str = "product",
            params: dict = dict(),
            **kwargs
        ) -> tuple[DuckDBPyRelation,DuckDBPyRelation]:
        rank = super().create_table(key="create_rank", table=rank_table, params=params)
        product = super().create_table(key="create_product", table=product_table, params=params)
        return rank, product

    def insert_into_table(
            self,
            obj: list,
            rank_table: str = ":default:",
            product_table: str = "product",
            params: dict = dict(),
            **kwargs
        ) -> tuple[DuckDBPyRelation,DuckDBPyRelation]:
        rank = super().insert_into_table(obj, key="insert_rank", table=rank_table, values=":select_rank:", params=params)
        product = super().insert_into_table(obj, key="upsert_product", table=product_table, values=":select_product:")
        return rank, product
