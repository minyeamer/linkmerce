from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.transform import JsonObject


class SearchItems(JsonTransformer):
    dtype = dict

    def is_valid_response(self, obj: dict) -> bool:
        if "errorMessage" in obj:
            self.raise_request_error(obj.get("errorMessage") or str())
        return True


class _SearchTransformer(DuckDBTransformer):
    content_type: Literal["blog","news","book","adult","encyc","cafearticle","kin","local","errata","webkr","image","shop","doc"]
    queries: list[str] = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, query: str, start: int = 1, **kwargs):
        items = SearchItems(path=["items"]).transform(obj)
        if items:
            params = dict(keyword=query, start=(start-1))
            self.insert_into_table(items, params=params)


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

    def set_tables(self, tables: dict | None = None):
        base = dict(rank="naver_rank_shop", product="naver_product")
        super().set_tables(dict(base, **(tables or dict())))

    def create_table(self, params: dict = dict(), **kwargs):
        super().create_table(key="create_rank", table=":rank:", params=params)
        super().create_table(key="create_product", table=":product:", params=params)

    def insert_into_table(self, obj: list[dict], params: dict = dict(), **kwargs):
        super().insert_into_table(obj, key="insert_rank", table=":rank:", values=":select_rank:", params=params)
        super().insert_into_table(obj, key="upsert_product", table=":product:", values=":select_product:")
