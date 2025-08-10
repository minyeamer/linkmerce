from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import JsonObject
    from duckdb import DuckDBPyRelation


class ExposureDiagnosis(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, keyword: str, is_own: bool | None = None, **kwargs):
        if isinstance(obj, dict):
            if not obj.get("code"):
                params = dict(keyword=keyword, is_own=is_own)
                return self.insert_into_table(obj["adList"], params=params) if obj["adList"] else None
            else:
                self.raise_request_error(obj)
        else:
            self.raise_parse_error()

    def raise_request_error(self, obj: JsonObject):
        msg = obj.get("title") or obj.get("message") or str()
        if (msg == "Forbidden") or ("권한이 없습니다." in msg) or ("인증이 만료됐습니다." in msg):
            from linkmerce.common.exceptions import UnauthorizedError
            raise UnauthorizedError(msg)
        else:
            super().raise_request_error(msg)


class ExposureRank(ExposureDiagnosis):
    queries = ["create_rank", "select_rank", "insert_rank", "create_product", "select_product", "upsert_product"]

    def create_table(
            self,
            rank_table: str = ":default:",
            product_table: str = "product",
            **kwargs
        ) -> tuple[DuckDBPyRelation,DuckDBPyRelation]:
        rank = super().create_table(key="create_rank", table=rank_table)
        product = super().create_table(key="create_product", table=product_table)
        return rank, product

    def insert_into_table(
            self,
            obj: list,
            rank_table: str = ":default:",
            product_table: str = "product",
            params: dict = dict(),
            **kwargs
        ) -> tuple[DuckDBPyRelation,DuckDBPyRelation]:
        def reparse_object(obj: list[dict]) -> list[dict]:
            obj[0] = dict(obj[0], lowPrice=obj[0].get("lowPrice", None), mobileLowPrice=obj[0].get("mobileLowPrice", None))
            return obj
        def split_params(keyword: str, is_own: bool | None = None, **kwargs) -> tuple[dict,dict]:
            return dict(keyword=keyword, is_own=is_own), dict(is_own=is_own)
        obj = reparse_object(obj)
        rank_params, product_params = split_params(**params)
        rank = super().insert_into_table(obj, key="insert_rank", table=rank_table, values=":select_rank:", params=rank_params)
        product = super().insert_into_table(obj, key="upsert_product", table=product_table, values=":select_product:", params=product_params)
        return rank, product
