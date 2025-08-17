from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject


class AdList(JsonTransformer):
    dtype = dict
    path = ["adList"]

    def is_valid_response(self, obj: dict) -> bool:
        if obj.get("code"):
            self.raise_request_error(obj.get("title") or obj.get("message") or str())
        return True


class ExposureDiagnosis(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, keyword: str, is_own: bool | None = None, **kwargs):
        ads = AdList().transform(obj)
        if ads:
            params = dict(keyword=keyword, is_own=is_own)
            return self.insert_into_table(ads, params=params)


class ExposureRank(ExposureDiagnosis):
    queries = ["create_rank", "select_rank", "insert_rank", "create_product", "select_product", "upsert_product"]

    def create_table(
            self,
            rank_table: str = ":default:",
            product_table: str = "product",
            **kwargs
        ):
        super().create_table(key="create_rank", table=rank_table)
        super().create_table(key="create_product", table=product_table)

    def insert_into_table(
            self,
            obj: list[dict],
            rank_table: str = ":default:",
            product_table: str = "product",
            params: dict = dict(),
            **kwargs
        ):
        def reparse_object(obj: list[dict]) -> list[dict]:
            obj[0] = dict(obj[0], lowPrice=obj[0].get("lowPrice", None), mobileLowPrice=obj[0].get("mobileLowPrice", None))
            return obj

        def split_params(keyword: str, is_own: bool | None = None, **kwargs) -> tuple[dict,dict]:
            return dict(keyword=keyword, is_own=is_own), dict(is_own=is_own)

        obj = reparse_object(obj)
        rank_params, product_params = split_params(**params)
        super().insert_into_table(obj, key="insert_rank", table=rank_table, values=":select_rank:", params=rank_params)
        super().insert_into_table(obj, key="upsert_product", table=product_table, values=":select_product:", params=product_params)
