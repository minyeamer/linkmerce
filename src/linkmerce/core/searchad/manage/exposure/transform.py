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

    def set_tables(self, tables: dict | None = None):
        base = dict(rank="naver_rank_ad", product="naver_product")
        super().set_tables(dict(base, **(tables or dict())))

    def create_table(self, **kwargs):
        super().create_table(key="create_rank", table=":rank:")
        super().create_table(key="create_product", table=":product:")

    def insert_into_table(self, obj: list[dict], params: dict = dict(), **kwargs):
        obj = self.reparse_object(obj)
        rank_params, product_params = self.split_params(**params)
        super().insert_into_table(obj, key="insert_rank", table=":rank:", values=":select_rank:", params=rank_params)
        super().insert_into_table(obj, key="upsert_product", table=":product:", values=":select_product:", params=product_params)

    def reparse_object(self, obj: list[dict]) -> list[dict]:
        obj[0] = dict(obj[0], lowPrice=obj[0].get("lowPrice", None), mobileLowPrice=obj[0].get("mobileLowPrice", None))
        return obj

    def split_params(self, keyword: str, is_own: bool | None = None, **kwargs) -> tuple[dict,dict]:
        return dict(keyword=keyword, is_own=is_own), dict(is_own=is_own)
