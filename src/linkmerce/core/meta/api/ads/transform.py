from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject


class AdObjects(JsonTransformer):
    dtype = dict
    path = ["data"]


class _AdTransformer(DuckDBTransformer):
    queries: list[str] = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, ad_account: str | None = None, **kwargs):
        results = AdObjects().transform(obj)
        if results:
            self.insert_into_table(results, params=dict(ad_account=ad_account))


class Campaigns(_AdTransformer):
    queries = ["create", "select", "insert"]


class Adsets(_AdTransformer):
    queries = ["create", "select", "insert"]


class Ads(_AdTransformer):
    queries = ["create", "select", "insert"]


class Insights(_AdTransformer):
    queries = ["create", "select", "insert"]
