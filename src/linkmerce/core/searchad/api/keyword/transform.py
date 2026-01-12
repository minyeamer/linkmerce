from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject


class KeywordList(JsonTransformer):
    dtype = dict
    path = ["keywordList"]


class Keyword(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, max_rank: int | None = None, **kwargs):
        keywords = KeywordList().transform(obj)
        if keywords:
            if isinstance(max_rank, int):
                keywords = keywords[:max_rank]
            return self.insert_into_table(keywords)
