from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject


class KeywordList(JsonTransformer):
    dtype = dict
    scope = "keywordList"
    fields = [
        "relKeyword", "monthlyPcQcCnt", "monthlyMobileQcCnt",
        "monthlyAvePcClkCnt", "monthlyAveMobileClkCnt", "compIdx", "plAvgDepth",
    ]


class Keyword(DuckDBTransformer):
    tables = {"table": "searchad_keyword"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "keywordList",
        fields = [
            "relKeyword", "monthlyPcQcCnt", "monthlyMobileQcCnt",
            "monthlyAvePcClkCnt", "monthlyAveMobileClkCnt", "compIdx", "plAvgDepth",
        ],
    )

    def parse(self, obj: JsonObject, max_rank: int | None = None, **kwargs) -> list[dict]:
        keyword_list = super().parse(obj, **kwargs)
        return keyword_list[:max_rank] if isinstance(max_rank, int) else keyword_list
