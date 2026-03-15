from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class Keyword(DuckDBTransformer):
    """네이버 검색광고 키워드 도구의 연관키워드 조회 결과를 `searchad_keyword` 테이블에 적재하는 클래스."""

    tables = {"table": "searchad_keyword"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "keywordList",
        fields = [
            "relKeyword", "monthlyPcQcCnt", "monthlyMobileQcCnt",
            "monthlyAvePcClkCnt", "monthlyAveMobileClkCnt", "compIdx", "plAvgDepth"
        ],
    )

    def parse(self, obj: list[dict], max_rank: int | None = None, **kwargs) -> list[dict]:
        """`max_rank`가 지정된 경우 상위 `max_rank`개의 키워드만 잘라내 반환한다."""
        keyword_list = super().parse(obj, **kwargs)
        return keyword_list[:max_rank] if isinstance(max_rank, int) else keyword_list
