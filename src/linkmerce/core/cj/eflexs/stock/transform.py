from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class Stock(DuckDBTransformer):
    """CJ eFLEXs 상세 재고현황을 `eflexs_stock` 테이블에 변환 및 적재하는 클래스."""

    extractor = "Stock"
    tables = {"table": "eflexs_stock"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "dsRealTime",
        fields = [
            "itemCd", "itemVarcode", "strrId", "strrNm", "itemNm", "whCd", "whNm", "zoneCd", "wcellNm",
            "lotNo", "invnQty", "avlbQty", "hldQty", "prcsQty", "remainInvnDays",
            "validDatetime", "inbDate",
        ],
    )
