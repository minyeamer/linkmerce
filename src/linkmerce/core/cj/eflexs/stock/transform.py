from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class Stock(DuckDBTransformer):
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
