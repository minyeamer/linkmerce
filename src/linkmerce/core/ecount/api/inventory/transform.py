from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class Inventory(DuckDBTransformer):
    """이카운트 재고관리 API 응답 데이터를 `ecount_inventory` 테이블에 적재하는 클래스."""

    extractor = "Inventory"
    tables = {"table": "ecount_inventory"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "Data.Result",
        fields = ["PROD_CD", "BAL_QTY"],
    )
