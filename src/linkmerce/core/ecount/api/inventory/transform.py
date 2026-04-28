from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class Inventory(DuckDBTransformer):
    """이카운트 재고현황을 변환 및 적재하는 클래스.

    - **Extractor**: `Inventory`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: ecount_inventory`
    """

    extractor = "Inventory"
    tables = {"table": "ecount_inventory"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "Data.Result",
        fields = ["PROD_CD", "BAL_QTY"],
    )
