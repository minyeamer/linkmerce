from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class Inventory(DuckDBTransformer):
    tables = {"table": "ecount_inventory"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "Data.Result",
        fields = ["PROD_CD", "BAL_QTY"],
    )
