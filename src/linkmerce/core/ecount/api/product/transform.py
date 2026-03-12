from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class Product(DuckDBTransformer):
    tables = {"table": "ecount_inventory"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "Data.Result",
        fields = ["PROD_CD", "CONT4", "PROD_DES", "REMARKS_WIN", "CONT1", "SIZE_DES", "UNIT", "IN_PRICE", "CONT2", "CONT3"],
    )
