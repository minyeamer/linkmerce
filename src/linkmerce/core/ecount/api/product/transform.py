from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class Product(DuckDBTransformer):
    """이카운트 품목관리 API 응답 데이터를 `ecount_product` 테이블에 적재하는 클래스."""

    tables = {"table": "ecount_product"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "Data.Result",
        fields = ["PROD_CD", "CONT4", "PROD_DES", "REMARKS_WIN", "CONT1", "SIZE_DES", "UNIT", "IN_PRICE", "CONT2", "CONT3"],
    )
