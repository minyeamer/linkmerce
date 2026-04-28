from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class Product(DuckDBTransformer):
    """이카운트 품목등록 리스트를 변환 및 적재하는 클래스.

    - **Extractor**: `Product`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: ecount_product`
    """

    extractor = "Product"
    tables = {"table": "ecount_product"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "Data.Result",
        fields = [
            "PROD_CD", "CONT4", "PROD_DES", "REMARKS_WIN", "CONT1", "SIZE_DES",
            "UNIT", "IN_PRICE", "CONT2", "CONT3"
        ],
    )
