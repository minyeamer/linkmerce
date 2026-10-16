from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class Invoice(DuckDBTransformer):
    """CJ대한통운 eFLEXs 운송장 상세 내역을 변환 및 적재하는 클래스.

    - **Extractor**: `Invoice`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: cj_eflexs_invoice`
    """

    extractor = "Invoice"
    tables = {"table": "cj_eflexs_invoice"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "payload.tab03",
        fields = [
            "hawbNo", "refOrdNo", "custOrdNo", "repItemNm", "dtldlvNm", "ctryNm", "boxTypeNm",
            "sbsBoxType", "skuCnt", "totalQty", "outbAmt", "feryAmt", "jejuAmt", "etcAmt",
            "svcAmt", "smTotlAmt", "billDate",
        ],
    )


class InvoiceOrder(Invoice):
    """CJ대한통운 eFLEXs B2C출고(운송장)상세 내역에서 주문별 정산 내역을 변환 및 적재하는 클래스.

    - **Extractor**: `Invoice`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: cj_eflexs_invoice_order`
    """

    extractor = "Invoice"
    tables = {"table": "cj_eflexs_invoice_order"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "payload.tab03",
        fields = [
            "hawbNo", "refOrdNo", "custOrdNo", "outbAmt", "feryAmt", "jejuAmt", "etcAmt",
            "svcAmt", "smTotlAmt", "billDate"
        ],
    )
