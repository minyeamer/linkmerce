from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class TimeContract(DuckDBTransformer):
    """브랜드검색 광고 계약기간 데이터를 변환 및 적재하는 클래스.

    - **Extractor**: `TimeContract`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: list -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: searchad_contract`
    """

    extractor = "TimeContract"
    tables = {"table": "searchad_contract"}
    parser = "json"
    parser_config = dict(
        dtype = list,
        fields = [
            "nccTimeContractId", "nccAdgroupId", "adgroupName", "customerId", "contractName",
            "contractStatus", "paymentAmt", "refundAmt", "contractQc", "totalKeywordQc",
            "regTm", "editTm", "contractStartDt", "contractEndDt",
            "exposureStartDt", "exposureEndDt", "cancelTm"
        ],
    )


class BrandNewContract(DuckDBTransformer):
    """신제품검색 광고 계약기간 데이터를 변환 및 적재하는 클래스.

    - **Extractor**: `BrandNewContract`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: list -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: searchad_contract_new`
    """

    extractor = "BrandNewContract"
    tables = {"table": "searchad_contract_new"}
    parser = "json"
    parser_config = dict(
        dtype = list,
        fields = [
            "brandNewContractId", "nccAdgroupId", "adgroupName", "customerId", "contractName",
            "contractStatus", "keywordGroupCategoryName", "biddingRound", "bidAmt", "paymentAmt",
            "refundAmt", "regTm", "editTm", "contractStartDt", "contractEndDt",
            "exposureStartDt", "exposureEndDt", "winningBidDt", "cancelTm"
        ],
    )
