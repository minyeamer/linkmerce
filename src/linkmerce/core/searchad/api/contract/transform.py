from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class TimeContract(DuckDBTransformer):
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
