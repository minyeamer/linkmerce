from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class TimeContract(DuckDBTransformer):
    """네이버 검색광고의 브랜드검색 광고 계약기간 API 응답 데이터를 `searchad_contract` 테이블에 적재하는 클래스."""

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
    """네이버 검색광고의 신제품검색 광고 계약기간 API 응답 데이터를 `searchad_contract_new` 테이블에 적재하는 클래스."""

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
