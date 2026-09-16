from linkmerce.common.transform import DuckDBTransformer


class AiReport(DuckDBTransformer):
    """AUCTION 광고센터 AI매출업 상품별 리포트를 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `AiReport`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: auction_adreport_ai`
    """

    extractor = "AiReport"
    tables = {"table": "auction_adreport_ai"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data.List",
        fields = [
            "SellerID", "SiteGoodsNo", "SumClickCnt", "SumExpense",
            "ItemSumConvertCnt", "ItemSumConvertAmnt", "ItemSumOrderQty"
        ],
    )
    params = {"end_date": "$end_date"}


class CpcReport(DuckDBTransformer):
    """AUCTION 광고센터 파워클릭 상품별 리포트를 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `CpcReport`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: auction_adreport_cpc`
    """

    extractor = "CpcReport"
    tables = {"table": "auction_adreport_cpc"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "searchReport",
        fields = [
            "siteID", "siteGoodsNo", "exposeCnt", "clickCnt", "sumClickExpense",
            "sumExposeRank", "orderCnt", "orderAmnt"
        ],
    )
    params = {"end_date": "$end_date"}
