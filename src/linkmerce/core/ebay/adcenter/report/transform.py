from linkmerce.common.transform import DuckDBTransformer
from linkmerce.core.ebay.adcenter import GmarketAdParser


class Report(DuckDBTransformer):
    """Gmarket 광고센터 일별/상품별 상세 리포트를 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `Report`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        - `GmarketAdParser: str -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: ebay_adreport`
    """

    extractor = "Report"
    tables = {"table": "ebay_adreport"}
    parser = GmarketAdParser
    parser_config = dict(
        scope = "data",
        fields = [
            "date", "groupId", "campaignId", "itemId",
            "impressions", "clicks", "spend", "productOrders", "unitSold", "skuRevenue", "a2c"
        ]
    )


class ReportDownload(DuckDBTransformer):
    """Gmarket 광고센터 상세 리포트 엑셀 파일을 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `ReportDownload`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        - `ExcelTransformer: bytes -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: ebay_adreport_dl`
    """

    extractor = "ReportDownload"
    tables = {"table": "ebay_adreport_dl"}
    parser = "excel"
    parser_config = dict(
        header = 6,
        fields = [
            "날짜", "그룹명", "캠페인명", "상품번호", "노출 수", "클릭 수", "광고 비용",
            "광고 상품 전환 수", "광고 상품 전환 수량", "광고 상품 전환 금액", "광고 상품 장바구니에 담은 수량"
        ]
    )
