from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class ProductAdReport(DuckDBTransformer):
    """쿠팡 광고센터 매출 성장 광고 보고서를 변환 및 적재하는 클래스.

    - **Extractor**: `ProductAdReport`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `ExcelTransformer: bytes -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: coupang_adreport_pa`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    vendor_id: str
        업체 코드
    """

    extractor = "ProductAdReport"
    tables = {"table": "coupang_adreport_pa"}
    parser = "excel"
    parser_config = dict(
        fields = [
            "캠페인 ID", "광고집행 옵션ID", "광고전환매출발생 옵션ID", "광고 노출 지면",
            "노출수", "클릭수", "광고비", "총 주문수(1일)", "직접 판매수량(1일)",
            "총 전환매출액(1일)", "직접 전환매출액(1일)", "날짜"
        ],
    )
    params = {"vendor_id": "$vendor_id"}


class NewCustomerAdReport(DuckDBTransformer):
    """쿠팡 광고센터 신규 구매 고객 확보 광고 보고서를 변환 및 적재하는 클래스.

    - **Extractor**: `NewCustomerAdReport`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `ExcelTransformer: bytes -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: coupang_adreport_nca`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    vendor_id: str
        업체 코드
    """

    extractor = "NewCustomerAdReport"
    tables = {"table": "coupang_adreport_nca"}
    parser = "excel"
    parser_config = dict(
        fields = [
            "캠페인 ID", "소재 ID", "소재", "광고집행 옵션 ID", "광고 노출 지면",
            "노출수", "클릭수", "집행 광고비", "참여수", "평균 재생 시간", "날짜"
        ],
    )
    params = {"vendor_id": "$vendor_id"}
