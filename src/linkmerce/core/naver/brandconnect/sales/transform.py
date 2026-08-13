from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class SalesPerformances(DuckDBTransformer):
    """네이버 쇼핑 커넥트 상품별 판매 실적을 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**
        - `SalesPerformances`

    - **Parser**
        - `JsonTransformer: dict -> list[dict]`

    - **Tables** ( *table_key: table_name (description)* ):
        1. `sales: naver_connect_sales` (판매 실적)
        2. `product: naver_connect_product` (상품 목록)

    Parameters
    ----------
    space_id: int | str
        브랜드 커넥트 스페이스 ID
    end_date: date | str
        조회 종료일
    """

    extractor = "SalesPerformances"
    tables = {"sales": "naver_connect_sales", "product": "naver_connect_product"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data",
        fields = [
            "smartStoreProductId", "productName", "productImageUrl",
            "salesCnt", "salesAmount", "commissionAmount",
        ],
    )
    params = {"space_id": "$space_id", "end_date": "$end_date"}
