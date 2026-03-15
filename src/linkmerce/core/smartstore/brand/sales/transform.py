from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


class SalesParser(JsonTransformer):
    """네이버 스토어 일별 매출 데이터를 추출하는 파서 클래스."""

    dtype = dict

    def assert_valid_response(self, obj: dict, **kwargs):
        """`error` 필드가 있으면 `UnauthorizedError` 또는 `RequestError`를 발생시킨다."""
        super().assert_valid_response(obj)
        if "error" in obj:
            from linkmerce.utils.nested import hier_get
            msg = hier_get(obj, "error.error") or "null"
            if msg == "Unauthorized":
                from linkmerce.common.exceptions import UnauthorizedError
                raise UnauthorizedError("Unauthorized request")
            super().raise_request_error(f"An error occurred during the request: {msg}")


class StoreSales(DuckDBTransformer):
    """네이버 스토어 일별 매출 데이터를 `naver_store_sales` 테이블에 적재하는 클래스."""

    tables = {"table": "naver_store_sales"}
    parser = SalesParser
    parser_config = dict(
        scope = "data.storeSales",
        fields = {"sales": ["paymentCount", "paymentAmount", "refundAmount"]},
    )
    params = {"mall_seq": "$mall_seq", "end_date": "$end_date"}


class CategorySales(DuckDBTransformer):
    """네이버 스토어 일별/카테고리별 매출 데이터를 `naver_category_sales` 테이블에 적재하는 클래스."""

    tables = {"table": "naver_category_sales"}
    parser = SalesParser
    parser_config = dict(
        scope = "data.categorySales",
        fields = {
            "product.category": ["identifier", "fullName"],
            "visit": ["click"],
            "sales": ["paymentCount", "paymentAmount"]
        },
    )
    params = {"mall_seq": "$mall_seq", "end_date": "$end_date"}


class ProductSales(DuckDBTransformer):
    """네이버 스토어 일별/상품별 매출 데이터를 `naver_product_sales` 테이블에 적재하는 클래스."""

    tables = {"table": "naver_product_sales"}
    parser = SalesParser
    parser_config = dict(
        scope = "data.productSales",
        fields = {
            "product": ["identifier", "name"],
            "product.category": ["identifier", "name", "fullName"],
            "visit": ["click"],
            "sales": ["paymentCount", "paymentAmount"]
        },
    )
    params = {"mall_seq": "$mall_seq", "end_date": "$end_date"}


class AggregatedSales(ProductSales):
    """네이버 스토어 일별/상품별 매출 데이터로부터 상품별 매출 및 상품 목록을 각각의 테이블에 변환 및 적재하는 클래스.

    테이블 키 | 테이블명 | 설명
    - `sales` | `naver_sales` | 네이버 스토어 상품별 매출
    - `product` | `naver_product` | 네이버 스토어 상품 목록"""

    tables = {"sales": "naver_sales", "product": "naver_product"}
    params = {"mall_seq": "$mall_seq", "start_date": "$start_date", "end_date": "$end_date"}
