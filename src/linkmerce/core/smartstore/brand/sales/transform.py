from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


class SalesParser(JsonTransformer):
    dtype = dict

    def assert_valid_response(self, obj: dict, **kwargs):
        super().assert_valid_response(obj)
        if "error" in obj:
            from linkmerce.utils.nested import hier_get
            msg = hier_get(obj, ["error","error"]) or "null"
            if msg == "Unauthorized":
                from linkmerce.common.exceptions import UnauthorizedError
                raise UnauthorizedError("Unauthorized request")
            super().raise_request_error(f"An error occurred during the request: {msg}")


class StoreSales(DuckDBTransformer):
    tables = {"table": "naver_store_sales"}
    parser = SalesParser
    parser_config = dict(
        scope = "data.storeSales",
        fields = {"sales": ["paymentCount", "paymentAmount", "refundAmount"]},
        defaults = {"mallSeq": "$mall_seq", "startDate": "$start_date", "endDate": "$end_date"}
    )


class CategorySales(DuckDBTransformer):
    tables = {"table": "naver_category_sales"}
    parser = SalesParser
    parser_config = dict(
        scope = "data.categorySales",
        fields = {
            "product.category": ["identifier", "fullName"],
            "visit": ["click"],
            "sales": ["paymentCount", "paymentAmount"]
        },
        defaults = {"mallSeq": "$mall_seq", "startDate": "$start_date", "endDate": "$end_date"}
    )


class ProductSales(DuckDBTransformer):
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
        defaults = {"mallSeq": "$mall_seq", "startDate": "$start_date", "endDate": "$end_date"}
    )


class AggregatedSales(ProductSales):
    tables = {"sales": "naver_sales", "product": "naver_product"}
