from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class StoreSales(DuckDBTransformer):
    """네이버 스토어의 일간 매출 데이터를 변환 및 적재하는 클래스.

    - **Extractor**: `StoreSales`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: naver_store_sales`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    mall_seq: int | str
        쇼핑몰 순번
    end_date: dt.date | str
        조회 종료일
    """

    extractor = "StoreSales"
    tables = {"table": "naver_store_sales"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data.storeSales",
        fields = {"sales": ["paymentCount", "paymentAmount", "refundAmount"]},
    )
    params = {"mall_seq": "$mall_seq", "end_date": "$end_date"}


class CategorySales(DuckDBTransformer):
    """네이버 스토어의 일간/카테고리별 매출 데이터를 변환 및 적재하는 클래스.

    - **Extractor**: `CategorySales`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: naver_category_sales`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    mall_seq: int | str
        쇼핑몰 순번
    end_date: dt.date | str
        조회 종료일
    """

    extractor = "CategorySales"
    tables = {"table": "naver_category_sales"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data.categorySales",
        fields = {
            "product.category": ["identifier", "fullName"],
            "visit": ["click"],
            "sales": ["paymentCount", "paymentAmount"]
        },
    )
    params = {"mall_seq": "$mall_seq", "end_date": "$end_date"}


class ProductSales(DuckDBTransformer):
    """네이버 스토어의 일간/상품별 매출 데이터를 변환 및 적재하는 클래스.

    - **Extractor**: `ProductSales`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: naver_product_sales`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    mall_seq: int | str
        쇼핑몰 순번
    end_date: dt.date | str
        조회 종료일
    """

    extractor = "ProductSales"
    tables = {"table": "naver_product_sales"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
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
    """네이버 스토어의 일간/상품별 매출 데이터를 변환 및 적재하는 클래스.

    **NOTE** 매출 데이터로부터 상품 매출과 상품 정보를 각각의 테이블로 분리한다.

    - **Extractor**: `ProductSales`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer`: `dict` -> `list[dict]`

    - **Table** ( *table_key: table_name (description)* ):
        1. `sales`: `naver_sales` (상품 매출)
        2. `product`: `naver_product` (상품 정보)

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    mall_seq: int | str
        쇼핑몰 순번
    start_date: dt.date | str
        조회 시작일
    end_date: dt.date | str
        조회 종료일
    """

    extractor = "ProductSales"
    tables = {"sales": "naver_sales", "product": "naver_product"}
    params = {"mall_seq": "$mall_seq", "start_date": "$start_date", "end_date": "$end_date"}
