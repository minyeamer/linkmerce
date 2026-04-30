from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class BrandCatalog(DuckDBTransformer):
    """네이버 브랜드 카탈로그 목록을 변환 및 적재하는 클래스.

    - **Extractor**: `BrandCatalog`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: naver_brand_catalog`
    """

    extractor = "BrandCatalog"
    tables = {"table": "naver_brand_catalog"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "items",
        fields = [
            "identifier", "prodName", "makerSeq", "makerName", "brandSeq", "brandName",
            {"category": ["identifier", "name", "fullId", "fullName"]}, "imageInfo.src",
            "officialAuthLowestPriceRatio.lowestPrice",
            "officialAuthLowestPriceRatioWithFee.lowestPrice",
            "lowestPrice", "allLowestPriceWithFee.lowestPrice",
            "productCount", "totalReviewCount", "reviewRating", "registerDate"
        ],
    )


class BrandProduct(DuckDBTransformer):
    """네이버 브랜드 상품 목록을 변환 및 적재하는 클래스.

    - **Extractor**: `BrandProduct`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: naver_brand_product`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    mall_seq: int | str | None
        쇼핑몰 순번
    """

    extractor = "BrandProduct"
    tables = {"table": "naver_brand_product"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "items",
        fields = [
            "identifier", "mallProductId", "catalogId", "name", "makerSeq", "makerName",
            "brandSeq", "brandName", "mallName", "categoryId", "categoryName",
            "fullCategoryId", "fullCategoryName", "outLinkUrl", "imageInfo.src",
            "lowestPrice", "deliveryFee", "clickCount", "totalReviewCount", "registerDate"
        ],
    )
    params = {"mall_seq": "$mall_seq"}


class BrandPrice(BrandProduct):
    """네이버 브랜드 상품 목록에서 판매가 변동을 추적하기 위한 가격 정보를 추출 및 적재하는 클래스.

    - **Extractor**: `BrandProduct`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer`: `dict` -> `list[dict]`

    - **Table** ( *table_key: table_name (description)* ):
        1. `price`: `naver_price_history` (브랜드 상품 가격)
        2. `product`: `naver_product` (브랜드 상품 정보)

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    mall_seq: int | str | None
        쇼핑몰 순번
    """

    extractor = "BrandProduct"
    tables = {"price": "naver_price_history", "product": "naver_product"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "items",
        fields = ["mallProductId", "categoryId", "fullCategoryId", "name", "lowestPrice", "registerDate"],
    )
    params = {"mall_seq": "$mall_seq"}


class ProductCatalog(BrandProduct):
    """네이버 브랜드 상품 목록에서 카탈로그-상품 매핑 내역을 추출 및 적재하는 클래스.

    - **Extractor**: `BrandProduct`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer`: `dict` -> `list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table`: `naver_catalog_product`
    """

    extractor = "BrandProduct"
    tables = {"table": "naver_catalog_product"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "items",
        fields = ["mallProductId", "catalogId"],
    )
    params = None
