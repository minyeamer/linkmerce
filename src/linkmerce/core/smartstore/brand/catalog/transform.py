from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


class CatalogItems(JsonTransformer):
    """네이버 브랜드 카탈로그 목록을 추출하는 파서 클래스."""

    dtype = dict
    scope = "items"

    def assert_valid_response(self, obj: dict, **kwargs):
        """`errors` 필드가 있으면 `RequestError`를 발생시킨다."""
        super().assert_valid_response(obj)
        if obj.get("errors"):
            from linkmerce.utils.nested import hier_get
            msg = hier_get(obj, "errors.0.message") or "null"
            self.raise_request_error(f"An error occurred during the request: {msg}")


class BrandCatalog(DuckDBTransformer):
    """네이버 브랜드 카탈로그 목록을 `naver_brand_catalog` 테이블에 적재하는 클래스."""

    extractor = "BrandCatalog"
    tables = {"table": "naver_brand_catalog"}
    parser = CatalogItems
    parser_config = dict(
        fields = [
            "identifier", "prodName", "makerSeq", "makerName", "brandSeq", "brandName",
            {"category": ["identifier", "name", "fullId", "fullName"]}, "image.src",
            "officialAuthLowestPriceRatio.lowestPrice",
            "officialAuthLowestPriceRatioWithFee.lowestPrice",
            "lowestPrice", "allLowestPriceWithFee.lowestPrice",
            "productCount", "totalReviewCount", "reviewRating", "registerDate"
        ],
    )


class BrandProduct(DuckDBTransformer):
    """네이버 브랜드 상품 목록을 `naver_brand_product` 테이블에 적재하는 클래스."""

    extractor = "BrandProduct"
    tables = {"table": "naver_brand_product"}
    parser = CatalogItems
    parser_config = dict(
        fields = [
            "identifier", "mallProductId", "catalogId", "name", "makerSeq", "makerName",
            "brandSeq", "brandName", "mallName", "categoryId", "categoryName",
            "fullCategoryId", "fullCategoryName", "outLinkUrl", "imageInfo.src",
            "lowestPrice", "deliveryFee", "clickCount", "totalReviewCount", "registerDate"
        ],
    )
    params = {"mall_seq": "$mall_seq"}


class BrandPrice(BrandProduct):
    """네이버 브랜드 상품 조회 결과로부터 상품 가격 및 상품 목록을 각각의 테이블에 변환 및 적재하는 클래스.

    테이블 키 | 테이블명 | 설명
    - `price` | `naver_price_history` | 네이버 브랜드 상품 가격
    - `product` | `naver_product` | 네이버 브랜드 상품 목록"""

    extractor = "BrandProduct"
    tables = {"price": "naver_price_history", "product": "naver_product"}
    parser = CatalogItems
    parser_config = dict(
        fields = ["mallProductId", "categoryId", "fullCategoryId", "lowestPrice", "registerDate"],
    )
    params = {"mall_seq": "$mall_seq"}


class ProductCatalog(BrandProduct):
    """네이버 카탈로그-상품 매핑 데이터를 `naver_catalog_product` 테이블에 적재하는 클래스."""

    extractor = "BrandProduct"
    tables = {"table": "naver_catalog_product"}
    parser = CatalogItems
    parser_config = dict(
        fields = ["mallProductId", "catalogId"],
    )
