from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


class CatalogItems(JsonTransformer):
    dtype = dict
    scope = "items"

    def assert_valid_response(self, obj: dict, **kwargs):
        super().assert_valid_response(obj)
        if obj.get("errors"):
            from linkmerce.utils.nested import hier_get
            msg = hier_get(obj, "errors.0.message") or "null"
            self.raise_request_error(f"An error occurred during the request: {msg}")


class BrandCatalog(DuckDBTransformer):
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
    tables = {"product": "naver_brand_product"}
    parser = CatalogItems
    parser_config = dict(
        fields = [
            "identifier", "mallProductId", "catalogId", "name", "makerSeq", "makerName",
            "brandSeq", "brandName", "mallName", "categoryId", "categoryName",
            "fullCategoryId", "fullCategoryName", "outLinkUrl", "imageInfo.src",
            "lowestPrice", "deliveryFee", "clickCount", "totalReviewCount", "registerDate"
        ],
        defaults = {"mallSeq": "$mall_seq"},
    )


class BrandPrice(BrandProduct):
    tables = {"price": "naver_price_history", "product": "naver_product"}
    parser = CatalogItems
    parser_config = dict(
        fields = ["mallProductId", "categoryId", "fullCategoryId", "lowestPrice", "registerDate"],
        defaults = {"mallSeq": "$mall_seq"},
    )


class ProductCatalog(BrandProduct):
    tables = {"product": "naver_catalog_product"}
    parser = CatalogItems
    parser_config = dict(
        fields = ["mallProductId", "catalogId"],
    )
