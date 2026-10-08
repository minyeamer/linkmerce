from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


class Product(DuckDBTransformer):
    """쿠팡 마켓플레이스 또는 로켓그로스 상품 목록을 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `Product`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: coupang_product`
    """

    extractor = "Product"
    tables = {"table": "coupang_product"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data",
        fields = [
            "sellerProductId", "sellerProductName", "displayCategoryCode", "categoryId",
            "productId", "vendorId", "saleStartedAt", "saleEndedAt", "brand", "statusName",
            "createdAt"
        ],
    )


class ProducItemParser(JsonTransformer):
    """로켓그로스 상품 목록을 마켓플레이스 및 로켓그로스 옵션 목록으로 변환하는 클래스."""

    dtype = dict
    scope = "data"
    fields = [
        "sellerProductId", "sellerProductName", "displayCategoryCode", "categoryId",
        "productId", "vendorId", "saleStartedAt", "saleEndedAt", "brand", "statusName",
        "createdAt", "businessType", {"item": ["sellerProductItemId", "vendorItemId", "itemName"]}
    ]

    def parse(self, obj: list[dict], **kwargs) -> list[dict]:
        """목록 응답의 각 `items`에서 판매 방식별 옵션 ID를 추출한다."""
        items = list()
        default_item = {"sellerProductItemId": 0, "vendorItemId": None, "itemName": None}

        for product in obj:
            has_item = False

            for item in (product.get("items") or list()):
                if not isinstance(item, dict):
                    continue

                for business_type, key in enumerate(("marketPlaceItemData", "rocketGrowthItemData")):
                    if isinstance(data := item.get(key), dict) and ("sellerProductItemId" in data):
                        items.append(product | {
                            "item": item | data,
                            "items": None,
                            "businessType": business_type
                        })

            if not has_item:
                items.append(product | {"item": default_item, "items": None, "businessType": 0})

        return items


class ProductItem(DuckDBTransformer):
    """쿠팡 마켓플레이스 또는 로켓그로스 상품의 옵션 목록을 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `Product`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `ProducItemParser: bytes -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: coupang_product`
    """

    extractor = "Product"
    tables = {"table": "coupang_product_item"}
    parser = ProducItemParser


class DetailedItemParser(JsonTransformer):
    """쿠팡 단일 상품에 대한 상세 정보를 마켓플레이스 및 로켓그로스 옵션 목록으로 변환하는 클래스."""

    dtype = dict
    scope = "data"
    fields = [
        "sellerProductId", "sellerProductName", "displayCategoryCode", {"categoryId": None},
        {"productId": None}, "vendorId", "saleStartedAt", "saleEndedAt", "displayProductName",
        "brand", "statusName", {"deliveryCharge": None}, "manufacture", "businessType",
        {"item": [
            "sellerProductItemId", "vendorItemId", "itemId", "itemName",
            "originalPrice", "salePrice", "barcode", "images.0.cdnPath"]}
    ]

    def parse(self, obj: dict, **kwargs) -> list[dict]:
        """상품 정보 내부의 옵션 목록 `items`를 평탄화해 상폼-옵션 목록으로 반환한다."""
        items = list()

        if isinstance(ship_info := obj.pop("marketplaceShippingAndReturnInfo", None), dict):
            obj = obj | ship_info

        for item in obj["items"]:
            if not isinstance(item, dict):
                continue

            has_data = False
            for business_type, key in enumerate(("marketPlaceItemData", "rocketGrowthItemData")):
                if isinstance(data := item.get(key), dict) and ("sellerProductItemId" in data):
                    has_data = True
                    items.append(obj | {
                        "item": item | data | (data.get("priceData") or dict()),
                        "items": None,
                        "businessType": business_type
                    })

            if (not has_data) and ("sellerProductItemId" in item):
                items.append(obj | {"item": item, "items": None, "businessType": 0})

        return items


class ProductDetail(DuckDBTransformer):
    """쿠팡 마켓플레이스 또는 로켓그로스 상품의 상세 정보를 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `ProductDetail`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `DetailedItemParser: bytes -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: coupang_product_detail`
    """

    extractor = "ProductDetail"
    tables = {"table": "coupang_product_detail"}
    parser = DetailedItemParser


class Inventory(DuckDBTransformer):
    """쿠팡 옵션별 수량, 가격, 판매상태를 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `Inventory`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: coupang_inventory`

    Parameters
    ----------
    vendor_id: str
        업체 코드
    """

    extractor = "Inventory"
    tables = {"table": "coupang_inventory"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data",
        fields = ["sellerItemId", "amountInStock", "salePrice", "onSale"],
    )
    params = {"vendor_id": "$vendor_id"}
