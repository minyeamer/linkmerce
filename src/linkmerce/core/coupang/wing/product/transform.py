from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, ExcelTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.transform import JsonObject


class ProductParser(JsonTransformer):
    dtype = dict
    scope = "data.productList"
    fields = [
        "vendorInventoryId", "vendorInventoryItemId", "vendorItemId", "barcode",
        "vendorId", "productName", "itemName", "displayCategoryCode", "categoryId", "categoryName",
        "brand", "manufacture", "valid", "salePrice", "deliveryCharge", "viUnitSoldAgg",
        "stockQuantity", "createdOn", "modifiedOn"
    ]
    defaults = {"isDeleted": "$is_deleted"}
    on_missing = "raise"
    product_type: Literal["product", "option"] = "product"

    def parse(self, products: JsonObject, **kwargs) -> JsonObject:
        if self.product_type == "option":
            options = list()
            for product in products:
                if not isinstance(product, dict):
                    continue
                for option in (product.get("vendorInventoryItems") or list()):
                    if isinstance(option, dict):
                        options.append(dict(product, **option))
            return options
        return products


class ProductOption(DuckDBTransformer):
    tables = {"table": "coupang_product"}
    parser = ProductParser
    parser_config = {"product_type": "option"}


class ProductDetail(DuckDBTransformer):
    queries = ["create", "bulk_insert", "bulk_insert_vendor", "insert_rfm"]
    tables = {"table": "coupang_product_detail"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data",
        fields = [
            "vendorInventoryId", "vendorInventoryItemId", "vendorItemId", "productId", "itemId",
            "barcode", "itemName", "originalPrice", "salePrice", "stockQuantity"
        ],
        on_missing = "raise",
    )

    def bulk_insert(self, result: list[dict], referer: Literal["vendor","rfm"] | None = None, **kwargs):
        render, params, total = self.prepare_bulk_params(result, **kwargs)
        if total > 0:
            key = f"bulk_insert_{referer}" if referer else "bulk_insert"
            query = self.prepare_query(key=key, render=render)
            return self.execute(query, **params)


class VendorInventoryItemParser(ExcelTransformer):
    header = 3
    fields = [
        "등록상품ID", "Product ID", "옵션 ID", "바코드", "쿠팡 노출 상품명", "업체 등록 상품명",
        "등록 옵션명", "판매상태", "할인율기준가", "판매가격", "판매수량", "잔여수량(재고)"
    ]
    defaults = {"vendorId": "$vendor_id", "isDeleted": "$is_deleted"}
    on_missing = "raise"


class EditableCatalogueParser(ExcelTransformer):
    sheet_name = "Template"
    header = 4
    fields = ["등록상품ID", "등록상품명", "쿠팡 노출상품명", "카테고리", "제조사", "브랜드", "검색어", "성인상품여부(Y/N)"]
    defaults = {"vendorId": "$vendor_id", "isDeleted": "$is_deleted"}
    on_missing = "raise"

    def select_fields(self, data: list[dict], **kwargs) -> list[dict]:
        info = {field: None for field in self.fields}
        for i in range(len(data)):
            if i["등록상품ID"]:
                info = {field: data[i].get(field) for field in self.fields}
            else:
                data[i].update(info)
        return data


class ProductDownload(DuckDBTransformer):
    tables = {"table": "coupang_product_download"}

    def parse(self, obj: JsonObject, request_type = "VENDOR_INVENTORY_ITEM", **kwargs) -> list[dict]:
        if request_type != "VENDOR_INVENTORY_ITEM":
            self.raise_parse_error(f"Parsing for request type '{request_type}' is not supported.")

        config = self.parser_config or dict()
        return VendorInventoryItemParser(**config).transform(obj, **kwargs)


class RocketInventory(DuckDBTransformer):
    tables = {"table": "coupang_rocket_inventory"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "viProperties",
        fields = {
            ".": ["vendorItemId", "gmvForLast7Days", "gmvForLast30Days", "unitsSoldForLast7Days", "unitsSoldForLast30Days"],
            "listingDetails": ["vendorInventoryId", "productId"],
            "creturnConfigViewDto": ["vendorInventoryItemId", "externalSkuId", "vendorId"],
            "inventoryDetails": [
                "orderableQuantity", "inProgressInboundStatistics.inProgressInboundQuantity",
                "daysOfCover", "storageFee.monthlyStorageFeeAmount.amount"
            ]
        },
        defaults = {"vendorId": "$vendor_id"},
        on_missing = "ignore",
    )


class RocketOption(DuckDBTransformer):
    tables = {"table": "coupang_rocket_option"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "viProperties",
        fields = {
            ".": ["vendorItemId", "unitsSoldForLast30Days"],
            "listingDetails": ["vendorInventoryId", "productId", "vendorInventoryName", "productRegistrationDate"],
            "creturnConfigViewDto": [
                "vendorInventoryItemId", "itemId", "externalSkuId", "vendorId", "productName", "itemName",
                "displayCategoryCodeLevel1", "displayCategoryCodeLevel2", "displayCategoryCodeLevel3",
                "displayCategoryCodeLevel4", "displayCategoryCodeLevel5", "onSale"
            ],
            "creturnConfigViewDto.creturnCategoryLevelThresholdDto": ["categoryId", "kanNameEn"],
            "inventoryDetails": ["isHiddenByVendor", "orderableQuantity"],
            "pricing": ["salesPrice.amount"]
        },
        defaults = {"vendorId": "$vendor_id"},
        on_missing = "ignore",
    )
