from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, ExcelTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal


class ProductParser(JsonTransformer):
    """쿠팡 상품 목록을 추출하는 파서 클래스.

    `product_type = "option"` 설정 시 상품에 중첩된 옵션을 평탄화해 반환한다."""

    dtype = dict
    scope = "data.productList"
    fields = [
        "vendorInventoryId", "vendorInventoryItemId", "vendorItemId", "barcode",
        "vendorId", "productName", "itemName", "displayCategoryCode", "categoryId", "categoryName",
        "brand", "manufacture", "valid", "salePrice", "deliveryCharge", "viUnitSoldAgg",
        "stockQuantity", "createdOn", "modifiedOn"
    ]
    product_type: Literal["product", "option"] = "product"

    def parse(self, obj: list[dict], **kwargs) -> list[dict]:
        """`product_type = "option"` 시 상품마다 `vendorInventoryItems`를 평탄화해 옵션 목록을 반환한다."""
        if self.product_type == "option":
            options = list()
            for product in obj:
                if not isinstance(product, dict):
                    continue
                for option in (product.get("vendorInventoryItems") or list()):
                    if isinstance(option, dict):
                        options.append(dict(product, **option))
            return options
        return obj


class ProductOption(DuckDBTransformer):
    """쿠팡 상품 옵션 목록을 `coupang_product` 테이블에 적재하는 클래스."""

    tables = {"table": "coupang_product"}
    parser = ProductParser
    parser_config = {"product_type": "option"}
    params = {"is_deleted": "$is_deleted"}


class ProductDetail(DuckDBTransformer):
    """쿠팡 상품 상세 정보를 `coupang_product_detail` 테이블에 적재하는 클래스."""

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
    )

    def bulk_insert(
            self,
            result: list[dict],
            query_key: str = "bulk_insert",
            render: dict | Literal["tables"] | None = "tables",
            params: dict | None = None,
            referer: Literal["vendor", "rfm"] | None = None,
            **kwargs
        ) -> list:
        """`referer`가 전달되면 전용 `bulk_insert` 쿼리를 선택해 실행한다."""
        query_key = f"bulk_insert_{referer}" if referer else query_key
        return super().bulk_insert(result, query_key, render, params, **kwargs)


class VendorInventoryItemParser(ExcelTransformer):
    """쿠팡 상품의 가격/재고/판매상태 다운로드 결과를 파싱하는 클래스."""

    header = 3
    fields = [
        "등록상품ID", "Product ID", "옵션 ID", "바코드", "쿠팡 노출 상품명", "업체 등록 상품명",
        "등록 옵션명", "판매상태", "할인율기준가", "판매가격", "판매수량", "잔여수량(재고)"
    ]


class EditableCatalogueParser(ExcelTransformer):
    """쿠팡 쿠팡상품정보 다운로드 결과를 파싱하는 클래스."""

    sheet_name = "Template"
    header = 4
    fields = ["등록상품ID", "등록상품명", "쿠팡 노출상품명", "카테고리", "제조사", "브랜드", "검색어", "성인상품여부(Y/N)"]

    def select_fields(self, data: list[dict], **kwargs) -> list[dict]:
        """`등록상품ID` 값이 비어있는 행을 병합된 영역으로 인식하고 빈 셀에 이전 행 값을 채워 반환한다."""
        info = {field: None for field in self.fields}
        for i in range(len(data)):
            if data[i]["등록상품ID"]:
                info = {field: data[i].get(field) for field in self.fields}
            else:
                data[i].update(info)
        return data


class ProductDownload(DuckDBTransformer):
    """쿠팡 상품 다운로드 결과를 `coupang_product_download` 테이블에 적재하는 클래스.

    `request_type`에 따라 적절한 파서를 선택한다. `VENDOR_INVENTORY_ITEM`만 지원한다."""

    tables = {"table": "coupang_product_download"}
    params = {"vendor_id": "$vendor_id", "is_deleted": "$is_deleted"}

    def parse(self, obj: bytes, request_type = "VENDOR_INVENTORY_ITEM", **kwargs) -> list[dict]:
        """`request_type`에 따라 파서를 선택한다. `VENDOR_INVENTORY_ITEM`만 지원한다."""
        if request_type != "VENDOR_INVENTORY_ITEM":
            self.raise_parse_error(f"Parsing for request type '{request_type}' is not supported.")

        config = self.parser_config or dict()
        return VendorInventoryItemParser(**config).transform(obj, **kwargs)


class RocketInventory(DuckDBTransformer):
    """쿠팡 로켓 재고 내역을 `coupang_rocket_inventory` 테이블에 적재하는 클래스."""

    tables = {"table": "coupang_rocket_inventory"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "viProperties",
        fields = {
            ".": ["vendorItemId", "gmvForLast7Days", "gmvForLast30Days", "unitsSoldForLast7Days", "unitsSoldForLast30Days"],
            "listingDetails": ["vendorInventoryId", "productId"],
            "creturnConfigViewDto": [{key: None} for key in ["vendorInventoryItemId", "externalSkuId", "vendorId"]],
            "inventoryDetails": [
                "orderableQuantity", {"inProgressInboundStatistics.inProgressInboundQuantity": None},
                "daysOfCover", {"storageFee.monthlyStorageFeeAmount.amount": None}
            ]
        },
    )
    params = {"vendor_id": "$vendor_id"}


class RocketOption(DuckDBTransformer):
    """쿠팡 로켓 옵션 목록을 `coupang_rocket_option` 테이블에 적재하는 클래스."""

    tables = {"table": "coupang_rocket_option"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "viProperties",
        fields = {
            ".": ["vendorItemId", "unitsSoldForLast30Days"],
            "listingDetails": ["vendorInventoryId", "productId", "vendorInventoryName", "productRegistrationDate"],
            "creturnConfigViewDto": [{key: None} for key in [
                "vendorInventoryItemId", "itemId", "externalSkuId", "vendorId", "productName", "itemName",
                "displayCategoryCodeLevel1", "displayCategoryCodeLevel2", "displayCategoryCodeLevel3",
                "displayCategoryCodeLevel4", "displayCategoryCodeLevel5", "onSale",
                "creturnCategoryLevelThresholdDto.categoryId", "creturnCategoryLevelThresholdDto.kanNameEn"
            ]],
            "inventoryDetails": ["isHiddenByVendor", "orderableQuantity"],
            "pricing": ["salesPrice.amount"]
        },
    )
    params = {"vendor_id": "$vendor_id"}
