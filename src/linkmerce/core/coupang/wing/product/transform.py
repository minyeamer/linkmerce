from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.transform import JsonObject


class ProductList(JsonTransformer):
    dtype = dict
    path = ["data","productList"]

    def transform(self, obj: JsonObject, by: Literal["product","option"] = "product", **kwargs) -> JsonObject:
        products = self.parse(obj)
        if by == "option":
            options = list()
            for product in products:
                if isinstance(product, dict):
                    for option in product.get("vendorInventoryItems", [dict()]):
                        if isinstance(option, dict):
                            options.append(dict(product, **option))
            return options
        else:
            return product


class ProductOption(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, is_deleted: bool = False, **kwargs):
        options = ProductList().transform(obj, by="option")
        if options:
            return self.insert_into_table(options, params=dict(is_deleted=is_deleted))


class OptionList(JsonTransformer):
    dtype = dict
    path = ["data"]


class ProductDetail(DuckDBTransformer):
    queries = ["create", "select", "insert", "insert_vendor", "insert_rfm"]

    def transform(self, obj: JsonObject, referer: Literal["vendor","rfm"] | None = None, **kwargs):
        options = OptionList().transform(obj)
        if options:
            return self.insert_into_table(options, key=(f"insert_{referer}" if referer else "insert"))


class ProductDownload(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(
            self,
            obj: JsonObject,
            request_type = "VENDOR_INVENTORY_ITEM",
            is_deleted: bool = False,
            vendor_id: str | None = None,
            **kwargs
        ):
        from linkmerce.utils.excel import excel2json
        sheet_name = "Template" if request_type == "EDITABLE_CATALOGUE" else None # data
        header = 4 if request_type == "EDITABLE_CATALOGUE" else 3
        report = excel2json(obj, sheet_name=sheet_name, header=header, warnings=False)
        if (request_type == "VENDOR_INVENTORY_ITEM") and report:
            # if request_type == "EDITABLE_CATALOGUE":
            #     self.fillna(report)
            return self.insert_into_table(report, params=dict(is_deleted=is_deleted, vendor_id=vendor_id))

    def fillna(self, report: list[dict]):
        info = {field: None for field in self.product_fields}
        for i in range(len(report)):
            if i["등록상품ID"]:
                info = {field: report[i].get(field) for field in self.product_fields}
            else:
                report[i].update(info)

    @property
    def product_fields(self) -> list[str]:
        return ["등록상품ID", "등록상품명", "쿠팡 노출상품명", "카테고리", "제조사", "브랜드", "검색어", "성인상품여부(Y/N)"]


class RocketInventorylist(JsonTransformer):
    dtype = dict
    path = ["viProperties"]

    def transform(self, obj: JsonObject, **kwargs) -> JsonObject:
        items = self.parse(obj)
        for item in (items or list()):
            if isinstance(item, dict):
                item.pop("productRecommendations", None)
                config = item.get("creturnConfigViewDto")
                if isinstance(config, dict):
                    config.pop("vendorItemCustomerReturnMonthlyInsights", None)
        return items


class RocketInventory(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, vendor_id: str | None = None, **kwargs):
        items = RocketInventorylist().transform(obj)
        if items:
            return self.insert_into_table(items, params=dict(vendor_id=vendor_id))


class RocketOption(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, vendor_id: str | None = None, **kwargs):
        items = RocketInventorylist().transform(obj)
        print(items[0])
        if items:
            return self.insert_into_table(items, params=dict(vendor_id=vendor_id))
