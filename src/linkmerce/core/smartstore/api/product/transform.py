from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


class ProductParser(JsonTransformer):
    scope = "contents"
    fields = [
        "channelProductNo", "originProductNo", "modelId", "channelServiceType", "name",
        "sellerManagementCode", "modelName", "brandName", "manufacturerName",
        "categoryId", "wholeCategoryId", "wholeCategoryName", "statusType",
        "channelProductDisplayStatusType", "representativeImage.url", "sellerTags",
        "salePrice", "discountedPrice", "stockQuantity",
        "deliveryAttributeType", "deliveryFee", "returnFee", "exchangeFee",
        "regDate", "modifiedDate", "groupProductNo"
    ]
    defaults = {"channelSeq": "$channel_seq"}

    def parse(self, contents: list[dict], **kwargs) -> list[dict]:
        products = list()
        for content in contents:
            for product in content["channelProducts"]:
                products.append(dict(product,
                    sellerTags=','.join([tag["text"] for tag in product["sellerTags"]])))
        return products


class Product(DuckDBTransformer):
    tables = {"table": "smartstore_product"}
    parser = ProductParser


class OpeionSimpleParser(JsonTransformer):
    scope = "originProduct.detailAttribute.optionInfo.optionSimple"
    fields = ["id", "groupName", "name", "usable", {"price": None}, {"stockQuantity": None}]


class OpeionCombParser(JsonTransformer):
    scope = "originProduct.detailAttribute.optionInfo"
    fields = [
        "id", "optionGroupName1", "optionName1", *[{key: None} for key in [
            "optionGroupName2", "optionName2", "optionGroupName3", "optionName3"]],
        {"sellerManagerCode": None}, "usable", "price", "stockQuantity"
    ]

    def parse(self, option_info: dict, **kwargs) -> list[dict]:
        options = list()
        option_groups = option_info.get("optionCombinationGroupNames") or dict()
        for option in (option_info.get("optionCombinations") or list()):
            if isinstance(option, dict):
                options.append(dict(option, **option_groups))
        return options


class SupplementParser(JsonTransformer):
    scope = "originProduct.detailAttribute.supplementProductInfo.supplementProducts"
    fields = [
        "id", "groupName", "name", {"sellerManagerCode": None}, "usable", "price", "stockQuantity"
    ]


class Option(DuckDBTransformer):
    tables = {"table": "smartstore_option"}
    parser = {
        "option_simple": OpeionSimpleParser,
        "option_comb": OpeionCombParser,
        "supplement": SupplementParser
    }
    parser_config = dict(
        defaults = {"productId": "$product_id", "channelSeq": "$channel_seq"},
    )
