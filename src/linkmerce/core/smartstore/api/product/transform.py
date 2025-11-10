from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject


class ProductList(JsonTransformer):
    def transform(self, obj: JsonObject, **kwargs) -> list[dict]:
        try:
            return [product for content in obj["contents"] for product in content["channelProducts"]]
        except:
            self.raise_parse_error()


class Product(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, channel_seq: int | str | None = None, **kwargs):
        products = ProductList().transform(obj)
        if products:
            products[0] = self.validate_product(products[0])
            self.insert_into_table(products, params=dict(channel_seq=channel_seq))

    def validate_product(self, product: dict) -> dict:
        for key in ["groupProductNo", "manufacturerName", "modelName", "modelId", "sellerManagementCode"]:
            if key not in product:
                product[key] = None
        return product


class OptionList(JsonTransformer):
    path = ["originProduct", "detailAttribute", "optionInfo"]

    def transform(self, obj: JsonObject, **kwargs) -> dict[str,list[dict]]:
        option_info = super().transform(obj)
        option_groups = option_info.get("optionCombinationGroupNames") or dict()
        return dict(
            simple = (option_info.get("optionSimple") or list()),
            combinations = [dict(option, **option_groups) for option in (option_info.get("optionCombinations") or list())],
        )


class SupplementList(JsonTransformer):
    path = ["originProduct", "detailAttribute", "supplementProductInfo", "supplementProducts"]


class Option(DuckDBTransformer):
    queries = ["create", "select_option", "insert_option", "select_option_comb", "insert_option_comb", "select_supplement", "insert_supplement"]

    def transform(self, obj: JsonObject, product_id: int | str, channel_seq: int | str | None = None, **kwargs):
        params = dict(product_id=product_id, channel_seq=channel_seq)
        options = OptionList().transform(obj)
        if options.get("simple"):
            self.insert_into_table(options["simple"], key="insert_option", values=":select_option:", params=params)
        if options.get("combinations"):
            self.insert_into_table(options["combinations"], key="insert_option_comb", values=":select_option_comb:", params=params)
        supplements = SupplementList().transform(obj)
        if supplements:
            self.insert_into_table(supplements, key="insert_supplement", values=":select_supplement:", params=params)
