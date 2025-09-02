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
                product[key] = product.get(key)
        return product
