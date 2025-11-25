from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any
    from linkmerce.common.transform import JsonObject


class ProductList(JsonTransformer):
    dtype = dict
    path = ["data", "list"]


class Product(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, **kwargs):
        products = ProductList().transform(obj)
        if products:
            products[0]["fnlChgDt"] = products[0].get("fnlChgDt")
            return self.insert_into_table(products)


class OptionList(JsonTransformer):
    dtype = dict
    path = ["data", "optionList"]


class Option(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, **kwargs):
        options = OptionList().transform(obj)
        if options:
            options[0]["fnlChgDt"] = options[0].get("fnlChgDt")
            return self.insert_into_table(options)


class OptionDownload(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: bytes, **kwargs):
        from linkmerce.utils.excel import excel2json
        options = excel2json(obj, header=2, warnings=False)[1:]
        if options:
            options = self.validate_options(options)
            return self.insert_into_table(options)

    def validate_options(self, options: list[dict[str,Any]]) -> list[dict]:
        keys = {key: key.split('\n')[0].strip() for key in options[0].keys()}
        options = [{key_abb: option.get(key_org) for key_org, key_abb in keys.items()} for option in options]
        for key in ["사방넷상품코드", "바코드", "옵션상세명칭", "공급상태", "단품추가금액", "옵션구분", "연결상품코드", "EA", "옵션제목", "등록일시"]:
            if key not in options[0]:
                options[0][key] = None
        return options


class AddProductGroup(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, **kwargs):
        groups = obj["data"]
        if groups:
            return self.insert_into_table(groups)


class AddProduct(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, **kwargs):
        products = ProductList().transform(obj)
        if products:
            meta = obj["data"]["meta"]
            return self.insert_into_table(products, params=dict(meta=meta))
