from __future__ import annotations

from linkmerce.common.transform import ExcelTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any
    from linkmerce.common.transform import JsonObject


class Product(DuckDBTransformer):
    tables = {"table": "sabangnet_product"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data.list",
        fields = [
            "prdNo", "modlNm", "onsfPrdCd", "prdNm", "prdAbbrRmrk", "brndNm", "mkcpNm",
            "lgstscSvcAcntIdK", "prdSplyStsCd", "prdcYy", "sepr", "splyCprc", "prdImgFilePathNm",
            "fstRegsDt", "fnlChgDt"
        ],
    )


class Option(DuckDBTransformer):
    tables = {"table": "sabangnet_option"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data.optionList",
        fields = [
            "prdNo", "skuNo", "optCnfgNm", "optDtlNm", "skuSplyStsCd", "skuQt", "skuAddAmt",
            "fstRegsDt", "fnlChgDt"
        ],
    )


class OptionParser(ExcelTransformer):
    header = 2
    fields = [{key: None} for key in [
        "사방넷상품코드", "바코드", "옵션제목", "옵션상세명칭", "연결상품코드", "공급상태",
        "옵션구분", "EA", "단품추가금액", "등록일시"
    ]]

    def parse(self, obj: bytes, **kwargs) -> list[dict]:
        data: list[dict[str, Any]] = super().parse(obj)[1:]
        keys = {key: key.split('\n')[0].strip() for key in data[0].keys()}
        return [{key_nowrap: option.get(key_wrap) for key_wrap, key_nowrap in keys.items()} for option in data]


class OptionDownload(DuckDBTransformer):
    tables = {"table": "sabangnet_option_download"}
    parser = OptionParser


class AddProductGroup(DuckDBTransformer):
    tables = {"table": "sabangnet_add_product_group"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data",
        fields = ["addPrdGrpId", "addPrdGrpNm", "fstRegsDt", "fnlChgDt"],
    )


class AddProduct(DuckDBTransformer):
    tables = {"table": "sabangnet_add_product"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data.list",
        fields = ["addPrdGrpId", "addPrdSkuCnfgSrno", "prdNo", "skuNo", "addPrdSkuCnfgNm", "sepr"],
    )

    def transform(self, obj: JsonObject, **kwargs):
        result = self.parse(obj, **kwargs)
        render, params, total = self.prepare_bulk_params(result, **kwargs)
        if total > 0:
            query = self.prepare_query(key="bulk_insert", render=render)
            params.update(meta=self.parse_metadata(obj))
            return self.execute(query, **params)

    def parse_metadata(self, obj: JsonObject) -> dict:
        from linkmerce.utils.nested import select_values
        meta = obj["data"]["meta"]
        return select_values(meta, ["addPrdGrpNm", "shmaId", "fstRegsDt", "fnlChgDt"], on_missing="raise")
