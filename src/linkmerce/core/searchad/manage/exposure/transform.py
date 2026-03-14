from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


class ExposureParser(JsonTransformer):
    dtype = dict
    scope = "adList"
    fields = [
        "rank", "imageUrl", "productTitle", "isOwn", "categoryNames",
        "fmpBrand", "fmpMaker", "lowPrice", "mobileLowPrice"
    ]

    def assert_valid_response(self, obj: dict, **kwargs):
        super().assert_valid_response(obj)
        if obj.get("code"):
            self.raise_request_error(obj.get("title") or obj.get("message") or str())


class ExposureDiagnosis(DuckDBTransformer):
    tables = {"table": "searchad_exposure"}
    parser = ExposureParser
    parser_config = dict(
        defaults = {"keyword": "$keyword"},
    )

    def bulk_insert(self, result: list[dict], is_own: bool | None = None, **kwargs):
        return super().bulk_insert(result, params=dict(is_own=is_own))


class ExposureRank(DuckDBTransformer):
    tables = {"rank": "searchad_rank", "product": "searchad_product"}
