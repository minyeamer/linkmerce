from __future__ import annotations

from linkmerce.common.transform import ExcelTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any
    from linkmerce.common.transform import JsonObject


class Product(DuckDBTransformer):
    """사방넷 상품 조회 결과를 `sabangnet_product` 테이블에 적재하는 클래스."""

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
    """사방넷 단품 옵션 조회 결과를 `sabangnet_option` 테이블에 적재하는 클래스."""

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
    """사방넷 옵션 다운로드 결과로부터 옵션 목록을 추출하는 파서 클래스."""

    header = 2
    fields = [
        "사방넷상품코드", "바코드", "옵션제목", "옵션상세명칭", "연결상품코드", "공급상태",
        "옵션구분", "EA", "단품추가금액", "등록일시"
    ]
    on_missing = "ignore"

    def parse(self, obj: bytes, **kwargs) -> list[dict]:
        """헤더 행의 줄바꿈 문자(`\\n`)를 제거해 칼럼명을 정규화한 후 데이터를 반환한다."""
        data: list[dict[str, Any]] = super().parse(obj)[1:]
        keys = {key: key.split('\n')[0].strip() for key in data[0].keys()}
        return [{key_nowrap: option.get(key_wrap) for key_wrap, key_nowrap in keys.items()} for option in data]


class OptionDownload(DuckDBTransformer):
    """사방넷 옵션 다운로드 결과를 `sabangnet_option_download` 테이블에 적재하는 클래스."""

    tables = {"table": "sabangnet_option_download"}
    parser = OptionParser


class AddProductGroup(DuckDBTransformer):
    """사방넷 추가상품 그룹을 `sabangnet_add_product_group` 테이블에 적재하는 클래스."""

    tables = {"table": "sabangnet_add_product_group"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data",
        fields = ["addPrdGrpId", "addPrdGrpNm", "fstRegsDt", "fnlChgDt"],
    )


class AddProduct(DuckDBTransformer):
    """사방넷 추가상품 목록을 `sabangnet_add_product` 테이블에 적재하는 클래스."""

    tables = {"table": "sabangnet_add_product"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data.list",
        fields = ["addPrdGrpId", "addPrdSkuCnfgSrno", "prdNo", "skuNo", "addPrdSkuCnfgNm", "sepr"],
    )

    def transform(self, obj: JsonObject, **kwargs):
        """추가상품 조회 결과 파싱 후 메타 데이터를 SQL 파라미터에 추가해 삽입 쿼리를 실행한다."""
        result = self.parse(obj, **kwargs)
        render, params, total = self.prepare_bulk_params(result, **kwargs)
        if total > 0:
            query = self.prepare_query("bulk_insert", render=render)
            return self.execute(query, **(params | {"meta": self.parse_metadata(obj)}))

    def parse_metadata(self, obj: JsonObject) -> dict:
        """추가상품 조회 결과에서 메타 데이터를 추출해 반환한다."""
        from linkmerce.utils.nested import select_values
        meta = obj["data"]["meta"]
        return select_values(meta, ["addPrdGrpNm", "shmaId", "fstRegsDt", "fnlChgDt"], on_missing="raise")
