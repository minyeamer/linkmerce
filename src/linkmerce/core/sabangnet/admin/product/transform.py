from __future__ import annotations

from linkmerce.common.transform import ExcelTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any
    from linkmerce.common.transform import JsonObject


class Product(DuckDBTransformer):
    """사방넷상품조회수정 메뉴의 상품 목록을 변환 및 적재하는 클래스.

    - **Extractor**: `Product`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: sabangnet_product`
    """

    extractor = "Product"
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
    """사방넷상품조회수정 메뉴의 옵션 목록을 변환 및 적재하는 클래스.

    - **Extractor**: `Option`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: sabangnet_option`
    """

    extractor = "Option"
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
    """사방넷단품대량수정 메뉴의 옵션 목록 다운로드 결과를 파싱하는 클래스."""

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
    """사방넷단품대량수정 메뉴의 옵션 목록을 변환 및 적재하는 클래스.

    - **Extractor**: `OptionDownload`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `OptionParser: bytes -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: sabangnet_option_download`
    """

    extractor = "OptionDownload"
    tables = {"table": "sabangnet_option_download"}
    parser = OptionParser


class AddProductGroup(DuckDBTransformer):
    """사방넷추가상품관리 메뉴의 추가상품 그룹 목록을 변환 및 적재하는 클래스.

    - **Extractor**: `AddProductGroup`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: sabangnet_add_product_group`
    """

    extractor = "AddProductGroup"
    tables = {"table": "sabangnet_add_product_group"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data",
        fields = ["addPrdGrpId", "addPrdGrpNm", "fstRegsDt", "fnlChgDt"],
    )


class AddProduct(DuckDBTransformer):
    """사방넷추가상품관리 메뉴의 추가상품 목록을 변환 및 적재하는 클래스.

    - **Extractor**: `AddProduct`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: sabangnet_add_product`
    """

    extractor = "AddProduct"
    tables = {"table": "sabangnet_add_product"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data.list",
        fields = ["addPrdGrpId", "addPrdSkuCnfgSrno", "prdNo", "skuNo", "addPrdSkuCnfgNm", "sepr"],
    )

    def transform(self, obj: JsonObject, **kwargs) -> list:
        """추가상품 조회 결과 파싱 후 메타 데이터를 SQL 파라미터에 추가해 삽입 쿼리를 실행한다."""
        result = self.parse(obj, **kwargs)
        return self.bulk_insert(result, params={"meta": self.parse_metadata(obj)})

    def parse_metadata(self, obj: JsonObject) -> dict:
        """추가상품 조회 결과에서 메타 데이터를 추출해 반환한다."""
        from linkmerce.utils.nested import select_values
        meta = obj["data"]["meta"]
        return select_values(meta, ["addPrdGrpNm", "shmaId", "fstRegsDt", "fnlChgDt"], on_missing="raise")
