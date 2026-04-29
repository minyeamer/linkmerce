from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class ExposureDiagnosis(DuckDBTransformer):
    """네이버 광고주센터에서 키워드별 노출 진단 결과를 변환 및 적재하는 클래스.

    - **Extractor**: `ExposureDiagnosis`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `ExposureParser: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: searchad_exposure`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    keyword: str
        조회 키워드
    is_own: bool | None
        소유 여부 필터
    """

    extractor = "ExposureDiagnosis"
    tables = {"table": "searchad_exposure"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "adList",
        fields = [
            "rank", "imageUrl", "productTitle", "isOwn", "categoryNames",
            "fmpBrand", "fmpMaker", "lowPrice", {"mobileLowPrice": None}
        ],
    )
    params = {"keyword": "$keyword", "is_own": "$is_own"}


class ExposureRank(ExposureDiagnosis):
    """네이버 광고주센터에서 키워드별 노출 진단 결과를 변환 및 적재하는 클래스.

    **NOTE** 노출 진단 결과로부터 상품 순위와 상품 정보를 각각의 테이블로 분리한다.

    - **Extractor**: `ExposureDiagnosis`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `ExposureParser`: `dict` -> `list[dict]`

    - **Table** ( *table_key: table_name (description)* ):
        1. `rank`: `searchad_rank` (노출 상품 순위)
        2. `product`: `searchad_product` (노출 상품 목록)

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    keyword: str
        조회 키워드
    is_own: bool | None
        소유 여부 필터
    """

    extractor = "ExposureDiagnosis"
    tables = {"rank": "searchad_rank", "product": "searchad_product"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "adList",
        fields = [
            "rank", "imageUrl", "productTitle", "isOwn", "categoryNames",
            "fmpBrand", "lowPrice", {"mobileLowPrice": None}
        ],
    )
    params = {"keyword": "$keyword", "is_own": "$is_own"}
