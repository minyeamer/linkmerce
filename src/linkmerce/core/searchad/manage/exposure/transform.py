from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


class ExposureParser(JsonTransformer):
    """네이버 검색광고 노출 진단 결과의 모든 항목을 추출하는 파서 클래스."""

    dtype = dict
    scope = "adList"
    fields = [
        "rank", "imageUrl", "productTitle", "isOwn", "categoryNames",
        "fmpBrand", "fmpMaker", "lowPrice", "mobileLowPrice"
    ]

    def assert_valid_response(self, obj: dict, **kwargs):
        """응답에 `code` 필드가 있으면 `RequestError`를 발생시킨다."""
        super().assert_valid_response(obj)
        if obj.get("code"):
            self.raise_request_error(obj.get("title") or obj.get("message") or str())


class ExposureDiagnosis(DuckDBTransformer):
    """네이버 검색광고 노출 진단 결과를 `searchad_exposure` 테이블에 적재하는 클래스.

    `is_own` 파라미터에 따라 노출 진단 결과를 필터링한다:
    - `True`: 소유한 광고 소재만 필터
    - `False`: 소유하지 않은 광고 소재만 필터
    - `None`: 모든 광고 소재의 진단 결과 추출
    """

    tables = {"table": "searchad_exposure"}
    parser = ExposureParser
    params = {"keyword": "$keyword", "is_own": "$is_own"}


class ExposureRank(ExposureDiagnosis):
    """네이버 검색광고 노출 진단 결과로부터 순위 및 상품 목록을 각각의 테이블에 변환 및 적재하는 클래스.

    테이블 키 | 테이블명 | 설명
    - `rank` | `searchad_rank` | 광고 노출 순위
    - `product` | `searchad_product` | 광고 노출 상품 목록

    `is_own` 파라미터에 따라 노출 순위를 필터링한다:
    - `True`: 소유한 광고 소재만 필터
    - `False`: 소유하지 않은 광고 소재만 필터
    - `None`: 모든 광고 소재의 진단 결과 추출"""

    tables = {"rank": "searchad_rank", "product": "searchad_product"}
