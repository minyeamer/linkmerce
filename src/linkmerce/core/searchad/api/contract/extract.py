from __future__ import annotations
from linkmerce.core.searchad.api import NaverSearchAdApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import JsonObject


class TimeContract(NaverSearchAdApi):
    """네이버 검색광고 API로 브랜드검색 광고 계약기간 데이터를 조회하는 클래스.

    - **API URL**: `GET` https://api.searchad.naver.com/ncc/time-contracts
    - **API Docs**: https://naver.github.io/searchad-apidoc/
    """
    method = "GET"
    uri = "/ncc/time-contracts"

    @NaverSearchAdApi.with_session
    def extract(self) -> JsonObject:
        """브랜드검색 광고 계약기간 데이터를 조회해 JSON 형식으로 반환한다."""
        response = self.request_json_safe()
        return self.parse(response)


class BrandNewContract(NaverSearchAdApi):
    """네이버 검색광고 API로 신제품검색 광고 계약기간 데이터를 조회하는 클래스.

    - **API URL**: `GET` https://api.searchad.naver.com/ncc/brand-new/contracts
    - **API Docs**: https://naver.github.io/searchad-apidoc/
    """
    method = "GET"
    uri = "/ncc/brand-new/contracts"

    @NaverSearchAdApi.with_session
    def extract(self) -> JsonObject:
        """신제품검색 광고 계약기간 데이터를 조회해 JSON 형식으로 반환한다."""
        response = self.request_json_safe()
        return self.parse(response)
