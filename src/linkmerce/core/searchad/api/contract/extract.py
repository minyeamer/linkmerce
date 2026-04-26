from __future__ import annotations
from linkmerce.core.searchad.api import NaverSearchAdApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import JsonObject


class TimeContract(NaverSearchAdApi):
    """네이버 검색광고 API로 브랜드검색 광고 계약기간 데이터를 조회하는 클래스.

    - **Menu**: 도구 > 브랜드/신제품 계약 관리 > 브랜드 검색형
    - **API**: https://api.searchad.naver.com/ncc/time-contracts
    - **Docs**: https://naver.github.io/searchad-apidoc/#/tags/TimeContract
    - **Referer**: https://ads.naver.com/manage/ad-accounts/{account_no}/sa/tool/time-contracts

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    api_key: str
        SA API 엑세스라이선스
    secret_key: str
        SA API 비밀키
    customer_id: int | str
        광고계정의 CUSTOMER_ID
    """

    method = "GET"
    uri = "/ncc/time-contracts"

    @NaverSearchAdApi.with_session
    def extract(self) -> JsonObject:
        """브랜드검색 광고 계약기간 데이터를 조회해 JSON 형식으로 반환한다.

        Returns
        -------
        list[dict]
            브랜드검색 광고 계약기간 데이터
        """
        response = self.request_json_safe()
        return self.parse(response)


class BrandNewContract(NaverSearchAdApi):
    """네이버 검색광고 API로 신제품검색 광고 계약기간 데이터를 조회하는 클래스.

    - **Menu**: 도구 > 브랜드/신제품 계약 관리 > 신제품 검색형
    - **API**: https://api.searchad.naver.com/ncc/brand-new/contracts
    - **Docs**: https://naver.github.io/searchad-apidoc/#/tags/BrandNewContract
    - **Referer**: https://ads.naver.com/manage/ad-accounts/{account_no}/sa/tool/time-contracts

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    api_key: str
        SA API 엑세스라이선스
    secret_key: str
        SA API 비밀키
    customer_id: int | str
        광고계정의 CUSTOMER_ID
    """

    method = "GET"
    uri = "/ncc/brand-new/contracts"

    @NaverSearchAdApi.with_session
    def extract(self) -> JsonObject:
        """신제품검색 광고 계약기간 데이터를 조회해 JSON 형식으로 반환한다.

        Returns
        -------
        list[dict]
            신제품검색 광고 계약기간 데이터
        """
        response = self.request_json_safe()
        return self.parse(response)
