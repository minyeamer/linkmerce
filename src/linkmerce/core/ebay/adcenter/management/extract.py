from __future__ import annotations

from linkmerce.core.ebay.adcenter import GmarketAdCenter

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    import datetime as dt


class CampaignGroup(GmarketAdCenter):
    """Gmarket 광고센터 캠페인 그룹 목록을 조회하는 클래스.

    - **Menu**: 광고 관리 > 캠페인 목록
    - **API**: https://adcenter.esmplus.com/ad/management
    - **Referer**: https://adcenter.esmplus.com/ad/management

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.
    """

    method = "POST"
    path = "/ad/management"
    date_format = "%Y%m%d"
    days_limit = 62

    @GmarketAdCenter.with_session
    def extract(
            self,
            start_date: dt.date | str | Literal[":today:"] = ":today:",
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            **kwargs,
        ) -> str:
        """조회 기간 내 캠페인 그룹 목록을 조회해 반환한다.

        Parameters
        ----------
        start_date: dt.date | str
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":today:"`: 오늘 날짜 (기본값)
        end_date: dt.date | str
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)

        Returns
        -------
        str
            캠페인 그룹 목록을 포함한 응답 텍스트
        """
        from linkmerce.core.ebay.adcenter import get_date_pair
        response = self.request_text(**get_date_pair(start_date, end_date))
        return self.parse(response, **kwargs)

    def build_request_json(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str,
            **kwargs,
        ) -> list[dict]:
        return [{
            "selectStartDate": str(start_date).replace('-', ''),
            "selectEndDate": str(end_date).replace('-', ''),
            "matchType": 2,
        }]

    def build_request_headers(self, **kwargs) -> dict[str, str]:
        return self.get_request_headers() | {
            "next-action": "40de689aa8afc789ccca5a08703f99f733e7487267",
            "referer": (self.origin + "/ad/management"),
        }


class Campaign(GmarketAdCenter):
    """Gmarket 광고센터 캠페인 목록을 조회하는 클래스.

    - **Menu**: 광고 관리 > 캠페인 목록 > 그룹 목록
    - **API**: https://adcenter.esmplus.com/ad/management?q=
    - **Referer**: https://adcenter.esmplus.com/ad/management?q=

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        캠페인 그룹별 요청 간 대기 시간(초). 기본값은 `1`
    tqdm_options: dict | None
        반복 요청 작업 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    path = "/ad/management"
    date_format = "%Y%m%d"
    days_limit = 62
    default_options = {"RequestEach": {"request_delay": 1}}

    @GmarketAdCenter.with_session
    def extract(
            self,
            campaign_group_id: int | str | Iterable[int | str],
            start_date: dt.date | str | Literal[":today:"] = ":today:",
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            **kwargs,
        ) -> str:
        """조회 기간 내 캠페인 그룹별 캠페인 목록을 조회해 반환한다.

        Parameters
        ----------
        campaign_group_id: int | str | Iterable[int | str]
            캠페인 그룹 ID 목록
        start_date: dt.date | str
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":today:"`: 오늘 날짜 (기본값)
        end_date: dt.date | str
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)

        Returns
        -------
        str
            캠페인 목록을 포함한 응답 텍스트
        """
        from linkmerce.core.ebay.adcenter import get_date_pair
        return (self.request_each(self.request_text)
                .partial(**get_date_pair(start_date, end_date))
                .expand(campaign_group_id=campaign_group_id)
                .run())

    def build_request_params(self, campaign_group_id: int | str, **kwargs) -> dict[str, str]:
        from base64 import b64encode
        from urllib.parse import quote
        import json
        params = {
            "step": "group",
            "campaignGroupId": int(campaign_group_id),
            "campaignId": 0,
            "campaignGroupName": '',
            "campaignName": '',
            "adType": "semiAuto",
        }
        urlencoded = quote(json.dumps(params, ensure_ascii=False, separators=(',', ':')))
        return {"q": b64encode(urlencoded.encode("utf-8")).decode()}

    def build_request_json(
            self,
            campaign_group_id: int | str,
            start_date: dt.date | str,
            end_date: dt.date | str,
            **kwargs,
        ) -> list[dict]:
        return [{
            "selectStartDate": str(start_date).replace('-', ''),
            "selectEndDate": str(end_date).replace('-', ''),
            "matchType": 2,
            "campaignGroupId": int(campaign_group_id),
        }]

    def build_request_headers(self, campaign_group_id: int | str, **kwargs) -> dict[str, str]:
        q = self.build_request_params(campaign_group_id)["q"]
        return self.get_request_headers() | {
            "next-action": "601f15a2d3bd42f39ead15b0b361e57e681f956e2d",
            "referer": (self.origin + f"/ad/management?q={q}"),
        }


class Product(GmarketAdCenter):
    """Gmarket 광고센터 상품 목록을 조회하는 클래스.

    - **Menu**: 광고 관리 > 캠페인 목록 > 그룹 목록 > 상품 목록
    - **API**: https://adcenter.esmplus.com/ad/management?q=
    - **Referer**: https://adcenter.esmplus.com/ad/management?q=

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        캠페인별 요청 간 대기 시간(초). 기본값은 `1`
    tqdm_options: dict | None
        반복 요청 작업 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    path = "/ad/management"
    date_format = "%Y%m%d"
    days_limit = 62
    default_options = {"RequestEach": {"request_delay": 1}}

    @GmarketAdCenter.with_session
    def extract(
            self,
            campaign_id: int | str | Iterable[int | str],
            start_date: dt.date | str | Literal[":today:"] = ":today:",
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            **kwargs,
        ) -> str:
        """조회 기간 내 캠페인별 상품 목록을 조회해 반환한다.

        Parameters
        ----------
        campaign_id: int | str | Iterable[int | str]
            캠페인 ID 목록
        start_date: dt.date | str
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":today:"`: 오늘 날짜 (기본값)
        end_date: dt.date | str
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)

        Returns
        -------
        str
            상품 목록을 포함한 응답 텍스트
        """
        from linkmerce.core.ebay.adcenter import get_date_pair
        return (self.request_each(self.request_text)
                .partial(**get_date_pair(start_date, end_date))
                .expand(campaign_id=campaign_id)
                .run())

    def build_request_params(self, campaign_id: int | str, **kwargs) -> dict[str, str]:
        from base64 import b64encode
        from urllib.parse import quote
        import json
        params = {
            "step": "product",
            "campaignGroupId": 0,
            "campaignId": int(campaign_id),
            "campaignGroupName": '',
            "campaignName": '',
            "adType": "semiAuto",
        }
        urlencoded = quote(json.dumps(params, ensure_ascii=False, separators=(',', ':')))
        return {"q": b64encode(urlencoded.encode("utf-8")).decode()}

    def build_request_json(
            self,
            campaign_id: int | str,
            start_date: dt.date | str,
            end_date: dt.date | str,
            **kwargs,
        ) -> list[dict]:
        return [{
            "selectStartDate": str(start_date).replace('-', ''),
            "selectEndDate": str(end_date).replace('-', ''),
            "matchType": 2,
            "campaignId": int(campaign_id),
        }]

    def build_request_headers(self, campaign_id: int | str, **kwargs) -> dict[str, str]:
        q = self.build_request_params(campaign_id)["q"]
        return self.get_request_headers() | {
            "next-action": "605b86623577a01de2fa48e10d79687dcf66cf8d3a",
            "referer": (self.origin + f"/ad/management?q={q}"),
        }
