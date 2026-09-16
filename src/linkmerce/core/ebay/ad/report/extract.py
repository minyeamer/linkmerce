from __future__ import annotations

from linkmerce.core.ebay.ad import AuctionAdCenter

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    import datetime as dt


class AiReport(AuctionAdCenter):
    """AUCTION 광고센터 AI매출업 상품별 리포트를 조회하는 클래스.

    - **Menu**: AI매출업 > 리포트 (항목별) > 상품별
    - **API**: https://ad.esmplus.com/Remarketing/Report/GetCpcRemarketingReportGoodsList
    - **Referer**: https://ad.esmplus.com/Remarketing/Report/GroupReport

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        페이지 요청 간 대기 시간(초). 기본값은 `1`
    tqdm_options: dict | None
        페이지 순회 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachPages` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        일별 요청 간 대기 시간(초). 기본값은 `1`
    tqdm_options: dict | None
        반복 요청 작업 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    path = "/Remarketing/Report/GetCpcRemarketingReportGoodsList"
    date_format = "%Y-%m-%d"
    max_page_size = 100
    page_start = 1
    default_options = {
        "PaginateAll": {"request_delay": 1},
        "RequestEachPages": {"request_delay": 1},
    }

    @AuctionAdCenter.with_session
    def extract(
            self,
            master_id: int | str,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            **kwargs,
        ) -> list[dict]:
        """상품별 리포트를 일별로 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        master_id: int | str
            ESM PLUS 마스터 아이디 번호
        start_date: dt.date | str
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
        end_date: dt.date | str
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)

        Returns
        -------
        list[dict]
            상품별/일별 리포트 조회 결과
        """
        context = self.generate_date_context(start_date, end_date, freq='D')
        return (self.request_each_pages(self.request_json, context=context)
                .partial(master_id=master_id)
                .all_pages(self.count_total, self.max_page_size, self.page_start)
                .run())

    def count_total(self, response: dict, **kwargs) -> int:
        """HTTP 응답에서 전체 행 수를 추출한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(response, "data.TotalCnt")

    def build_request_json(
            self,
            master_id: int | str,
            start_date: dt.date | str,
            end_date: dt.date | str,
            page: int = 1,
            page_size: int = 100,
            **kwargs,
        ) -> dict:
        return {
            "CurrentPage": page,
            "GroupNo": "0",
            "MasterId": str(master_id),
            "PageSize": page_size,
            "SchEndDate": str(end_date),
            "SchStartDate": str(start_date),
            "SellerId": "",
            "SiteGoodsNo": "",
            "SiteId": "0",
        }

    def build_request_headers(self, **kwargs) -> dict[str, str]:
        return self.get_request_headers() | {
            "referer": (self.origin + "/Remarketing/Report/GroupReport"),
        }


class CpcReport(AuctionAdCenter):
    """AUCTION 광고센터 파워클릭 상품별 리포트를 조회하는 클래스.

    - **Menu**: 파워클릭 > 리포트 (항목별) > 상품별
    - **API**: https://ad.esmplus.com/CPC/Report/GetReportListData
    - **Referer**: https://ad.esmplus.com/cpc/report/groupReport

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        페이지 요청 간 대기 시간(초). 기본값은 `1`
    tqdm_options: dict | None
        페이지 순회 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachPages` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        일별 요청 간 대기 시간(초). 기본값은 `1`
    tqdm_options: dict | None
        반복 요청 작업 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    path = "/CPC/Report/GetReportListData"
    date_format = "%Y-%m-%d"
    max_page_size = 100
    page_start = 1
    default_options = {
        "PaginateAll": {"request_delay": 1},
        "RequestEachPages": {"request_delay": 1},
    }

    @AuctionAdCenter.with_session
    def extract(
            self,
            master_id: int | str,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            **kwargs,
        ) -> list[dict]:
        """상품별 리포트를 일별로 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        master_id: int | str
            ESM PLUS 마스터 아이디 번호
        start_date: dt.date | str
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
        end_date: dt.date | str
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)

        Returns
        -------
        list[dict]
            상품별/일별 리포트 조회 결과
        """
        context = self.generate_date_context(start_date, end_date, freq='D')
        return (self.request_each_pages(self.request_json, context=context)
                .partial(master_id=master_id)
                .all_pages(self.count_total, self.max_page_size, self.page_start)
                .run())

    def count_total(self, response: dict, **kwargs) -> int:
        """HTTP 응답에서 전체 행 수를 추출한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(response, "searchReportSum.totalCnt")

    def build_request_data(
            self,
            master_id: int | str,
            start_date: dt.date | str,
            end_date: dt.date | str,
            page: int = 1,
            page_size: int = 100,
            **kwargs,
        ) -> dict:
        return {
            "selectedMastrId": str(master_id),
            "startDate": str(start_date),
            "endDate": str(end_date),
            "sellerID": "",
            "groupNo": "0",
            "siteGoodsNo": "",
            "keywordName": "",
            "adId": "",
            "pageSize": page_size,
            "pageIdx": page,
            "sortColumn": 1,
            "sortType": 1,
            "pageType": "Good",
            "isPast": "false",
            "siteType": "",
        }

    def build_request_headers(self, **kwargs) -> dict[str, str]:
        return self.get_request_headers() | {
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "referer": (self.origin + "/cpc/report/groupReport"),
        }

    @property
    def page_type(self) -> dict[str, str]:
        """리포트 페이지 유형 코드와 한글명 매핑을 반환한다."""
        return {
            "Group": "광고그룹별", "Good": "상품별", "Keyword": "키워드별", "Category": "카테고리별",
            "Vip": "상품상세페이지", "Media": "노출매체별", "AdId": "광고ID별"
        }
