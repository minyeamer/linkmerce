from __future__ import annotations
from linkmerce.core.coupang.advertising import CoupangAds

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence


class Campaign(CoupangAds):
    """쿠팡 광고센터 캠페인 목록을 조회하는 클래스.

    - **Menu**: 광고 관리 > 매출 성장 / 신규 구매 고객 확보 / 인지도 상승
    - **API**: https://advertising.coupang.com/marketing/tetris-api/campaigns
    - **Referer**: https://advertising.coupang.com/marketing/dashboard/sales

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        페이지 요청 간 대기 시간(초). 기본값은 `1`
    tqdm_options: dict | None
        페이지 순회 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    path = "/marketing/tetris-api/campaigns"
    max_page_size = 20
    page_start = 0
    date_format = "%Y%m%d"
    default_options = {"PaginateAll": {"request_delay": 1}}

    @CoupangAds.with_session
    def extract(
            self,
            goal_type: Literal["SALES", "NCA", "REACH"] = "SALES",
            is_deleted: bool = False,
            vendor_id: str | None = None,
            **kwargs
        ) -> list[dict]:
        """광고 목표별 캠페인 목록을 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        goal_type: str
            조회할 광고 목표
                - `"SALES"`: 매출 성장 (기본값)
                - `"NCA"`: 신규 구매 고객 확보
                - `"REACH"`: 인지도 상승
        is_deleted: bool
            삭제된 캠페인 조회 여부
                - `True`: 삭제된 캠페인만 조회
                - `False`: 삭제되지 않은 전체 캠페인 조회 (기본값)
        vendor_id: str | None
            업체 코드. 조회 시점에는 사용되지 않고 파서 함수에 전달된다.

        Returns
        -------
        list[dict]
            전체 또는 삭제된 캠페인 목록
        """
        return (self.paginate_all(self.request_json_with_timeout, self.count_total, self.max_page_size, self.page_start)
                .run(goal_type=goal_type, is_deleted=is_deleted, vendor_id=vendor_id, **kwargs))

    def count_total(self, response: dict, **kwargs) -> int:
        """HTTP 응답에서 전체 캠페인 수를 추출한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(response, ["pageInfo", "totalCount"])

    def request_json_with_timeout(self, max_retries: int = 5, **kwargs) -> dict:
        """요청 후 타임아웃(Timeout)이 발생하면 `max_retries` 횟수만큼 성공할 때까지 재시도한다."""
        from requests.exceptions import Timeout
        import random
        session = self.get_session()
        message = self.build_request_message(**kwargs)
        for retry_count in range(1, max_retries+1):
            try:
                with session.request(**message, timeout=random.randint(30, 60)) as response:
                    return response.json()
            except Timeout as error:
                if retry_count == max_retries:
                    raise error

    def build_request_json(
            self,
            goal_type: Literal["SALES", "NCA", "REACH"] = "SALES",
            page: int = 0,
            size: int = 20,
            is_deleted: bool = False,
            **kwargs
        ) -> dict:
        return {
            "isDeleted": is_deleted,
            "pagination": {"page": page, "size": size},
            "sortedBy": "ID",
            "isSortDesc": "DESC",
            "budgetTypes": None,
            "isActive": None,
            "name": "",
            "creationContext": None,
            "objective": None,
            "primaryOrderBy": "DEFAULT",
            "goalType": goal_type,
            "targetCampaignId": None,
            "vendorItemId": None
        }


class Creative(CoupangAds):
    """쿠팡 광고센터 신규 구매 고객 확보(NCA) 캠페인의 소재 정보를 조회하는 클래스.

    - **Menu**: 광고 관리 > 신규 구매 고객 확보 > 캠페인 > 광고
    - **API**: https://advertising.coupang.com/marketing/tetris-api/nca/campaign/{campaign_id}
    - **Referer**: https://advertising.coupang.com/marketing/dashboard/nca

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        캠페인별 요청 간 대기 시간(초). 기본값은 `0.3`
    tqdm_options: dict | None
        반복 요청 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    path = "/marketing/tetris-api/nca/campaign/{}"
    max_page_size = 20
    page_start = 0
    date_format = "%Y%m%d"
    default_options = {"RequestEach": {"request_delay": 0.3}}

    @CoupangAds.with_session
    def extract(
            self,
            campaign_id: int | str | Sequence[int | str],
            vendor_id: str | None = None,
            **kwargs
        ) -> dict | list[dict]:
        """신규 구매 고객 확보 캠페인별 소재 목록을 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        campaign_id: int | str | Sequence[int | str]
            조회할 캠페인 ID. 단일 값 또는 배열을 입력한다.
        vendor_id: str | None
            업체 코드. 조회 시점에는 사용되지 않고 파서 함수에 전달된다.

        Returns
        -------
        dict | list[dict]
            신규 구매 고객 확보 캠페인별 소재 정보 목록. `campaign_id` 타입에 따라 반환 타입이 다르다.
                - `campaign_id`가 `int | str` 타입일 때 -> `dict`
                - `campaign_id`가 `Sequence[int | str]` 타입일 때 -> `list[dict]`
        """
        return (self.request_each(self.request_json_safe)
                .partial(vendor_id=vendor_id)
                .expand(campaign_id=campaign_id)
                .run())

    def build_request_message(self, campaign_id: int | str, **kwargs) -> dict:
        """각 HTTP 요청마다 URL에 캠페인 ID를 포맷팅한다."""
        kwargs["url"] = self.url.format(campaign_id)
        return super().build_request_message(**kwargs)

    def set_request_headers(self, **kwargs):
        referer = self.origin + "/marketing/dashboard/nca"
        return super().set_request_headers(contents="json", origin=self.origin, referer=referer, **kwargs)
