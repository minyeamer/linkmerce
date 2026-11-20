from __future__ import annotations
from linkmerce.core.coupang.api import CoupangApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    import datetime as dt


class RevenueHistory(CoupangApi):
    """쿠팡 마켓플레이스 매출내역을 수집하는 클래스.

    - **Menu**: 정산 > 매출내역 조회
    - **API**: https://api-gateway.coupang.com/v2/providers/openapi/apis/api/v1/revenue-history
    - **Docs**: https://developers.coupang.com/ko/api/settlement/sales-detail-query
    - **Referer**: https://wing.coupang.com/tenants/msf/wing/view/payment-report-view

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    access_key: str
        쿠팡 Open API 액세스 키
    secret_key: str
        쿠팡 Open API 시크릿 키
    vendor_id: str
        업체 코드

    **NOTE** 인스턴스 생성 시 `options` 인자로 `CursorAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        커서 요청 간 대기 시간(초). 기본값은 `0.3`
    """

    method = "GET"
    path = "/v2/providers/openapi/apis/api/v1/revenue-history"
    date_format = "%Y-%m-%d"
    days_limit = 31
    default_options = {"CursorAll": {"request_delay": 0.3}}

    @CoupangApi.with_session
    def extract(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            max_retries: int = 5,
            **kwargs
        ) -> list[dict]:
        """검색 기간에 대한 마켓플레이스 매출내역을 JSON 형식으로 반환한다.

        Parameters
        ----------
        start_date: dt.date | str
            매출인식 시작일. `dt.date` 객체 또는 `"yyyy-MM-dd"` 형식의 문자열을 입력한다.
        end_date: dt.date | str
            매출인식 종료일. `dt.date` 객체 또는 `"yyyy-MM-dd"` 형식의 문자열을 입력한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
        max_retries: int
            동시 요청 제한이 발생할 경우 최대 재시도 횟수. 기본값은 `5`

        Returns
        -------
        list[dict]
            매출내역
        """
        return (self.cursor_all(self.request_json_until_success, self.get_next_cursor)
                .run(
                    start_date = start_date,
                    end_date = (start_date if end_date == ":start_date:" else end_date),
                    vendor_id = self.vendor_id,
                    max_retries = max_retries,
                ))

    def get_next_cursor(self, response: dict, **context) -> str | None:
        """다음 페이지를 가리키는 `nextToken` 커서를 추출한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(response, "nextToken") or None

    def build_request_params(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str,
            next_cursor: str | None = None,
            **kwargs
        ) -> dict:
        return {
            "vendorId": self.vendor_id,
            "recognitionDateFrom": str(start_date),
            "recognitionDateTo": str(end_date),
            "token": next_cursor or str(),
            "maxPerPage": 50,
        }
