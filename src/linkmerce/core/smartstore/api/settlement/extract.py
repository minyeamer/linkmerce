from __future__ import annotations
from linkmerce.core.smartstore.api import SmartstoreApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class Settlement(SmartstoreApi):
    """네이버 커머스 API로 건별 정산 내역 조회 결과를 수집하는 클래스.

    - **Menu**: 정산관리 > 정산 내역 (일별/건별) > 건별 정산내역 (건별 정산 내역 조회)
    - **API**: https://api.commerce.naver.com/external/v1/pay-settle/settle/case
    - **Docs**: https://apicenter.commerce.naver.com/ko/basic/commerce-api
    - **Referer**: https://sell.smartstore.naver.com/#/naverpay/settlemgt/sellerdailysettle

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    client_id: str
        커머스 API 애플리케이션 ID
    client_secret: str
        커머스 API 애플리케이션 시크릿

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        페이지 요청 간 대기 시간(초). 기본값은 `1`
    tqdm_options: dict | None
        페이지 순회 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachPages` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        조회일자별 요청 간 대기 시간(초). 기본값은 `1`
    tqdm_options: dict | None
        반복 요청 작업 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    version = "v1"
    path = "/pay-settle/settle/case"
    date_format = "%Y-%m-%d"
    max_page_size = 1000
    page_start = 1
    default_options = {
        "PaginateAll": {"request_delay": 1},
        "RequestEachPages": {"request_delay": 1},
    }

    @SmartstoreApi.with_session
    @SmartstoreApi.with_token
    def extract(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            period_type: str = "SETTLE_COMPLETE_DATE",
            settle_type: str | None = None,
            settle_decision_type: str | None = None,
            channel_seq: int | str | None = None,
            max_retries: int = 5,
            **kwargs
        ) -> dict | list[dict]:
        """건별 정산 내역을 일별로 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        start_date: dt.date | str
            조회 기준의 시작 일시. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
        end_date: dt.date | str
            조회 기준의 종료 일시. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
        period_type: str
            조회 기간 기준. `period_type` 속성의 키를 전달할 수 있다. 기본값은 정산 완료일(`"SETTLE_COMPLETE_DATE"`)
        settle_type: str | None
            정산 구분. `settle_type` 속성의 키를 전달할 수 있다.
        settle_decision_type: str | None
            결제일 구분. `settle_decision_type` 속성의 키를 전달할 수 있다.   
            (조회 기간 기준이 "결제일"인 경우에 적용된다.)
        channel_seq: int | str | None
            채널 번호. 조회 시점에는 사용되지 않고 파서 함수에 전달된다.
        max_retries: int
            동시 요청 제한이 발생할 경우 최대 재시도 횟수. 기본값은 `5`

        Returns
        -------
        dict | list[dict]
            건별 정산 내역. 조회 기간에 따라 반환 타입이 다르다.
                - `start_date`와 `end_date`가 동일할 때 -> `dict`
                - `start_date`와 `end_date`가 다를 때 -> `list[dict]`
        """
        return (self.request_each_pages(self.request_json_until_success)
                .partial(
                    period_type = period_type,
                    settle_decision_type = settle_decision_type,
                    settle_type = settle_type,
                    channel_seq = channel_seq,
                    max_retries = max_retries,
                ).expand(date=self.generate_date_range(start_date, end_date, freq='D'))
                .all_pages(self.count_total, self.max_page_size, self.page_start)
                .run())

    def count_total(self, response: JsonObject, **kwargs) -> int:
        """HTTP 응답에서 전체 항목 수를 추출한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(response, "pagination.totalElements")

    def build_request_params(
            self,
            date: dt.date,
            period_type: str = "SETTLE_COMPLETE_DATE",
            settle_type: str | None = None,
            settle_decision_type: str | None = None,
            page: int = 1,
            page_size: int = 1000,
            **kwargs
        ) -> dict:
        return {
            "searchDate": str(date),
            "periodType": f"SETTLE_CASEBYCASE_{period_type}",
            **({"settleType": settle_type} if settle_type else dict()),
            **({"settleDecisionType": settle_decision_type} if settle_decision_type else dict()),
            "pageNumber": page,
            "pageSize": page_size,
        }

    @property
    def period_type(self) -> dict[str, str]:
        """조회 기간 기준 코드와 한글명 매핑을 반환한다."""
        return {
            "SETTLE_SCHEDULE_DATE": "정산 예정일", "SETTLE_BASIS_DATE": "정산 기준일",
            "SETTLE_COMPLETE_DATE": "정산 완료일", "PAY_DATE": "결제일",
            "TAXRETURN_BASIS_DATE": "세금 신고 기준일"
        }

    @property
    def settle_type(self) -> dict[str, str]:
        """정산 구분 코드와 한글명 매핑을 반환한다."""
        return {
            "SETTLED": "정산 확정 건", "UNSETTLED": "정산 미확정 건", "BEFORE_CANCEL": "정산 전 취소 건"
        }

    @property
    def settle_decision_type(self) -> dict[str, str]:
        """결제일 구분 코드와 한글명 매핑을 반환한다."""
        return {
            "NORMAL_SETTLE_ORIGINAL": "일반 정산",
            "NORMAL_SETTLE_AFTER_CANCEL": "정산 후 취소", "NORMAL_SETTLE_BEFORE_CANCEL": "정산 전 취소",
            "QUICK_SETTLE_ORIGINAL": "빠른정산", "QUICK_SETTLE_CANCEL": "빠른정산 회수",
            "QUANTITY_CANCEL_DEDUCTION": "수량 취소 정산(공제)", "QUANTITY_CANCEL_RESTORE": "수량 취소 정산(환급)"
        }
