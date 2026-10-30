from __future__ import annotations
import datetime as dt

from linkmerce.core.coupang.api import CoupangApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence


class ReturnRequest(CoupangApi):
    """쿠팡 마켓플레이스 반품 및 취소 요청 목록을 수집하는 클래스.

    - **Menu**: 반품 API > 반품 / 취소 요청 목록 조회
    - **API**: https://api-gateway.coupang.com/v2/providers/openapi/apis/api/v6/vendors/{vendor_id}/returnRequests
    - **Docs**: https://developers.coupang.com/ko/api/returns/return-cancellation-request-list-query
    - **Referer**: https://wing.coupang.com/tenants/sfl-portal/return-delivery/list

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    access_key: str
        쿠팡 Open API 액세스 키
    secret_key: str
        쿠팡 Open API 시크릿 키
    vendor_id: str
        업체 코드

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        검색 기간 및 상태/유형별 요청 간 대기 시간(초). 기본값은 `0.3`
    tqdm_options: dict | None
        반복 요청 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    path = "/v2/providers/openapi/apis/api/v6/vendors/{vendor_id}/returnRequests"
    datetime_format = "%Y-%m-%dT%H:%M"
    default_options = {"RequestEach": {"request_delay": 0.3}}

    @CoupangApi.with_session
    def extract(
            self,
            start_datetime: dt.datetime | str,
            end_datetime: dt.datetime | str | Literal[":end_of_day:"] = ":end_of_day:",
            cancel_type: Literal["ALL", "RETURN", "CANCEL"] = "ALL",
            status: Literal["ALL", "RU", "UC", "CC", "PR"] | Sequence[str] | None = "ALL",
            max_retries: int = 5,
            **kwargs
        ) -> dict | list[dict]:
        """검색 기간에 대한 마켓플레이스 반품 및 취소 요청 목록을 JSON 형식으로 반환한다.

        Parameters
        ----------
        start_datetime: dt.datetime | str
            검색 시작 일시. `dt.datetime` 객체 또는 "yyyy-MM-ddTHH:mm" 형식의 문자열을 입력한다.
        end_datetime: dt.datetime | str
            검색 종료 일시. `dt.datetime` 객체 또는 "yyyy-MM-ddTHH:mm" 형식의 문자열을 입력한다.
                - `":end_of_day:"`: `start_datetime`의 하루 중 마지막 시점 (기본값)
        cancel_type: str
            조회 유형. `"ALL"`이면 반품과 취소 유형을 각각 조회한다.
                - `"RETURN"`: 반품
                - `"CANCEL"`: 취소
        status: str | Sequence[str]
            반품 상태. `"ALL"` 값으로 모든 상태를 조회하거나, 개별 상태 코드를 단일 값 또는 배열로 입력할 수 있다.
                - `"RU"`: 출고중지요청
                - `"UC"`: 반품접수
                - `"CC"`: 반품완료
                - `"PR"`: 쿠팡확인요청
        max_retries: int
            동시 요청 제한이 발생할 경우 최대 재시도 횟수. 기본값은 `5`

        Returns
        -------
        dict | list[dict]
            반품 및 취소 요청 목록. 검색 기간 및 상태/유형에 따라 반환 타입이 다르다.
                - 검색 기간 및 상태/유형 조합이 1개일 때 -> `dict`
                - 검색 기간 및 상태/유형 조합이 여러 개일 때 -> `list[dict]`
        """
        context = self.build_request_context(start_datetime, end_datetime, cancel_type, status)
        return (self.request_each(self.request_json_until_success, context=context)
                .partial(vendor_id=self.vendor_id, max_retries=max_retries)
                .run())

    def build_request_context(
            self,
            start_datetime: dt.datetime | str,
            end_datetime: dt.datetime | str | Literal[":end_of_day:"] = ":end_of_day:",
            cancel_type: Literal["ALL", "RETURN", "CANCEL"] = "RETURN",
            status: Literal["ALL", "RU", "UC", "CC", "PR"] | Sequence[str] | None = "ALL",
        ) -> dict | list[dict]:
        """검색 기간 및 상태/유형을 규칙에 따라 매개변수 목록 또는 단일 키워드 인자로 변환한다."""
        from linkmerce.core.coupang.api.common import split_datetime_context
        ranges = split_datetime_context(start_datetime, end_datetime, self.datetime_format, days_interval=30)
        ranges = [ranges] if isinstance(ranges, dict) else list(ranges)

        context = list()
        if cancel_type in ("ALL", "RETURN"):
            if isinstance(status, str):
                status = list(self.status.keys()) if status == "ALL" else [status]
            context += [kwargs | {"cancel_type": "RETURN", "status": s}
                        for kwargs in ranges for s in (status or ["CC"])]
        if cancel_type in ("ALL", "CANCEL"):
            context += [kwargs | {"cancel_type": "CANCEL"} for kwargs in ranges]
        return context[0] if len(context) == 1 else context

    def build_request_params(
            self,
            start_datetime: dt.datetime,
            end_datetime: dt.datetime,
            cancel_type: Literal["RETURN", "CANCEL"] = "CANCEL",
            status: Literal["RU", "UC", "CC", "PR"] | None = None,
            **kwargs
        ) -> dict:
        return {
            "searchType": "timeFrame",
            "createdAtFrom": start_datetime.strftime("%Y-%m-%dT%H:%M"),
            "createdAtTo": end_datetime.strftime("%Y-%m-%dT%H:%M"),
            **({"status": status} if (cancel_type != "CANCEL") and status else dict()),
            "cancelType": cancel_type,
        }

    @property
    def cancel_type(self) -> dict[str, str]:
        """조회 유형 코드와 한글명 매핑을 반환한다."""
        return {"RETURN": "반품", "CANCEL": "취소"}

    @property
    def status(self) -> dict[str, str]:
        """반품 상태 코드와 한글명 매핑을 반환한다."""
        return {"RU": "출고중지요청", "UC": "반품접수", "CC": "반품완료", "PR": "쿠팡확인요청"}


class ReturnRequestDetail(CoupangApi):
    """쿠팡 마켓플레이스 반품 접수번호별 반품요청 정보를 수집하는 클래스.

    - **Menu**: 반품 API > 반품요청 단건 조회
    - **API**: https://api-gateway.coupang.com/v2/providers/openapi/apis/api/v6/vendors/{vendor_id}/returnRequests/{receipt_id}
    - **Docs**: https://developers.coupang.com/ko/api/returns/query-one-return-request

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    access_key: str
        쿠팡 Open API 액세스 키
    secret_key: str
        쿠팡 Open API 시크릿 키
    vendor_id: str
        업체 코드

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간(초). 기본값은 `0.3`
    tqdm_options: dict | None
        반복 요청 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    path = "/v2/providers/openapi/apis/api/v6/vendors/{vendor_id}/returnRequests/{receipt_id}"
    default_options = {"RequestEach": {"request_delay": 0.3}}

    @CoupangApi.with_session
    def extract(
            self,
            receipt_id: int | str | Sequence[int | str],
            max_retries: int = 5,
            **kwargs
        ) -> dict | list[dict]:
        """반품 접수번호별 요청 정보를 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        receipt_id: int | str | Sequence[int | str]
            반품 접수번호. 단일 값 또는 배열. 취소 접수번호는 조회할 수 없다.
        max_retries: int
            동시 요청 제한이 발생할 경우 최대 재시도 횟수. 기본값은 `5`

        Returns
        -------
        dict | list[dict]
            반품요청 단건 또는 목록. `receipt_id` 타입에 따라 반환 타입이 다르다.
                - `receipt_id`가 `int | str` 타입일 때 -> `dict`
                - `receipt_id`가 `Sequence[int | str]` 타입일 때 -> `list[dict]`
        """
        return (self.request_each(self.request_json_until_success)
                .partial(vendor_id=self.vendor_id, max_retries=max_retries)
                .expand(receipt_id=receipt_id)
                .run())

    def build_request_message(self, receipt_id: int | str, **kwargs) -> dict:
        """반품 접수번호를 반품요청 단건 조회 경로에 붙인다."""
        path = self.path.format(vendor_id=self.vendor_id, receipt_id=str(receipt_id))
        kwargs["url"] = self.concat_path(self.origin, path)
        return super().build_request_message(**kwargs)


class ReturnWithdraw(CoupangApi):
    """쿠팡 마켓플레이스 반품철회 이력을 수집하는 클래스.

    - **Menu**: 반품 API > 반품철회 이력 기간별 조회
    - **API**: https://api-gateway.coupang.com/v2/providers/openapi/apis/api/v4/vendors/{vendor_id}/returnWithdrawRequests
    - **Docs**: https://developers.coupang.com/ko/api/returns/query-return-return-withdrawal-history-by-date-range

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

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachCursor` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        검색 기간별 요청 간 대기 시간(초). 기본값은 `0.3`
    tqdm_options: dict | None
        반복 요청 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    path = "/v2/providers/openapi/apis/api/v4/vendors/{vendor_id}/returnWithdrawRequests"
    date_format = "%Y-%m-%d"
    default_options = {
        "CursorAll": {"request_delay": 0.3},
        "RequestEachCursor": {"request_delay": 0.3},
    }

    @CoupangApi.with_session
    def extract(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            max_retries: int = 5,
            **kwargs
        ) -> list[dict]:
        """검색 기간에 대한 마켓플레이스 반품철회 이력을 JSON 형식으로 반환한다.

        Parameters
        ----------
        start_date: dt.date | str
            조회 시작일. `dt.date` 객체 또는 `"yyyy-MM-dd"` 형식의 문자열을 입력한다.
        end_date: dt.date | str
            조회 종료일. `dt.date` 객체 또는 `"yyyy-MM-dd"` 형식의 문자열을 입력한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
        max_retries: int
            동시 요청 제한이 발생할 경우 최대 재시도 횟수. 기본값은 `5`

        Returns
        -------
        list[dict]
            반품철회 이력. 조회 기간에 따라 반환 타입이 다르다.
                - `start_date`부터 `end_date` 사이가 7일 이내일 때 -> `dict`
                - `start_date`부터 `end_date` 사이가 7일을 초과할 때 -> `list[dict]`
        """
        from linkmerce.core.coupang.api.common import split_datetime_context
        end_date = start_date if end_date == ":start_date:" else end_date
        context = split_datetime_context(start_date, end_date, self.date_format, days_interval=7)
        return (self.request_each_cursor(self.request_json_until_success, context=context)
                .partial(max_retries=max_retries)
                .all_cursor(self.get_next_cursor, next_cursor=1)
                .run())

    def get_next_cursor(self, response: dict, **context) -> str | None:
        """다음 페이지를 가리키는 `nextPageIndex` 커서를 추출한다."""
        return response.get("nextPageIndex") or None

    def build_request_params(
            self,
            start_datetime: dt.date | dt.datetime,
            end_datetime: dt.date | dt.datetime,
            next_cursor: int | str = 1,
            **kwargs
        ) -> dict:
        return {
            "dateFrom": start_datetime.strftime(self.date_format),
            "dateTo": end_datetime.strftime(self.date_format),
            "pageIndex": next_cursor,
            "sizePerPage": 100,
        }


class ExchangeRequest(CoupangApi):
    """쿠팡 마켓플레이스 교환요청 목록을 수집하는 클래스.

    - **Menu**: 교환 API > 교환요청 목록 조회
    - **API**: https://api-gateway.coupang.com/v2/providers/openapi/apis/api/v4/vendors/{vendor_id}/exchangeRequests
    - **Docs**: https://developers.coupang.com/ko/api/exchanges/query-a-list-of-exchange-requests
    - **Referer**: https://wing.coupang.com/tenants/sfl-portal/exchange/list

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

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachCursor` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        검색 기간 및 상태별 요청 간 대기 시간(초). 기본값은 `0.3`
    tqdm_options: dict | None
        반복 요청 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    path = "/v2/providers/openapi/apis/api/v4/vendors/{vendor_id}/exchangeRequests"
    datetime_format = "%Y-%m-%dT%H:%M:%S"
    default_options = {
        "CursorAll": {"request_delay": 0.3},
        "RequestEachCursor": {"request_delay": 0.3},
    }

    @CoupangApi.with_session
    def extract(
            self,
            start_datetime: dt.datetime | str,
            end_datetime: dt.datetime | str | Literal[":end_of_day:"] = ":end_of_day:",
            status: Literal["RECEIPT", "PROGRESS", "SUCCESS", "REJECT", "CANCEL"] | Sequence[str] | None = None,
            order_id: int | str | None = None,
            max_retries: int = 5,
            **kwargs
        ) -> list[dict]:
        """검색 기간에 대한 마켓플레이스 교환요청 목록을 JSON 형식으로 반환한다.

        Parameters
        ----------
        start_datetime: dt.datetime | str
            검색 시작 일시. `dt.datetime` 객체 또는 "yyyy-MM-ddTHH:mm:ss" 형식의 문자열을 입력한다.
        end_datetime: dt.datetime | str
            검색 종료 일시. `dt.datetime` 객체 또는 "yyyy-MM-ddTHH:mm:ss" 형식의 문자열을 입력한다.
                - `":end_of_day:"`: `start_datetime`의 해당 날 마지막 시각 (기본값)
        status: str | Sequence[str] | None
            교환진행상태. 생략하여 모든 상태를 조회하거나, 개별 상태 코드를 단일 값 또는 배열로 입력할 수 있다.
                - `"RECEIPT"`: 접수
                - `"PROGRESS"`: 진행
                - `"SUCCESS"`: 완료
                - `"REJECT"`: 불가
                - `"CANCEL"`: 철회
        order_id: int | str | None
            검색할 주문번호를 입력할 수 있다.
        max_retries: int
            동시 요청 제한이 발생할 경우 최대 재시도 횟수. 기본값은 `5`

        Returns
        -------
        list[dict]
            교환요청 목록. 조회 기간 및 상태에 따라 반환 타입이 다르다.
                - `start_date`부터 `end_date` 사이가 7일 이내이고 `status`가 단일 값일 때 -> `dict`
                - `start_date`부터 `end_date` 사이가 7일을 초과하거나 `status`가 배열일 때 -> `list[dict]`
            조회 기간 및 교환 상태별 모든 페이지의 원본 응답 목록
        """
        from linkmerce.core.coupang.api.common import split_datetime_context
        context = split_datetime_context(start_datetime, end_datetime, self.datetime_format, days_interval=7)
        return (self.request_each_cursor(self.request_json_until_success, context=context)
                .partial(order_id=order_id, max_retries=max_retries)
                .expand(status=status)
                .all_cursor(self.get_next_cursor)
                .run())

    def get_next_cursor(self, response: dict, **context) -> str | None:
        """다음 페이지를 가리키는 `nextToken` 커서를 추출한다."""
        return response.get("nextToken") or None

    def build_request_params(
            self,
            start_datetime: dt.datetime,
            end_datetime: dt.datetime,
            status: str | None = None,
            order_id: int | str | None = None,
            next_cursor: str | None = None,
            **kwargs
        ) -> dict:
        return {
            "createdAtFrom": start_datetime.strftime(self.datetime_format),
            "createdAtTo": end_datetime.strftime(self.datetime_format),
            **({"status": status} if status is not None else dict()),
            **({"orderId": order_id} if order_id is not None else dict()),
            **({"nextToken": next_cursor} if next_cursor is not None else dict()),
        }

    @property
    def status(self) -> dict[str, str]:
        """교환 상태 코드와 한글명을 매핑해 반환한다."""
        return {
            "RECEIPT": "접수", "PROGRESS": "진행", "SUCCESS": "완료",
            "REJECT": "불가", "CANCEL": "철회",
        }
