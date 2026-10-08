from __future__ import annotations
import datetime as dt

from linkmerce.core.coupang.api import CoupangApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Iterable


class Order(CoupangApi):
    """쿠팡 Open API로 마켓플레이스 발주서 목록 조회 결과를 수집하는 클래스.

    - **Menu**: 배송 / 주문 API > 발주서 목록 조회(분단위 전체)
    - **API**: https://api-gateway.coupang.com/v2/providers/openapi/apis/api/v5/vendors/{vendor_id}/ordersheets
    - **Docs**: https://developers.coupang.com/ko/api/shipments/po-list-query-by-minute

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
        검색 기간 및 발주서 상태별 요청 간 대기 시간(초). 기본값은 `0.3`
    tqdm_options: dict | None
        반복 요청 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    path = "/v2/providers/openapi/apis/api/v5/vendors/{vendor_id}/ordersheets"
    datetime_format = "%Y-%m-%dT%H:%M%z"
    default_options = {"RequestEach": {"request_delay": 0.3}}

    @CoupangApi.with_session
    def extract(
            self,
            start_datetime: dt.datetime | str,
            end_datetime: dt.datetime | str | Literal[":end_of_day:", ":max_window:"] = ":end_of_day:",
            status: Literal["ALL", "ACCEPT", "INSTRUCT", "DEPARTURE", "DELIVERING", "FINAL_DELIVERY", "NONE_TRACKING"] | Iterable[str] = "ALL",
            **kwargs
        ) -> dict | list[dict]:
        """검색 기간에 대한 마켓플레이스 발주서 목록을 JSON 형식으로 반환한다.

        Parameters
        ----------
        start_datetime: dt.datetime | str
            검색 시작 일시. `dt.datetime` 객체 또는 "yyyy-mm-ddT00:00+09:00" 형태의 문자열을 입력한다.
        end_datetime: dt.datetime | str
            검색 종료 일시. `dt.datetime` 객체 또는 "yyyy-mm-ddT00:00+09:00" 형태의 문자열을 입력한다.
                - `":end_of_day:"`: `start_datetime`의 하루 중 마지막 시점 (기본값)
                - `":max_window:"`: `start_datetime`으로부터 23시간 59분이 지난 시점
        status: str | Iterable[str]
            발주서 상태. `"ALL"` 값으로 모든 상태를 조회하거나, 개별 상태 코드를 단일 값 또는 배열로 입력할 수 있다.
                - `"ACCEPT"`: 결제완료
                - `"INSTRUCT"`: 상품준비중
                - `"DEPARTURE"`: 배송지시
                - `"DELIVERING"`: 배송중
                - `"FINAL_DELIVERY"`: 배송완료
                - `"NONE_TRACKING"`: 업체 직접 배송

        Returns
        -------
        dict | list[dict]
            발주서 목록. 조회 기간 또는 발주서 상태에 따라 반환 타입이 다르다.
                - 조회 구간이 하나이고 `status`가 `"ALL"`을 제외한 문자열일 때 -> `dict`
                - 조회 구간이 여러 개이거나 `status`가 배열 또는 `"ALL"`일 때 -> `list[dict]`
        """
        from linkmerce.core.coupang.api.common import split_datetime_context
        context = split_datetime_context(start_datetime, end_datetime, self.datetime_format, days_interval=1)
        if isinstance(status, str) and (status == "ALL"):
            status = list(self.status.keys())

        return (self.request_each(self.request_json, context=context)
                .expand(status=status)
                .run())

    def build_request_params(
            self,
            start_datetime: dt.datetime,
            end_datetime: dt.datetime,
            status: Literal["ACCEPT", "INSTRUCT", "DEPARTURE", "DELIVERING", "FINAL_DELIVERY", "NONE_TRACKING"],
            **kwargs
        ) -> dict:
        from linkmerce.core.coupang.api.common import strftime
        return {
            "createdAtFrom": strftime(start_datetime),
            "createdAtTo": strftime(end_datetime),
            "status": status,
            "searchType": "timeFrame",
        }

    @property
    def status(self) -> dict[str, str]:
        """발주서 상태 코드와 한글명 매핑을 반환한다."""
        return {
            "ACCEPT": "결제완료", "INSTRUCT": "상품준비중", "DEPARTURE": "배송지시",
            "DELIVERING": "배송중", "FINAL_DELIVERY": "배송완료",
            "NONE_TRACKING": "업체 직접 배송(배송 연동 미적용), 추적불가"
        }


class OrderDetail(CoupangApi):
    """쿠팡 주문번호별 또는 배송번호별 마켓플레이스 발주서를 수집하는 클래스.

    ### 주문번호 API
    - **Menu**: 배송 / 주문 API > 발주서 단건 조회(orderId)
    - **API**: https://api-gateway.coupang.com/v2/providers/openapi/apis/api/v5/vendors/{vendor_id}/{order_id}/ordersheets
    - **Docs**: https://developers.coupang.com/ko/api/shipments/single-po-query-using-orderid

    ### 배송번호 API
    - **Menu**: 배송 / 주문 API > 발주서 단건 조회(shipmentBoxId)
    - **API**: https://api-gateway.coupang.com/v2/providers/openapi/apis/api/v5/vendors/{vendor_id}/ordersheets/{shipment_box_id}
    - **Docs**: https://developers.coupang.com/ko/api/shipments/single-po-query-using-shipmentboxid

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
        주문별 요청 간 대기 시간(초). 기본값은 `0.3`
    tqdm_options: dict | None
        반복 요청 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    path = "/v2/providers/openapi/apis/api/v5/vendors/{vendor_id}/{query_id}/ordersheets"
    default_options = {"RequestEach": {"request_delay": 0.3}}

    @CoupangApi.with_session
    def extract(
            self,
            query_id: int | str | Iterable[int | str],
            query_type: Literal["order_id", "shipment_box_id"] = "order_id",
            **kwargs
        ) -> dict | list[dict]:
        """주문번호별 또는 배송번호별 마켓플레이스 발주서를 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        query_id: int | str | Iterable[int | str]
            주문번호 또는 배송번호. 단일 값 또는 배열
        query_type: str
            번호 유형
                - `"order_id"`: 주문번호 (기본값)
                - `"shipment_box_id"`: 배송번호

        Returns
        -------
        dict | list[dict]
            발주서 단건 또는 목록. `query_id` 타입에 따라 반환 타입이 다르다.
                - `query_id`가 `int | str` 타입일 때 -> `dict`
                - `query_id`가 `Iterable[int | str]` 타입일 때 -> `list[dict]`
        """
        query_id = query_id if isinstance(query_id, (int, str)) else list(query_id)
        return (self.request_each(self.request_json)
                .partial(query_path=self.get_query_path(query_type))
                .expand(query_id=query_id)
                .run())

    def get_query_path(self, query_type: Literal["order_id", "shipment_box_id"] = "order_id") -> str:
        """`query_type`에 맞는 발주서 단건 조회 경로를 반환한다."""
        if query_type == "order_id":
            return "/v2/providers/openapi/apis/api/v5/vendors/{vendor_id}/{query_id}/ordersheets"
        elif query_type == "shipment_box_id":
            return "/v2/providers/openapi/apis/api/v5/vendors/{vendor_id}/ordersheets/{query_id}"
        raise ValueError("The query_type must be order_id or shipment_box_id.")

    def build_request_message(self, query_id: int | str, query_path: str, **kwargs) -> dict:
        """주문번호 또는 배송번호를 발주서 단건 조회 경로에 붙인다."""
        path = query_path.format(vendor_id=self.vendor_id, query_id=str(query_id))
        kwargs["url"] =  self.concat_path(self.origin, path)
        return super().build_request_message(**kwargs)
