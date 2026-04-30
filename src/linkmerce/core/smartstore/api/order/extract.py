from __future__ import annotations
from linkmerce.core.smartstore.api import SmartstoreApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Iterable
    import datetime as dt


class Order(SmartstoreApi):
    """네이버 커머스 API로 상품 주문 내역 조회 결과를 수집하는 클래스.

    - **Menu**: 판매관리 > 주문통합검색 (조건형 상품 주문 상세 내역 조회)
    - **API**: https://api.commerce.naver.com/external/v1/pay-order/seller/product-orders
    - **Docs**: https://apicenter.commerce.naver.com/ko/basic/commerce-api
    - **Referer**: https://sell.smartstore.naver.com/#/naverpay/manage/order

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    client_id: str
        커머스 API 애플리케이션 ID
    client_secret: str
        커머스 API 애플리케이션 시크릿

    **NOTE** 인스턴스 생성 시 `options` 인자로 `CursorAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        조회 기간 내 커서 요청 간 대기 시간(초). 기본값은 `1`
    tqdm_options: dict | None
        반복 요청 작업 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    version = "v1"
    path = "/pay-order/seller/product-orders"
    date_format = "%Y-%m-%d"
    default_options = {"CursorAll": {"request_delay": 1}}

    @SmartstoreApi.with_session
    @SmartstoreApi.with_token
    def extract(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            range_type: str = "PAYED_DATETIME",
            product_order_status: Iterable[str] = list(),
            claim_status: Iterable[str] = list(),
            place_order_status: str | None = None,
            page_start: int = 1,
            max_retries: int = 5,
            **kwargs
        ) -> dict | list[dict]:
        """상품 주문 내역을 일별로 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        start_date: dt.date | str
            조회 기준의 시작 일시. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
        end_date: dt.date | str
            조회 기준의 종료 일시. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
        range_type: str
            조회 기준 유형. `range_type` 속성의 키를 전달할 수 있다. 기본값은 결제일시(`"PAYED_DATETIME"`)
        product_order_status: Iterable[str]
            상품 주문 상태 목록. `product_order_status` 속성의 키를 하나 이상 전달할 수 있다.
        claim_status: Iterable[str]
            클레임 상태 목록. `claim_status` 속성의 키를 하나 이상 전달할 수 있다.
        place_order_status: str | None
            발주 상태. `place_order_status` 속성의 키를 전달할 수 있다.
        page_start: int
            페이지 번호. 기본값은 `1`
        max_retries: int
            동시 요청 제한이 발생할 경우 최대 재시도 횟수. 기본값은 `5`

        Returns
        -------
        dict | list[dict]
            상품 주문 내역. 조회 기간에 따라 반환 타입이 다르다.
                - `start_date`와 `end_date`가 동일할 때 -> `dict`
                - `start_date`와 `end_date`가 다를 때 -> `list[dict]`
        """
        return (self.request_each_cursor(self.request_json_until_success)
                .partial(
                    range_type = range_type,
                    product_order_status = product_order_status,
                    claim_status = claim_status,
                    place_order_status = place_order_status,
                    max_retries = max_retries,
                    channel_seq = kwargs.get("channel_seq"),
                ).expand(date=self.generate_date_range(start_date, end_date, freq='D'))
                .all_cursor(self.get_next_cursor, next_cursor=page_start)
                .run())

    def get_next_cursor(self, response: dict, **context) -> int:
        """다음 페이지 커서를 추출한다."""
        from linkmerce.utils.nested import hier_get
        pagination = hier_get(response, "data.pagination") or dict()
        return (pagination.get("page") + 1) if pagination.get("hasNext") else None

    def build_request_params(
            self,
            date: dt.date,
            range_type: str = "PAYED_DATETIME",
            product_order_status: Iterable[str] = list(),
            claim_status: Iterable[str] = list(),
            place_order_status: str | None = None,
            next_cursor: int = 1,
            page_size: int = 300,
            **kwargs
        ) -> dict:
        return {
            "from": f"{date}T00:00:00.000+09:00",
            "to": f"{date}T23:59:59.999+09:00",
            "rangeType": range_type,
            "productOrderStatuses": ','.join(product_order_status),
            "claimStatuses": ','.join(claim_status),
            "placeOrderStatusType": (place_order_status or list()),
            "page": next_cursor,
            "pageSize": page_size,
        }

    @property
    def range_type(self) -> dict[str, str]:
        """조회 기준 유형 코드와 한글명 매핑을 반환한다."""
        return {
            "PAYED_DATETIME": "결제 일시", "ORDERED_DATETIME": "주문 일시", "DISPATCHED_DATETIME": "발송 처리 일시",
            "PURCHASE_DECIDED_DATETIME": "구매 확정 일시", "CLAIM_REQUESTED_DATETIME": "클레임 요청 일시",
            "CLAIM_COMPLETED_DATETIME": "클레임 완료 일시", "COLLECT_COMPLETED_DATETIME": "수거 완료 일시",
            "GIFT_RECEIVED_DATETIME": "선물 수락 일시", "HOPE_DELIVERY_INFO_CHANGED_DATETIME": "배송 희망일 변경 일시"
        }

    @property
    def product_order_status(self) -> dict[str, str]:
        """상품 주문 상태 코드와 한글명 매핑을 반환한다."""
        return {
            "PAYMENT_WAITING": "결제 대기", "PAYED": "결제 완료", "DELIVERING": "배송 중", "DELIVERED": "배송 완료",
            "PURCHASE_DECIDED": "구매 확정", "EXCHANGED": "교환", "CANCELED": "취소", "RETURNED": "반품",
            "CANCELED_BY_NOPAYMENT": "미결제 취소"
        }

    @property
    def claim_status(self) -> dict[str, str]:
        """클레임 상태 코드와 한글명 매핑을 반환한다."""
        return {
            "CANCEL_REQUEST": "취소 요청", "CANCELING": "취소 처리 중", "CANCEL_DONE": "취소 처리 완료", "CANCEL_REJECT": "취소 철회",
            "RETURN_REQUEST": "반품 요청", "EXCHANGE_REQUEST": "교환 요청", "COLLECTING": "수거 처리 중", "COLLECT_DONE": "수거 완료",
            "EXCHANGE_REDELIVERING": "교환 재배송 중", "RETURN_DONE": "반품 완료", "EXCHANGE_DONE": "교환 완료",
            "RETURN_REJECT": "반품 철회", "EXCHANGE_REJECT": "교환 철회", "PURCHASE_DECISION_HOLDBACK": "구매 확정 보류",
            "PURCHASE_DECISION_REQUEST": "구매 확정 요청", "PURCHASE_DECISION_HOLDBACK_RELEASE": "구매 확정 보류 해제",
            "ADMIN_CANCELING": "직권 취소 중", "ADMIN_CANCEL_DONE": "직권 취소 완료", "ADMIN_CANCEL_REJECT": "직권 취소 철회", 
        }

    @property
    def place_order_status(self) -> dict[str, str]:
        """발주 상태 코드와 한글명 매핑을 반환한다."""
        return {"NOT_YET": "발주 미확인", "OK": "발주 확인", "CANCEL": "발주 확인 해제"}


class OrderStatus(SmartstoreApi):
    """네이버 커머스 API로 변경 상품 주문 내역 조회 결과를 수집하는 클래스.

    - **Menu**: 판매관리 > 주문통합검색 (변경 상품 주문 내역 조회)
    - **API**: https://api.commerce.naver.com/external/v1/pay-order/seller/product-orders/last-changed-statuses
    - **Docs**: https://apicenter.commerce.naver.com/docs/commerce-api/current/seller-get-last-changed-status-pay-order-seller
    - **Referer**: https://sell.smartstore.naver.com/#/naverpay/manage/order

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    client_id: str
        커머스 API 애플리케이션 ID
    client_secret: str
        커머스 API 애플리케이션 시크릿

    **NOTE** 인스턴스 생성 시 `options` 인자로 `CursorAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        조회 기간 내 커서 요청 간 대기 시간(초). 기본값은 `1`
    tqdm_options: dict | None
        반복 요청 작업 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    version = "v1"
    path = "/pay-order/seller/product-orders/last-changed-statuses"
    datetime_format = "%Y-%m-%dT%H:%M:%S.%f%z"
    default_options = {"CursorAll": {"request_delay": 1}}

    @SmartstoreApi.with_session
    @SmartstoreApi.with_token
    def extract(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            last_changed_type: str | None = None,
            channel_seq: int | str | None = None,
            max_retries: int = 5,
            **kwargs
        ) -> dict | list[dict]:
        """변경 상품 주문 내역을 일별로 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        start_date: dt.date | str
            조회 기준의 시작 일시. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
        end_date: dt.date | str | Literal[":start_date:"]
            조회 기준의 종료 일시. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
        last_changed_type: str | None
            최종 변경 구분. `last_changed_type` 속성의 키를 전달할 수 있다.
        channel_seq: int | str | None
            채널 번호. 조회 시점에는 사용되지 않고 파서 함수에 전달된다.
        max_retries: int
            동시 요청 제한이 발생할 경우 최대 재시도 횟수. 기본값은 `5`

        Returns
        -------
        dict | list[dict]
            변경 상품 주문 내역. 조회 기간에 따라 반환 타입이 다르다.
                - `start_date`와 `end_date`가 동일할 때 -> `dict`
                - `start_date`와 `end_date`가 다를 때 -> `list[dict]`
        """
        return (self.request_each_cursor(self.request_json_until_success)
                .partial(last_changed_type=last_changed_type, channel_seq=channel_seq, max_retries=max_retries)
                .expand(date=self.generate_date_range(start_date, end_date, freq='D'))
                .all_cursor(self.get_next_cursor, next_cursor=dict())
                .run())

    def get_next_cursor(self, response: dict, date: dt.date, **context) -> dict[str, str]:
        """다음 페이지 `moreFrom` 커서를 추출한다."""
        from linkmerce.utils.nested import hier_get
        more = hier_get(response, "data.more") or dict()
        if more.get("moreFrom") and ((more.get("moreFrom") or str()) <= f"{date}T23:59:59.999+09:00"):
            return more

    def build_request_params(
            self,
            date: dt.date,
            last_changed_type: str | None = None,
            next_cursor: dict[str, str] = dict(),
            limit_count: int = 300,
            **kwargs
        ) -> dict:
        return {
            "lastChangedFrom": next_cursor.get("moreFrom") or f"{date}T00:00:00.000+09:00",
            "lastChangedTo": f"{date}T23:59:59.999+09:00",
            **({"lastChangedType": last_changed_type} if last_changed_type is not None else dict()),
            **({"moreSequence": next_cursor["moreSequence"]} if "moreSequence" in next_cursor else dict()),
            "limitCount": limit_count,
        }

    @property
    def last_changed_type(self) -> dict[str, str]:
        """최종 변경 구분 코드와 한글명 매핑을 반환한다."""
        return {
            "PAY_WAITING": "결제대기", "PAYED": "결제완료", "EXCHANGE_OPTION": "옵션변경", "DELIVERY_ADDRESS_CHANGED": "배송지변경",
            "GIFT_RECEIVED": "선물수락", "CLAIM_REJECTED": "클레임철회", "DISPATCHED": "발송처리", "CLAIM_REQUESTED": "클레임요청",
            "COLLECT_DONE": "수거완료", "CLAIM_COMPLETED": "클레임완료", "PURCHASE_DECIDED": "구매확정",
            "HOPE_DELIVERY_INFO_CHANGED": "배송희망일변경", "CLAIM_REDELIVERING": "교환재배송처리"
        }
