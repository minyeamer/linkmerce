from __future__ import annotations
from linkmerce.core.smartstore.api import SmartstoreApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Iterable
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class Order(SmartstoreApi):
    """네이버 커머스 API로 조건형 상품 주문 상세 내역 조회 결과를 수집하는 클래스.

    `RequestEachCursor` Task를 사용하여 일별 주문 데이터를 조회한다."""

    method = "GET"
    version = "v1"
    path = "/pay-order/seller/product-orders"
    date_format = "%Y-%m-%d"

    @property
    def default_options(self) -> dict:
        return {"CursorAll": {"request_delay": 1}}

    @SmartstoreApi.with_session
    @SmartstoreApi.with_token
    def extract(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            range_type: str = "PAYED_DATETIME",
            product_order_status: Iterable[str] = list(),
            claim_status: Iterable[str] = list(),
            place_order_status: str = list(),
            page_start: int = 1,
            max_retries: int = 5,
            **kwargs
        ) -> JsonObject:
        """일별 조건형 상품 주문 상세 내역 조회 결과를 수집해 JSON 형식으로 반환한다."""
        partial = {"range_type": range_type, "product_order_status": product_order_status, "claim_status": claim_status, "place_order_status": place_order_status, "max_retries": max_retries}

        return (self.request_each_cursor(self.request_json_until_success)
                .partial(**partial, channel_seq=kwargs.get("channel_seq"))
                .expand(date=self.generate_date_range(start_date, end_date, freq='D'))
                .all_cursor(self.get_next_cursor, next_cursor=page_start)
                .run())

    def get_next_cursor(self, response: JsonObject, **context) -> int:
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
            place_order_status: str = list(),
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
            "placeOrderStatusType": place_order_status,
            "page": next_cursor,
            "pageSize": page_size,
        }

    @property
    def range_type(self) -> dict[str, str]:
        """주문 조회 기간 유형 매핑을 반환한다."""
        return {
            "PAYED_DATETIME": "결제 일시", "ORDERED_DATETIME": "주문 일시", "DISPATCHED_DATETIME": "발송 처리 일시",
            "PURCHASE_DECIDED_DATETIME": "구매 확정 일시", "CLAIM_REQUESTED_DATETIME": "클레임 요청 일시",
            "CLAIM_COMPLETED_DATETIME": "클레임 완료 일시", "COLLECT_COMPLETED_DATETIME": "수거 완료 일시",
            "GIFT_RECEIVED_DATETIME": "선물 수락 일시", "HOPE_DELIVERY_INFO_CHANGED_DATETIME": "배송 희망일 변경 일시"
        }

    @property
    def product_order_status(self) -> dict[str, str]:
        """상품주문 상태 코드별 한글 명칭 매핑을 반환한다."""
        return {
            "PAYMENT_WAITING": "결제 대기", "PAYED": "결제 완료", "DELIVERING": "배송 중", "DELIVERED": "배송 완료",
            "PURCHASE_DECIDED": "구매 확정", "EXCHANGED": "교환", "CANCELED": "취소", "RETURNED": "반품",
            "CANCELED_BY_NOPAYMENT": "미결제 취소"
        }

    @property
    def claim_status(self) -> dict[str, str]:
        """클레임 상태 코드별 한글 명칭 매핑을 반환한다."""
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
        """발주 상태 코드별 한글 명칭 매핑을 반환한다."""
        return {"NOT_YET": "발주 미확인", "OK": "발주 확인", "CANCEL": "발주 확인 해제"}


class OrderStatus(SmartstoreApi):
    """네이버 커머스 API로 변경 상품 주문 내역 조회 결과를 수집하는 클래스.

    `RequestEachCursor` Task를 사용하여 일별 주문 변경 데이터를 조회한다."""

    method = "GET"
    version = "v1"
    path = "/pay-order/seller/product-orders/last-changed-statuses"
    datetime_format = "%Y-%m-%dT%H:%M:%S.%f%z"

    @property
    def default_options(self) -> dict:
        return {"CursorAll": {"request_delay": 1}}

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
        ) -> JsonObject:
        """일별 변경 상품 주문 내역 조회 결과를 수집해 JSON 형식으로 반환한다."""
        return (self.request_each_cursor(self.request_json_until_success)
                .partial(last_changed_type=last_changed_type, channel_seq=channel_seq, max_retries=max_retries)
                .expand(date=self.generate_date_range(start_date, end_date, freq='D'))
                .all_cursor(self.get_next_cursor, next_cursor=dict())
                .run())

    def get_next_cursor(self, response: JsonObject, date: dt.date, **context) -> dict[str, str]:
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
        """상태 변경 유형별 한글 명칭 매핑을 반환한다."""
        return {
            "PAY_WAITING": "결제대기", "PAYED": "결제완료", "EXCHANGE_OPTION": "옵션변경", "DELIVERY_ADDRESS_CHANGED": "배송지변경",
            "GIFT_RECEIVED": "선물수락", "CLAIM_REJECTED": "클레임철회", "DISPATCHED": "발송처리", "CLAIM_REQUESTED": "클레임요청",
            "COLLECT_DONE": "수거완료", "CLAIM_COMPLETED": "클레임완료", "PURCHASE_DECIDED": "구매확정",
            "HOPE_DELIVERY_INFO_CHANGED": "배송희망일변경", "CLAIM_REDELIVERING": "교환재배송처리"
        }
