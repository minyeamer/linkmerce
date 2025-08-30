from __future__ import annotations
from linkmerce.core.smartstore.api import SmartstoreAPI

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Iterable
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class Order(SmartstoreAPI):
    method = "GET"
    version = "v1"
    path = "/pay-order/seller/product-orders"
    date_format = "%Y-%m-%d"

    @property
    def default_options(self) -> dict:
        return dict(CursorAll = dict(delay=1))

    @SmartstoreAPI.with_session
    @SmartstoreAPI.with_token
    def extract(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            range_type: Literal["PAYED_DATETIME","ORDERING_CONFIRM","DELIVERY_OPERATED","DELIVERY_COMPLETED","PURCHASE_DECISION_COMPLETED"] = "PAYED_DATETIME",
            product_order_status: Iterable[str] = list(),
            claim_status: Iterable[str] = list(),
            place_order_status: str = list(),
            page_start: int = 1,
            retry_count: int = 5,
            **kwargs
        ) -> JsonObject:
        partial = dict(range_type=range_type, product_order_status=product_order_status,
            claim_status=claim_status, place_order_status=place_order_status, retry_count=retry_count)

        return (self.request_each_cursor(self.request_json_until_success)
                .partial(**partial)
                .expand(date=self.generate_date_range(start_date, end_date, freq='D'))
                .all_cursor(self.get_next_cursor, next_cursor=page_start)
                .run())

    def get_next_cursor(self, response: JsonObject, **context) -> int:
        from linkmerce.utils.map import hier_get
        pagination = hier_get(response, ["data","pagination"]) or dict()
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
    def range_type(self) -> dict[str,str]:
        return {
            "PAY_COMPLETED": "결제일", "ORDERING_CONFIRM": "발주확인일", "DELIVERY_OPERATED": "발송처리일",
            "DELIVERY_COMPLETED": "배송완료일", "PURCHASE_DECISION_COMPLETED": "구매확정일"
        }

    @property
    def product_order_status(self) -> dict[str,str]:
        return {
            "PAYMENT_WAITING": "결제대기", "PAYED": "결제완료", "WAITING_DISPATCH": "발송대기", "DELIVERING": "배송중",
            "DELIVERED": "배송완료", "PURCHASE_DECIDED": "구매확정", "CANCELED": "취소", "CANCELED_BY_NOPAYMENT": "미결제취소",
            "EXCHANGED": "교환", "RETURNED": "반품"
        }

    @property
    def claim_status(self) -> dict[str,str]:
        return {
            "CANCEL_REQUEST": "취소요청", "CANCELING": "취소중", "CANCEL_DONE": "취소완료", "CANCEL_REJECT": "취소철회",
            "RETURN_REQUEST": "반품요청", "EXCHANGE_REQUEST": "교환요청", "COLLECTING": "수거중", "COLLECT_DONE": "수거완료",
            "EXCHANGE_REDELIVERING": "교환재배송중", "EXCHANGE_DONE": "교환완료", "RETURN_DONE": "반품완료",
            "RETURN_REJECT": "반품철회", "EXCHANGE_REJECT": "교환철회", "PURCHASE_DECISION_HOLDBACK": "구매확정보류",
            "PURCHASE_DECISION_REQUEST": "구매확정요청", "PURCHASE_DECISION_HOLDBACK_RELEASE": "구매확정보류해제",
            "ADMIN_CANCELING": "직권취소중", "ADMIN_CANCEL_DONE": "직권취소완료", "ADMIN_CANCEL_REJECT": "직권취소철회"
        }

    @property
    def place_order_status(self) -> dict[str,str]:
        return {"NOT_YET": "발주 미확인", "OK": "발주 확인", "CANCEL": "발주 확인 해제"}


class ProductOrder(Order):
    ...


class OrderStatus(SmartstoreAPI):
    method = "GET"
    version = "v1"
    path = "/pay-order/seller/product-orders/last-changed-statuses"
    datetime_format = "%Y-%m-%dT%H:%M:%S.%f%z"

    @property
    def default_options(self) -> dict:
        return dict(CursorAll = dict(delay=1))

    @SmartstoreAPI.with_session
    @SmartstoreAPI.with_token
    def extract(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            last_changed_type: str | None = None,
            retry_count: int = 5,
            **kwargs
        ) -> JsonObject:
        return (self.request_each_cursor(self.request_json_until_success)
                .partial(last_changed_type=last_changed_type, retry_count=retry_count)
                .expand(date=self.generate_date_range(start_date, end_date, freq='D'))
                .all_cursor(self.get_next_cursor, next_cursor=dict())
                .run())

    def get_next_cursor(self, response: JsonObject, date: dt.date, **context) -> dict[str,str]:
        from linkmerce.utils.map import hier_get
        more = hier_get(response, ["data","more"]) or dict()
        if more.get("moreFrom") and ((more.get("moreFrom") or str()) <= f"{date}T23:59:59.999+09:00"):
            return more

    def build_request_params(
            self,
            date: dt.date,
            last_changed_type: str | None = None,
            next_cursor: dict[str,str] = dict(),
            limit_count: int = 300,
            **kwargs
        ) -> dict:
        return {
            "lastChangedFrom": next_cursor.get("moreFrom") or f"{date}T00:00:00.000+09:00",
            "lastChangedTo": f"{date}T23:59:59.999+09:00",
            **({"lastChangedType": last_changed_type} if last_changed_type is not None else {}),
            **({"moreSequence": next_cursor["moreSequence"]} if "moreSequence" in next_cursor else {}),
            "limitCount": limit_count,
        }

    @property
    def last_changed_type(self) -> dict[str,str]:
        return {
            "PAY_WAITING": "결제대기", "PAYED": "결제완료", "EXCHANGE_OPTION": "옵션변경", "DELIVERY_ADDRESS_CHANGED": "배송지변경",
            "GIFT_RECEIVED": "선물수락", "CLAIM_REJECTED": "클레임철회", "DISPATCHED": "발송처리", "CLAIM_REQUESTED": "클레임요청",
            "COLLECT_DONE": "수거완료", "CLAIM_COMPLETED": "클레임완료", "PURCHASE_DECIDED": "구매확정",
            "HOPE_DELIVERY_INFO_CHANGED": "배송희망일변경", "CLAIM_REDELIVERING": "교환재배송처리"
        }
