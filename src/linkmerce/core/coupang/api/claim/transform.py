from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence


class ReceiptParser(JsonTransformer):
    """쿠팡 마켓플레이스 반품 및 취소 요청 목록 조회 결과를 파싱하는 클래스."""

    dtype = dict
    scope = "data"
    fields = [
        "receiptId", "orderId", "receiptType", "receiptStatus", "createdAt", "modifiedAt",
        "cancelReasonCategory1", "cancelReasonCategory2", "cancelReason", "returnDeliveryId",
        "returnDeliveryType", "faultByType", "completeConfirmDate", "completeConfirmType",
    ]


class ReceiptItemParser(JsonTransformer):
    """쿠팡 마켓플레이스 반품 및 취소 요청의 상품 목록을 파싱하는 클래스."""

    dtype = dict
    scope = "data"
    fields = [
        "receiptId", "orderId", "receiptType", "receiptStatus", "createdAt", "modifiedAt",
        "returnDeliveryId", "completeConfirmDate",
        {"item": [
            "vendorItemId", # "vendorItemPackageName",
            "purchaseCount", "cancelCount", "shipmentBoxId", "sellerProductId",
        ]},
    ]

    def parse(self, obj: list[dict], **kwargs) -> list[dict]:
        return [(receipt | {"item": item}) for receipt in obj for item in receipt["returnItems"]]


class ReturnRequest(DuckDBTransformer):
    """쿠팡 마켓플레이스 반품 및 취소 요청 목록 조회 결과를 변환 및 적재하는 클래스.

    - **Extractor**: `ReturnRequest`

    - **Parsers** ( *parser_class: input_type -> output_type* ):
        1. `ReceiptParser: dict -> list[dict]`
        2. `ReceiptItemParser: dict -> list[dict]`

    - **Tables** ( *table_key: table_name (description)* ):
        1. `receipt: coupang_return_request` (반품 및 취소 요청 정보)
        2. `item: coupang_return_request_item` (반품 및 취소 요청 상품 정보)

    Parameters
    ----------
    vendor_id: str
        업체 코드
    """

    extractor = "ReturnRequest"
    tables = {"receipt": "coupang_return_request", "item": "coupang_return_request_item"}
    parser = "json"
    parser = {"receipt": ReceiptParser, "receipt_item": ReceiptItemParser}
    params = {"vendor_id": "$vendor_id"}


class ReturnRequestDetail(ReturnRequest):
    """쿠팡 마켓플레이스 반품요청 단건 조회 결과를 변환 및 적재하는 클래스.

    - **Extractor**: `ReturnRequestDetail`

    - **Parsers** ( *parser_class: input_type -> output_type* ):
        1. `ReceiptParser: dict -> list[dict]`
        2. `ReceiptItemParser: dict -> list[dict]`

    - **Tables** ( *table_key: table_name (description)* ):
        1. `receipt: coupang_return_request` (반품 및 취소 요청 정보)
        2. `item: coupang_return_request_item` (반품 및 취소 요청 상품 정보)

    Parameters
    ----------
    vendor_id: str
        업체 코드
    """

    extractor = "ReturnRequestDetail"
    tables = {"receipt": "coupang_return_request", "item": "coupang_return_request_item"}
    parser = {"receipt": ReceiptParser, "receipt_item": ReceiptItemParser}

    def set_queries(self, name: Literal["self"] | str = "self", keys: Sequence[str] | None = None):
        """`models.sql` 파일에서 쿼리를 불러온다. `name = "self"` -> `ReturnRequest` 클래스명을 키로 사용한다."""
        super().set_queries(ReturnRequest.__name__ if name == "self" else name, keys)


class WithdrawItemParser(JsonTransformer):
    """쿠팡 마켓플레이스 반품철회 이력의 상품 목록을 파싱하는 클래스."""

    dtype = dict
    scope = "data"
    fields = ["cancelId", "orderId", "vendorId", "refundDeliveryDuty", "createdAt", "vendorItemId"]

    def parse(self, obj: list[dict], **kwargs) -> list[dict]:
        return [(cancel | {"vendorItemId": id}) for cancel in obj for id in cancel["vendorItemIds"]]


class ReturnWithdraw(DuckDBTransformer):
    """쿠팡 마켓플레이스 반품철회 이력 조회 결과를 변환 및 적재하는 클래스.

    - **Extractor**: `ReturnWithdraw`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `WithdrawItemParser: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: coupang_return_withdraw`
    """

    extractor = "ReturnWithdraw"
    tables = {"table": "coupang_return_withdraw"}
    parser = WithdrawItemParser


class ExchangeParser(JsonTransformer):
    """쿠팡 마켓플레이스 교환요청 목록 조회 결과를 파싱하는 클래스."""

    dtype = dict
    scope = "data"
    fields = [
        "exchangeId", "orderId", "vendorId", "exchangeStatus", "referType", "faultType",
        "reasonCode", "reasonCodeText", "reasonEtcDetail", "cancelReason",
        "createdAt", "modifiedAt", "deliveryStatus", "collectStatus",
    ]


class ExchangeItemParser(JsonTransformer):
    """쿠팡 마켓플레이스 교환요청의 상품 목록을 파싱하는 클래스."""

    dtype = dict
    scope = "data"
    fields = [
        "exchangeId", "orderId", "vendorId", "exchangeStatus",
        "createdAt", "modifiedAt",
        {"item": [
            "orderItemId", "orderItemUnitPrice", "targetItemId", "targetItemUnitPrice",
            "quantity", "originalShipmentBoxId",
        ]},
    ]

    def parse(self, obj: list[dict], **kwargs) -> list[dict]:
        return [(receipt | {"item": item}) for receipt in obj for item in receipt["exchangeItemDtoV1s"]]


class ExchangeRequest(DuckDBTransformer):
    """쿠팡 마켓플레이스 교환요청 목록 조회 결과를 변환 및 적재하는 클래스.

    - **Extractor**: `ExchangeRequest`

    - **Parsers** ( *parser_class: input_type -> output_type* ):
        1. `ExchangeParser: dict -> list[dict]`
        2. `ExchangeItemParser: dict -> list[dict]`

    - **Tables** ( *table_key: table_name (description)* ):
        1. `exchange: coupang_exchange_request` (교환 요청 정보)
        2. `item: coupang_exchange_request_item` (교환 요청 상품 정보)
    """

    extractor = "ExchangeRequest"
    tables = {"exchange": "coupang_exchange_request", "item": "coupang_exchange_request_item"}
    parser = {"exchange": ExchangeParser, "exchange_item": ExchangeItemParser}
