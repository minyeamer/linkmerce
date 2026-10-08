from __future__ import annotations
from linkmerce.core.coupang.api import CoupangApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    import datetime as dt


class Inventory(CoupangApi):
    """쿠팡 로켓창고의 재고 요약 목록을 수집하는 클래스.

    - **Menu**: Rocket Growth > 로켓창고 재고 API
    - **API**: https://api-gateway.coupang.com/v2/providers/rg_open_api/apis/api/v1/vendors/{vendor_id}/rg/inventory/summaries
    - **Docs**: https://developers.coupang.com/ko/api/rocket-growth/rg-inventory-api

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
        커서 요청 간 대기 시간(초). 기본값은 `1.3` (분당 50회 호출 제한)
    """

    method = "GET"
    path = "/v2/providers/rg_open_api/apis/api/v1/vendors/{vendor_id}/rg/inventory/summaries"
    default_options = {"CursorAll": {"request_delay": 1.3}}

    @CoupangApi.with_session
    def extract(self, vendor_item_id: int | str | None = None, **kwargs) -> list[dict]:
        """로켓창고의 모든 상품 또는 특정 SKU에 대한 재고 목록을 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        vendor_item_id: int | str | None
            노출옵션ID. 생략하면 전체 옵션 조회

        Returns
        -------
        list[dict]
            로켓창고 재고 요약 목록
        """
        return (self.cursor_all(self.request_json, self.get_next_cursor)
            .run(vendor_item_id=vendor_item_id))

    def get_next_cursor(self, response: dict, **context) -> str | None:
        """다음 페이지를 가리키는 `nextToken` 커서를 추출한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(response, "nextToken") or None

    def build_request_params(
            self,
            vendor_item_id: int | str | None = None,
            next_cursor: str | None = None,
            **kwargs
        ) -> dict | None:
        params = dict()
        if vendor_item_id is not None:
            params["vendorItemId"] = str(vendor_item_id)
        if next_cursor is not None:
            params["nextToken"] = next_cursor
        return params or None


class Order(CoupangApi):
    """쿠팡 로켓그로스 주문 목록을 수집하는 클래스.

    - **Menu**: Rocket Growth > 로켓그로스 주문 API(목록 쿼리)
    - **API**: https://api-gateway.coupang.com/v2/providers/rg_open_api/apis/api/v1/vendors/{vendor_id}/rg/orders
    - **Docs**: https://developers.coupang.com/ko/api/rocket-growth/rg-order-apilist-query

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
        커서 요청 간 대기 시간(초). 기본값은 `1.3` (분당 50회 호출 제한)
    """

    method = "GET"
    path = "/v2/providers/rg_open_api/apis/api/v1/vendors/{vendor_id}/rg/orders"
    date_format = "%Y%m%d"
    days_limit = 30
    default_options = {"CursorAll": {"request_delay": 1.3}}

    @CoupangApi.with_session
    def extract(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":next_start_date:"] = ":next_start_date:",
            **kwargs
        ) -> list[dict]:
        """로켓그로스 주문 목록을 결제일 기준으로 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        start_date: dt.date | str
            검색 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
        end_date: dt.date | str
            검색 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":next_start_date:"`: `start_date`의 1일 후 날짜 (기본값)

        Returns
        -------
        list[dict]
            기간 내 로켓그로스 주문 목록
        """
        if end_date == ":next_start_date:":
            from linkmerce.utils.date import strpdate
            import datetime as dt
            end_date = strpdate(start_date) + dt.timedelta(days=1)

        return (self.cursor_all(self.request_json, self.get_next_cursor)
            .run(start_date=start_date, end_date=end_date))

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
            "paidDateFrom": str(start_date).replace('-', ''),
            "paidDateTo": str(end_date).replace('-', ''),
            **({"nextToken": next_cursor} if next_cursor is not None else {})
        }
