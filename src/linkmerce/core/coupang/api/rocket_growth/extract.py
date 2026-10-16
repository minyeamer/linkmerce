from __future__ import annotations
from linkmerce.core.coupang.api import CoupangApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
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
    def extract(self, vendor_item_id: int | str | None = None, max_retries: int = 5, **kwargs) -> list[dict]:
        """로켓창고의 모든 상품 또는 특정 SKU에 대한 재고 목록을 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        vendor_item_id: int | str | None
            노출옵션ID. 생략하면 전체 옵션 조회
        max_retries: int
            동시 요청 제한이 발생할 경우 최대 재시도 횟수. 기본값은 `5`

        Returns
        -------
        list[dict]
            로켓창고 재고 요약 목록
        """
        return (self.cursor_all(self.request_json_until_success, self.get_next_cursor)
            .run(vendor_item_id=vendor_item_id, max_retries=max_retries))

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
    extend_timeout = True
    default_options = {"CursorAll": {"request_delay": 1.3}}

    @CoupangApi.with_session
    def extract(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":next_start_date:"] = ":next_start_date:",
            max_retries: int = 5,
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
        max_retries: int
            동시 요청 제한이 발생할 경우 최대 재시도 횟수. 기본값은 `5`

        Returns
        -------
        list[dict]
            기간 내 로켓그로스 주문 목록
        """
        if end_date == ":next_start_date:":
            from linkmerce.utils.date import strpdate
            import datetime as dt
            end_date = strpdate(start_date) + dt.timedelta(days=1)

        return (self.cursor_all(self.request_json_until_success, self.get_next_cursor)
            .run(start_date=start_date, end_date=end_date, max_retries=max_retries))

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


class OrderDetail(CoupangApi):
    """쿠팡 주문번호별 로켓그로스 주문을 수집하는 클래스.

    - **Menu**: Rocket Growth > 로켓그로스 주문API
    - **API**: https://api-gateway.coupang.com/v2/providers/rg_open_api/apis/api/v1/vendors/{vendor_id}/rg/order/{order_id}
    - **Docs**: https://developers.coupang.com/ko/api/rocket-growth/api

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
        주문별 요청 간 대기 시간(초). 기본값은 `1.3`
    tqdm_options: dict | None
        반복 요청 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    path = "/v2/providers/rg_open_api/apis/api/v1/vendors/{vendor_id}/rg/order/{order_id}"
    extend_timeout = True
    default_options = {"RequestEach": {"request_delay": 1.3}}

    @CoupangApi.with_session
    def extract(self, order_id: int | str | Iterable[int | str], max_retries: int = 5, **kwargs) -> dict | list[dict]:
        """주문번호별 로켓그로스 주문을 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        order_id: int | str | Iterable[int | str]
            주문번호. 단일 값 또는 배열
        max_retries: int
            동시 요청 제한이 발생할 경우 최대 재시도 횟수. 기본값은 `5`

        Returns
        -------
        dict | list[dict]
            주문 단건 또는 목록. `order_id` 타입에 따라 반환 타입이 다르다.
                - `order_id`가 `int | str` 타입일 때 -> `dict`
                - `order_id`가 `Iterable[int | str]` 타입일 때 -> `list[dict]`
        """
        order_id = order_id if isinstance(order_id, (int, str)) else list(order_id)
        return (self.request_each(self.request_json_until_success)
                .partial(max_retries=max_retries)
                .expand(order_id=order_id)
                .run())

    def build_request_message(self, order_id: int | str, **kwargs) -> dict:
        """주문번호를 로켓그로스 주문 단건 조회 경로에 붙인다."""
        path = self.path.format(vendor_id=self.vendor_id, order_id=str(order_id))
        kwargs["url"] = self.concat_path(self.origin, path)
        return super().build_request_message(**kwargs)
