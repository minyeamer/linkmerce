from __future__ import annotations

from linkmerce.core.ebay.esmplus import EsmPlus

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    import datetime as dt


class Order(EsmPlus):
    """ESM PLUS 주문내역을 조회하는 클래스.

    - **Menu**: 판매 관리 > 주문통합검색 > 전체
    - **API**: https://post-tx.esmplus.com/api/order-integration/orders
    - **Referer**: https://www.esmplus.com/Home/v2/order-integration

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        페이지 요청 간 대기 시간(초). 기본값은 `1`
    tqdm_options: dict | None
        페이지 순회 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    subdomain = "post-tx"
    path = "/order-integration/orders"
    date_format = "%Y-%m-%d"
    max_page_size = 500
    page_start = 1
    default_options = {
        "PaginateAll": {"request_delay": 1},
        "RequestEachPages": {"request_delay": 1},
    }

    @EsmPlus.with_session
    def extract(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            search_key: Literal["ON", "GN", "BN", "BI", "RN", "SON", "SGN"] = "ON",
            search_value: str = str(),
            **kwargs,
        ) -> list[dict]:
        """주문내역을 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        start_date: dt.date | str
            검색 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
        end_date: dt.date | str
            검색 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
        search_key: str
            검색조건
                - `"ON"`: 주문번호
                - `"GN"`: 상품번호
                - `"BN"`: 구매자명
                - `"BI"`: 구매자아이디
                - `"RN"`: 수령인명
                - `"SON"`: SSG 원주문번호
                - `"SGN"`: SSG 상품번호
        search_value: str
            검색어

        Returns
        -------
        list[dict]
            주문내역 조회 결과
        """
        return (self.paginate_all(
                    self.request_json,
                    counter = self.count_total,
                    max_page_size = self.max_page_size,
                    page_start = self.page_start
                ).run(
                    start_date = start_date,
                    end_date = (start_date if end_date == ":start_date:" else end_date),
                    search_key = search_key,
                    search_value = search_value,
                    **kwargs
                ))

    def count_total(self, response: dict, **kwargs) -> int:
        """HTTP 응답에서 전체 행 수를 추출한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(response, "data.count.all")

    def build_request_params(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str,
            search_key: Literal["ON", "GN", "BN", "BI", "RN", "SON", "SGN"] = "ON",
            search_value: str = str(),
            page: int = 1,
            page_size: int = 500,
            **kwargs,
        ) -> dict:
        return {
            "page": page,
            "pageSize": page_size,
            "tabSiteId": 0,
            "searchSDT": str(start_date),
            "searchEDT": str(end_date),
            "searchKey": search_key,
            "searchKeyword": search_value,
        }

    def build_request_headers(self, **kwargs) -> dict[str, str]:
        return self.get_request_headers() | {
            "referer": (self.origin + "/shipping/order-integration"),
        }
