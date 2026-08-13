from __future__ import annotations

from linkmerce.core.naver.brandconnect import BrandConnect

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import datetime as dt
    from typing import Iterable, Literal


class SalesPerformances(BrandConnect):
    """네이버 쇼핑 커넥트 상품별 판매 실적을 조회하는 클래스.

    - **Menu**: 쇼핑 커넥트 > 판매 실적 > 상품별
    - **API**: https://gw-brandconnect.naver.com/affiliate/query/sales-performances/partner-products
    - **Referer**: https://brandconnect.naver.com/{space_id}/affiliate/sales

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        스페이스별 요청 간 대기 시간(초). 기본값은 `1`
    tqdm_options: dict | None
        반복 요청 작업 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    path = "/affiliate/query/sales-performances/partner-products"
    date_format = "%Y-%m-%d"
    days_limit = 90
    default_options = {"RequestEach": {"request_delay": 1}}

    @BrandConnect.with_session
    def extract(
            self,
            space_id: int | str | Iterable[int | str],
            start_date: dt.date | str,
            end_date: dt.date | str = ":start_date:",
        ) -> dict | list[dict]:
        """쇼핑 커넥트 스페이스의 일별 상품 판매 실적을 JSON 형식으로 조회한다.

        Parameters
        ----------
        space_id: int | str | Iterable[int | str]
            브랜드 커넥트 스페이스 ID. 정수 또는 문자열, 또는 정수/문자열의 배열을 입력한다.
        start_date: dt.date | str
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
        end_date: dt.date | str
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)

        Returns
        -------
        dict | list[dict]
            상품별 판매 실적
                - `space_id`가 `int | str` 타입이면 `dict`를 반환한다.
                - `space_id`가 `Iterable[int | str]` 타입이면 `list[dict]`를 반환한다.
        """
        context = self.generate_date_context(start_date, end_date, freq='D', format=self.date_format)
        return (self.request_each(self.request_json_safe, context=context)
                .expand(space_id=space_id)
                .run())

    def build_request_params(
            self,
            start_date: str,
            end_date: str,
            sort_type: Literal["SALES_COUNT", "SALES_AMOUNT", "COMMISSION_AMOUNT"] = "SALES_COUNT",
            page: int = 1,
            page_base: int = 1,
            **kwargs,
        ) -> dict:
        return {
            "startDate": start_date,
            "endDate": end_date,
            "sortType": sort_type,
            "page": page,
            "pageBase": page_base,
        }

    def build_request_headers(self, space_id: int | str, **kwargs) -> dict[str, str]:
        return self.get_request_headers() | {
            "referer": self.concat_path(self.origin, str(space_id), "/affiliate/sales"),
            "x-space-id": str(space_id),
        }
