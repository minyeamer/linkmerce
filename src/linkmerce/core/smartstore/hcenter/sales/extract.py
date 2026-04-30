from __future__ import annotations
from linkmerce.core.smartstore.hcenter import PartnerCenter

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    import datetime as dt


class _Sales(PartnerCenter):
    """네이버 스토어의 매출 데이터를 조회하는 공통 클래스.

    **NOTE** 26-02-27 이후 '브랜드 애널리틱스 > 스토어 트래픽' 메뉴 삭제로 이용 불가   
    [공지사항](https://adcenter.shopping.naver.com/board/notice_detail.nhn?noticeSeq=311782) 참고 (~ v0.6.8)

    - **Menu**: 브랜드 애널리틱스 > 스토어 트래픽 > 스토어 매출
    - **API**: https://hcenter.shopping.naver.com/brand/content
    - **Referer**: https://center.shopping.naver.com/brand-analytics/store

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestLoop` Task 옵션을 전달할 수 있다.

    max_retries: int | None
        최대 반복 실행 횟수. `None`이면 조건을 만족할 때까지 무한 반복한다. 기본값은 `5`
    request_delay: Literal["incremental"] | float | int | tuple[int, int]
        재시도 간 대기 시간. `"incremental"`이면 대기 시간이 1초씩 점진적으로 증가한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachLoop` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `1`
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    path = "/brand/content"
    date_format = "%Y-%m-%d"
    sales_type: Literal["store", "category", "product"]
    fields: list[dict]
    default_options = {
        "RequestLoop": {"max_retries": 5, "ignored_errors": Exception},
        "RequestEachLoop": {"request_delay": 1, "max_concurrent": 3},
    }

    @PartnerCenter.with_session
    def extract(
            self,
            mall_seq: int | str | Iterable[int | str],
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            date_type: Literal["daily", "weekly", "monthly"] = "daily",
            page: int | Iterable[int] = 1,
            page_size: int = 1000,
            **kwargs
        ) -> dict | list[dict]:
        """네이버 스토어의 기간별 매출 데이터를 동기 방식으로 순차 조회해 JSON 형식으로 반환한다.

        **NOTE** 2년 전 데이터까지 제공된다.

        Parameters
        ----------
        mall_seq: int | str | Iterable[int | str]
            쇼핑몰 순번. 정수 또는 문자열, 또는 정수/문자열의 배열을 입력한다.
        start_date: dt.date | str
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
        end_date: dt.date | str
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
        date_type: str
            기간
                - `"daily"`: 일간 (기본값)
                - `"weekly"`: 주간
                - `"monthly"`: 월간
        page: int | Iterable[int]
            페이지 번호. 정수 또는 정수의 배열을 입력한다.
        page_size: int
            한 번에 표시할 목록 수

        Returns
        -------
        dict | list[dict]
            네이버 스토어의 기간별 매출 데이터.
            아래 조건을 모두 만족하면 `dict` 타입을, 그렇지 않으면 `list[dict]` 타입을 반환한다.
                1. `mall_seq`가 `int | str` 타입으로 입력된 경우
                2. `start_date`와 `end_date`이 동일한 경우
                3. `page`가 `int` 타입으로 입력된 경우
        """
        context = self.generate_date_context(start_date, end_date, freq=date_type[0].upper(), format=self.date_format)
        return (self.request_each_loop(self.request_json_safe, context=context)
                .partial(date_type=date_type, page_size=page_size)
                .expand(mall_seq=mall_seq, page=page)
                .loop(self.is_valid_response)
                .run())

    @PartnerCenter.async_with_session
    async def extract_async(
            self,
            mall_seq: int | str | Iterable[int | str],
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            date_type: Literal["daily", "weekly", "monthly"] = "daily",
            page: int = 1,
            page_size: int = 1000,
            **kwargs
        ) -> dict | list[dict]:
        """네이버 스토어의 기간별 매출 데이터를 비동기 방식으로 병렬 조회해 JSON 형식으로 반환한다.

        **NOTE** 2년 전 데이터까지 제공된다.

        Parameters
        ----------
        mall_seq: int | str | Iterable[int | str]
            쇼핑몰 순번. 정수 또는 문자열, 또는 정수/문자열의 배열을 입력한다.
        start_date: dt.date | str
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
        end_date: dt.date | str
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
        date_type: str
            기간
                - `"daily"`: 일간 (기본값)
                - `"weekly"`: 주간
                - `"monthly"`: 월간
        page: int | Iterable[int]
            페이지 번호. 정수 또는 정수의 배열을 입력한다.
        page_size: int
            한 번에 표시할 목록 수

        Returns
        -------
        dict | list[dict]
            네이버 스토어의 기간별 매출 데이터.
            아래 조건을 모두 만족하면 `dict` 타입을, 그렇지 않으면 `list[dict]` 타입을 반환한다.
                1. `mall_seq`가 `int | str` 타입으로 입력된 경우
                2. `start_date`와 `end_date`이 동일한 경우
                3. `page`가 `int` 타입으로 입력된 경우
        """
        context = self.generate_date_context(start_date, end_date, freq=date_type[0].upper(), format=self.date_format)
        return await (self.request_each_loop(self.request_async_json_safe, context=context)
                .partial(date_type=date_type, page_size=page_size)
                .expand(mall_seq=mall_seq, page=page)
                .loop(self.is_valid_response)
                .run_async())

    def is_valid_response(self, response: dict) -> bool:
        """JSON 파싱한 응답 본문에 `error` 필드가 있으면 `UnauthorizedError` 또는 `RequestError`를 발생시킨다."""
        if isinstance(response, dict):
            if "error" in response:
                from linkmerce.utils.nested import hier_get
                msg = hier_get(response, "error.error") or "null"
                if msg == "Unauthorized":
                    from linkmerce.common.exceptions import UnauthorizedError
                    raise UnauthorizedError("Unauthorized request")
                else:
                    from linkmerce.common.exceptions import RequestError
                    raise RequestError(f"An error occurred during the request: {msg}")
            return True
        return False

    def build_request_json(
            self,
            mall_seq: int | str,
            start_date: dt.date,
            end_date: dt.date,
            date_type: Literal["daily", "weekly", "monthly"] = "daily",
            page: int = 1,
            page_size: int = 1000,
            **kwargs
        ) -> dict:
        return dict(self.get_request_body(),
            variables={
                "queryRequest": {
                    "mallSequence": str(mall_seq),
                    "dateType": date_type.capitalize(),
                    "startDate": str(start_date),
                    "endDate": str(end_date),
                    **({"sortBy": "PaymentAmount"} if self.sales_type != "store" else dict()),
                    **({"pageable": {"page":int(page), "size":int(page_size)}} if self.sales_type != "store" else dict()),
                }
            })

    def set_request_body(self):
        from linkmerce.utils.graphql import GraphQLOperation, GraphQLSelection
        super().set_request_body(
            GraphQLOperation(
                operation = f"get{self.sales_type.capitalize()}Sale",
                variables = {"queryRequest": dict()},
                types = {"queryRequest": "StoreTrafficRequest"},
                selection = GraphQLSelection(
                    name = f"{self.sales_type}Sales",
                    variables = ["queryRequest"],
                    fields = self.fields,
                )
            ).generate_body(query_options = {
                "selection": {"variables": {"linebreak": False}, "fields": {"linebreak": True}},
                "suffix": '\n',
            }))

    def set_request_headers(self, **kwargs):
        contents = {"type": "text", "charset": "UTF-8"}
        referer = self.origin + "/iframe/brand-analytics/store/productSales"
        super().set_request_headers(contents=contents, origin=self.origin, referer=referer, **kwargs)


class StoreSales(_Sales):
    """네이버 스토어의 매출 데이터를 조회하는 공통 클래스.

    **NOTE** 26-02-27 이후 '브랜드 애널리틱스 > 스토어 트래픽' 메뉴 삭제로 이용 불가   
    [공지사항](https://adcenter.shopping.naver.com/board/notice_detail.nhn?noticeSeq=311782) 참고 (~ v0.6.8)

    - **Menu**: 브랜드 애널리틱스 > 스토어 트래픽 > 스토어 매출
    - **API**: https://hcenter.shopping.naver.com/brand/content
    - **Referer**: https://center.shopping.naver.com/brand-analytics/store

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestLoop` Task 옵션을 전달할 수 있다.

    max_retries: int | None
        최대 반복 실행 횟수. `None`이면 조건을 만족할 때까지 무한 반복한다. 기본값은 `5`
    request_delay: Literal["incremental"] | float | int | tuple[int, int]
        재시도 간 대기 시간. `"incremental"`이면 대기 시간이 1초씩 점진적으로 증가한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachLoop` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `1`
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    sales_type = "store"

    @property
    def fields(self) -> list[dict]:
        """스토어 매출 GraphQL 응답 필드 목록을 반환한다."""
        return [
            {"period": ["date"]},
            {"sales": [
                "paymentAmount", "paymentCount", "paymentUserCount", "refundAmount",
                "paymentAmountPerPaying", "paymentAmountPerUser", "refundRate"]}
        ]


class CategorySales(_Sales):
    """네이버 스토어의 카테고리별 매출 데이터를 조회하는 공통 클래스.

    **NOTE** 26-02-27 이후 '브랜드 애널리틱스 > 스토어 트래픽' 메뉴 삭제로 이용 불가   
    [공지사항](https://adcenter.shopping.naver.com/board/notice_detail.nhn?noticeSeq=311782) 참고 (~ v0.6.8)

    - **Menu**: 브랜드 애널리틱스 > 스토어 트래픽 > 스토어 상품별 매출 > 카테고리별
    - **API**: https://hcenter.shopping.naver.com/brand/content
    - **Referer**: https://center.shopping.naver.com/brand-analytics/store

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestLoop` Task 옵션을 전달할 수 있다.

    max_retries: int | None
        최대 반복 실행 횟수. `None`이면 조건을 만족할 때까지 무한 반복한다. 기본값은 `5`
    request_delay: Literal["incremental"] | float | int | tuple[int, int]
        재시도 간 대기 시간. `"incremental"`이면 대기 시간이 1초씩 점진적으로 증가한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachLoop` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `1`
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    sales_type = "category"

    @property
    def fields(self) -> list[dict]:
        """카테고리별 매출 GraphQL 응답 필드 목록을 반환한다."""
        return [
            {"product": [{"category": ["identifier", "fullName"]}]},
            {"sales": ["paymentAmount", "paymentCount", "purchaseConversionRate", "paymentAmountPerPaying"]},
            {"visit": ["click"]},
            {"measuredThrough": ["type"]},
        ]


class ProductSales(_Sales):
    """네이버 스토어의 상품별 매출 데이터를 조회하는 공통 클래스.

    **NOTE** 26-02-27 이후 '브랜드 애널리틱스 > 스토어 트래픽' 메뉴 삭제로 이용 불가   
    [공지사항](https://adcenter.shopping.naver.com/board/notice_detail.nhn?noticeSeq=311782) 참고 (~ v0.6.8)

    - **Menu**: 브랜드 애널리틱스 > 스토어 트래픽 > 스토어 상품별 매출 > 상품별
    - **API**: https://hcenter.shopping.naver.com/brand/content
    - **Referer**: https://center.shopping.naver.com/brand-analytics/store

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestLoop` Task 옵션을 전달할 수 있다.

    max_retries: int | None
        최대 반복 실행 횟수. `None`이면 조건을 만족할 때까지 무한 반복한다. 기본값은 `5`
    request_delay: Literal["incremental"] | float | int | tuple[int, int]
        재시도 간 대기 시간. `"incremental"`이면 대기 시간이 1초씩 점진적으로 증가한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachLoop` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `1`
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    sales_type = "product"

    @property
    def fields(self) -> list[dict]:
        """상품별 매출 GraphQL 응답 필드 목록을 반환한다."""
        return [
            {"product": ["identifier", "name", {"category": ["identifier", "name", "fullName"]}]},
            {"sales": ["paymentAmount", "paymentCount", "purchaseConversionRate"]},
            {"visit": ["click"]},
            {"rest": [{"comparePreWeek": ["isNewlyAdded"]}]},
        ]
