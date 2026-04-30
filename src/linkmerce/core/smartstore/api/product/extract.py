from __future__ import annotations
from linkmerce.core.smartstore.api import SmartstoreApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    import datetime as dt


class Product(SmartstoreApi):
    """네이버 커머스 API로 상품 목록 조회 결과를 수집하는 클래스.

    - **Menu**: 상품 관리 > 상품 조회/수정 (상품 목록 조회)
    - **API**: https://api.commerce.naver.com/external/v1/products/search
    - **Docs**: https://apicenter.commerce.naver.com/docs/commerce-api/current/search-product
    - **Referer**: https://sell.smartstore.naver.com/#/products/origin-list

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    client_id: str
        커머스 API 애플리케이션 ID
    client_secret: str
        커머스 API 애플리케이션 시크릿

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간(초). 기본값은 `1`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    version = "v1"
    path = "/products/search"
    date_format = "%Y-%m-%d"
    default_options = {"PaginateAll": {"request_delay": 1}}

    @SmartstoreApi.with_session
    @SmartstoreApi.with_token
    def extract(
            self,
            search_keyword: Sequence[int] = list(),
            keyword_type: Literal["CHANNEL_PRODUCT_NO", "PRODUCT_NO", "GROUP_PRODUCT_NO"] = "CHANNEL_PRODUCT_NO",
            status_type: Sequence[Literal["ALL", "WAIT", "SALE", "OUTOFSTOCK", "UNADMISSION", "REJECTION", "SUSPENSION", "CLOSE", "PROHIBITION"]] = ["SALE"],
            period_type: Literal["PROD_REG_DAY", "SALE_START_DAY", "SALE_END_DAY", "PROD_MOD_DAY"] = "PROD_REG_DAY",
            from_date: dt.date | str | None = None,
            to_date: dt.date | str | None = None,
            channel_seq: int | str | None = None,
            max_retries: int = 5,
            **kwargs
        ) -> list[dict]:
        """상품 목록을 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        search_keyword: Sequence[int]
            검색 키워드
                - 채널 상품번호(`CHANNEL_PRODUCT_NO`) 선택 시 채널 상품번호를 입력한다.
                - 원상품번호(`PRODUCT_NO`) 선택 시 채널 원상품번호를 입력한다.
                - 그룹상품번호(`GROUP_PRODUCT_NO`) 선택 시 그룹상품번호를 입력한다.
                - 판매자 관리 코드(`SELLER_CODE`) 선택 시 판매자 관리 코드를 입력한다.
        keyword_type: Literal["CHANNEL_PRODUCT_NO", "PRODUCT_NO", "GROUP_PRODUCT_NO"]
            검색 키워드 타입. 기본값은 채널 상품번호(`CHANNEL_PRODUCT_NO`)
        status_type: Sequence[str]
            상품 판매 상태 목록
                - `"WAIT"`: 판매 대기
                - `"SALE"`: 판매 중
                - `"OUTOFSTOCK"`: 품절
                - `"UNADMISSION"`: 승인 대기
                - `"REJECTION"`: 승인 거부
                - `"SUSPENSION"`: 판매 중지
                - `"CLOSE"`: 판매 종료
                - `"PROHIBITION"`: 판매 금지
        period_type: str
            검색 기간 유형
                - `"PROD_REG_DAY"`: 상품 등록일
                - `"SALE_START_DAY"`: 판매 시작일
                - `"SALE_END_DAY"`: 판매 종료일
                - `"PROD_MOD_DAY"`: 최종 수정일
        from_date: dt.date | str | None
            검색 기간 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
        to_date: dt.date | str | None
            검색 기간 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
        channel_seq: int | str | None
            채널 번호. 조회 시점에는 사용되지 않고 파서 함수에 전달된다.
        max_retries: int
            동시 요청 제한이 발생할 경우 최대 재시도 횟수. 기본값은 `5`

        Returns
        -------
        list[dict]
            상품 목록
        """
        return self.paginate_all(
                    self.request_json_until_success,
                    counter = self.count_total,
                    max_page_size = 500,
                    page_start = 1
                ).run(
                    search_keyword = search_keyword,
                    keyword_type = keyword_type,
                    status_type = status_type,
                    period_type = period_type,
                    from_date = from_date,
                    to_date = to_date,
                    channel_seq = channel_seq,
                    max_retries = max_retries,
                )

    def count_total(self, response: dict, **kwargs) -> int:
        """HTTP 응답에서 전체 상품 수를 추출한다."""
        return response.get("totalElements") if isinstance(response, dict) else None

    def build_request_json(
            self,
            search_keyword: Sequence[int] = list(),
            keyword_type: Literal["CHANNEL_PRODUCT_NO", "PRODUCT_NO", "GROUP_PRODUCT_NO"] = "CHANNEL_PRODUCT_NO",
            status_type: Sequence[Literal["ALL", "WAIT", "SALE", "OUTOFSTOCK", "UNADMISSION", "REJECTION", "SUSPENSION", "CLOSE", "PROHIBITION"]] = ["SALE"],
            page: int = 1,
            page_size: int = 500,
            period_type: Literal["PROD_REG_DAY", "SALE_START_DAY", "SALE_END_DAY", "PROD_MOD_DAY"] = "PROD_REG_DAY",
            from_date: dt.date | str | None = None,
            to_date: dt.date | str | None = None,
            **kwargs
        ) -> dict:
        if search_keyword:
            body = {"searchKeywordType": keyword_type, self.keyword_type[keyword_type]: list(map(int, search_keyword))}
        else:
            body = dict()
        return body | {
            "productStatusTypes": (list(self.status_type.keys()) if "ALL" in status_type else status_type),
            "page": int(page),
            "size": int(page_size),
            "orderType": "REG_DATE",
            **({
                "periodType": period_type,
                **({"fromDate": str(from_date)} if from_date else dict()),
                **({"toDate": str(to_date)} if to_date else dict()),
            } if from_date or to_date else dict()),
        }

    @property
    def keyword_type(self) -> dict[str, str]:
        """상품 검색 키워드 유형별 API 파라미터 매핑을 반환한다."""
        return {
            "CHANNEL_PRODUCT_NO": "channelProductNos",
            "PRODUCT_NO": "originProductNos",
            "GROUP_PRODUCT_NO": "groupProductNos",
        }

    @property
    def status_type(self) -> dict[str, str]:
        """상품 판매 상태 코드와 한글명 매핑을 반환한다."""
        return {
            "WAIT": "판매 대기", "SALE": "판매 중", "OUTOFSTOCK": "품절", "UNADMISSION": "승인 대기", "REJECTION": "승인 거부",
            "SUSPENSION": "판매 중지", "CLOSE": "판매 종료", "PROHIBITION": "판매 금지"}

    @property
    def order_type(self) -> dict[str, str]:
        """상품 정렬 기준 코드와 한글명 매핑을 반환한다."""
        return {
            "NO": "상품번호순", "REG_DATE": "등록일순", "MOD_DATE": "수정일순", "NAME": "상품명순", "SELLER_CODE": "판매자 상품코드순",
            "LOW_PRICE": "판매가 낮은 순", "HIGH_PRICE": "판매가 높은 순", "POPULARITY": "인기도순", "ACCUMULATE_SALE": "누적 판매 건수순",
            "LOW_DISCOUNT_PRICE": "할인가 낮은 순", "SALE_START": "판매 시작일순", "SALE_END": "판매 종료일순"}

    @property
    def period_type(self) -> dict[str, str]:
        """상품 기간 검색 유형 코드와 한글명 매핑을 반환한다."""
        return {
            "PROD_REG_DAY": "상품 등록일", "SALE_START_DAY": "판매 시작일",
            "SALE_END_DAY": "판매 종료일", "PROD_MOD_DAY": "최종 수정일",
        }


class Option(SmartstoreApi):
    """네이버 커머스 API로 채널 상품 조회 결과를 수집하는 클래스.

    - **Menu**: 상품 관리 > 상품 조회/수정 > 상품 수정 (채널 상품 조회)
    - **API**: https://api.commerce.naver.com/external/v2/products/channel-products/:channelProductNo
    - **Docs**: https://apicenter.commerce.naver.com/docs/commerce-api/current/read-channel-product-1-product
    - **Referer**: https://sell.smartstore.naver.com/#/products/edit/:productNo

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    client_id: str
        커머스 API 애플리케이션 ID
    client_secret: str
        커머스 API 애플리케이션 시크릿

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간(초). 기본값은 `1`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    version = "v2"
    path = "/products/channel-products/{}"
    default_options = {"RequestEach": {"request_delay": 1}}

    @SmartstoreApi.with_session
    @SmartstoreApi.with_token
    def extract(
            self,
            product_id: int | str | Sequence[int | str],
            channel_seq: int | str | None = None,
            max_retries: int = 5,
            **kwargs
        ) -> dict | list[dict]:
        """상품별 채널 상품 조회 결과를 JSON 형식으로 반환한다.

        Parameters
        ----------
        product_id: int | str | Sequence[int | str]
            상품코드 목록
        channel_seq: int | str | None
            채널 번호. 조회 시점에는 사용되지 않고 파서 함수에 전달된다.
        max_retries: int
            동시 요청 제한이 발생할 경우 최대 재시도 횟수. 기본값은 `5`

        Returns
        -------
        dict | list[dict]
            채널 상품 조회 결과. `product_id` 타입에 따라 반환 타입이 다르다.
                - `product_id`가 `int | str` 타입일 때 -> `dict`
                - `product_id`가 `Iterable[int | str]` 타입일 때 -> `list[dict]`
        """
        return (self.request_each(self.request_json_until_success)
                .partial(channel_seq=channel_seq, max_retries=max_retries)
                .expand(product_id=product_id)
                .run())

    def build_request_message(self, product_id: int | str, **kwargs) -> dict:
        kwargs["url"] = self.url.format(product_id)
        return super().build_request_message(**kwargs)
