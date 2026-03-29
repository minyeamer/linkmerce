from __future__ import annotations
from linkmerce.core.smartstore.api import SmartstoreApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class Product(SmartstoreApi):
    """네이버 커머스 API로 상품 목록 조회 결과를 수집하는 클래스.

    `PaginateAll` Task를 사용하여 검색 조건에 대한 전체 상품 목록을 조회한다."""

    method = "POST"
    version = "v1"
    path = "/products/search"
    date_format = "%Y-%m-%d"

    @property
    def default_options(self) -> dict:
        return {"PaginateAll": {"request_delay": 1}}

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
        ) -> JsonObject:
        """상품 목록 조회 결과를 수집해 JSON 형식으로 반환한다."""
        return (self.paginate_all(self.request_json_until_success, counter=self.count_total, max_page_size=500, page_start=1)
                .run(search_keyword=search_keyword, keyword_type=keyword_type, status_type=status_type, period_type=period_type,
                    from_date=from_date, to_date=to_date, channel_seq=channel_seq, max_retries=max_retries))

    def count_total(self, response: JsonObject, **kwargs) -> int:
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
        return {
            **({"searchKeywordType": keyword_type, self.keyword_type[keyword_type]: list(map(int, search_keyword))} if search_keyword else dict()),
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
        """상품 판매 상태 코드별 한글 명칭 매핑을 반환한다."""
        return {
            "WAIT": "판매 대기", "SALE": "판매 중", "OUTOFSTOCK": "품절", "UNADMISSION": "승인 대기", "REJECTION": "승인 거부",
            "SUSPENSION": "판매 중지", "CLOSE": "판매 종료", "PROHIBITION": "판매 금지"}

    @property
    def order_type(self) -> dict[str, str]:
        """상품 정렬 기준별 한글 명칭 매핑을 반환한다."""
        return {
            "NO": "상품번호순", "REG_DATE": "등록일순", "MOD_DATE": "수정일순", "NAME": "상품명순", "SELLER_CODE": "판매자 상품코드순",
            "LOW_PRICE": "판매가 낮은 순", "HIGH_PRICE": "판매가 높은 순", "POPULARITY": "인기도순", "ACCUMULATE_SALE": "누적 판매 건수순",
            "LOW_DISCOUNT_PRICE": "할인가 낮은 순", "SALE_START": "판매 시작일순", "SALE_END": "판매 종료일순"}

    @property
    def period_type(self) -> dict[str, str]:
        """상품 기간 검색 유형별 한글 명칭 매핑을 반환한다."""
        return {
            "PROD_REG_DAY": "상품 등록일", "SALE_START_DAY": "판매 시작일",
            "SALE_END_DAY": "판매 종료일", "PROD_MOD_DAY": "최종 수정일",
        }


class Option(SmartstoreApi):
    """네이버 커머스 API로 채널 상품 조회 결과를 수집하는 클래스.

    `RequestEach` Task를 사용하여 상품코드(`product_id`) 목록에 대해 순차 조회한다."""

    method = "GET"
    version = "v2"
    path = "/products/channel-products/{}"

    @property
    def default_options(self) -> dict:
        return {"RequestEach": {"request_delay": 1}}

    @SmartstoreApi.with_session
    @SmartstoreApi.with_token
    def extract(
            self,
            product_id: Sequence[int | str],
            channel_seq: int | str | None = None,
            max_retries: int = 5,
            **kwargs
        ) -> JsonObject:
        """상품코드(`product_id`) 목록에 대해 채널 상품 조회 결과를 수집해 JSON 형식으로 반환한다."""
        return (self.request_each(self.request_json_until_success)
                .partial(channel_seq=channel_seq, max_retries=max_retries)
                .expand(product_id=product_id)
                .run())

    def build_request_message(self, product_id: int | str, **kwargs) -> dict:
        kwargs["url"] = self.url.format(product_id)
        return super().build_request_message(**kwargs)
