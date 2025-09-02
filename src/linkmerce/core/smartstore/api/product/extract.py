from __future__ import annotations
from linkmerce.core.smartstore.api import SmartstoreAPI

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class Product(SmartstoreAPI):
    method = "POST"
    version = "v1"
    path = "/products/search"
    date_format = "%Y-%m-%d"

    @property
    def default_options(self) -> dict:
        return dict(PaginateAll = dict(delay=1))

    @SmartstoreAPI.with_session
    @SmartstoreAPI.with_token
    def extract(
            self,
            search_keyword: Sequence[int] = list(),
            keyword_type: Literal["CHANNEL_PRODUCT_NO","PRODUCT_NO","GROUP_PRODUCT_NO"] = "CHANNEL_PRODUCT_NO",
            status_type: Sequence[Literal["ALL","WAIT","SALE","OUTOFSTOCK","UNADMISSION","REJECTION","SUSPENSION","CLOSE","PROHIBITION"]] = ["SALE"],
            period_type: Literal["PROD_REG_DAY","SALE_START_DAY","SALE_END_DAY","PROD_MOD_DAY"] = "PROD_REG_DAY",
            from_date: dt.date | str | None = None,
            to_date: dt.date | str | None = None,
            channel_seq: int | str | None = None,
            retry_count: int = 5,
            **kwargs
        ) -> JsonObject:
        return (self.paginate_all(self.request_json_until_success, counter=self.count_total, max_page_size=500, page_start=1)
                .run(search_keyword=search_keyword, keyword_type=keyword_type, status_type=status_type, period_type=period_type,
                    from_date=from_date, to_date=to_date, channel_seq=channel_seq, retry_count=retry_count))

    def count_total(self, response: JsonObject, **kwargs) -> int:
        return response.get("totalElements") if isinstance(response, dict) else None

    def build_request_json(
            self,
            search_keyword: Sequence[int] = list(),
            keyword_type: Literal["CHANNEL_PRODUCT_NO","PRODUCT_NO","GROUP_PRODUCT_NO"] = "CHANNEL_PRODUCT_NO",
            status_type: Sequence[Literal["ALL","WAIT","SALE","OUTOFSTOCK","UNADMISSION","REJECTION","SUSPENSION","CLOSE","PROHIBITION"]] = ["SALE"],
            page: int = 1,
            page_size: int = 500,
            period_type: Literal["PROD_REG_DAY","SALE_START_DAY","SALE_END_DAY","PROD_MOD_DAY"] = "PROD_REG_DAY",
            from_date: dt.date | str | None = None,
            to_date: dt.date | str | None = None,
            **kwargs
        ) -> dict:
        return {
            **({"searchKeywordType": keyword_type, self.keyword_type[keyword_type]: list(map(int, search_keyword))} if search_keyword else {}),
            "productStatusTypes": (list(self.status_type.keys()) if "ALL" in status_type else status_type),
            "page": int(page),
            "size": int(page_size),
            "orderType": "REG_DATE",
            **({
                "periodType": period_type,
                **({"fromDate": str(from_date)} if from_date else {}),
                **({"toDate": str(to_date)} if to_date else {}),
            } if from_date or to_date else {}),
        }

    @property
    def keyword_type(self) -> dict[str,str]:
        return {"CHANNEL_PRODUCT_NO":"channelProductNos", "PRODUCT_NO":"originProductNos", "GROUP_PRODUCT_NO":"groupProductNos"}

    @property
    def status_type(self) -> dict[str,str]:
        return {
            "WAIT": "판매 대기", "SALE": "판매 중", "OUTOFSTOCK": "품절", "UNADMISSION": "승인 대기", "REJECTION": "승인 거부",
            "SUSPENSION": "판매 중지", "CLOSE": "판매 종료", "PROHIBITION": "판매 금지"}

    @property
    def order_type(self) -> dict[str,str]:
        return {
            "NO": "상품번호순", "REG_DATE": "등록일순", "MOD_DATE": "수정일순", "NAME": "상품명순", "SELLER_CODE": "판매자 상품코드순",
            "LOW_PRICE": "판매가 낮은 순", "HIGH_PRICE": "판매가 높은 순", "POPULARITY": "인기도순", "ACCUMULATE_SALE": "누적 판매 건수순",
            "LOW_DISCOUNT_PRICE": "할인가 낮은 순", "SALE_START": "판매 시작일순", "SALE_END": "판매 종료일순"}

    @property
    def period_type(self) -> dict[str,str]:
        return {"PROD_REG_DAY": "상품 등록일", "SALE_START_DAY": "판매 시작일", "SALE_END_DAY": "판매 종료일", "PROD_MOD_DAY": "최종 수정일"}
