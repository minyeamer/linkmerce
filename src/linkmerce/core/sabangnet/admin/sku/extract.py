from __future__ import annotations
from linkmerce.core.sabangnet.admin import SabangnetAdmin

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class MappingSearch(SabangnetAdmin):
    method = "POST"
    path = "/prod-api/customer/order/SkuCodeMapping/getSkuCodeMappingSearch"
    max_page_size = 500
    page_start = 1
    date_format = "%Y%m%d"

    @property
    def default_options(self) -> dict:
        return dict(PaginateAll = dict(request_delay=1))

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(
            self,
            start_date: dt.date | str | Literal[":base_date:",":today:"] = ":base_date:",
            end_date: dt.date | str | Literal[":start_date:",":today:"] = ":today:",
            shop_id: str = str(),
            sort_type: str = "001",
            sort_asc: bool = True,
            sku_yn: bool | None = None,
            **kwargs
        ) -> JsonObject:
        from linkmerce.core.sabangnet.admin import get_date_pair
        start_date, end_date = get_date_pair(start_date, end_date)
        return (self.paginate_all(self.request_json_safe, self.count_total, self.max_page_size, self.page_start)
                .run(start_date=start_date, end_date=end_date, shop_id=shop_id,
                    sort_type=sort_type, sort_asc=sort_asc, sku_yn=sku_yn))

    def count_total(self, response: JsonObject, **kwargs) -> int:
        from linkmerce.utils.map import hier_get
        return hier_get(response, ["data","metaData","total"])

    def build_request_json(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str,
            shop_id: str = str(),
            sort_type: str = "001",
            sort_asc: bool = True,
            page: int = 1,
            size: int = 500,
            **kwargs
        ) -> dict:
        return {
            "dayOption": "001",
            "startDate": str(start_date).replace('-',''),
            "endDate": str(end_date).replace('-',''),
            "pageSize": size,
            "shmaId": shop_id,
            "sortOption": sort_type,
            "sort": ("ASC" if sort_asc else "DESC"),
            "searchCondition": "",
            "searchKeyword": "",
            "currentPage": page,
        }

    def build_request_headers(self, **kwargs: str) -> dict[str,str]:
        from linkmerce.utils.headers import add_headers
        host = dict(host=self.origin, referer=self.origin, origin=self.origin)
        return add_headers(self.get_request_headers(), authorization=self.get_authorization(), **host)

    def set_request_headers(self, **kwargs: str):
        super().set_request_headers(contents="json", **kwargs)

    @property
    def sort_type(self) -> dict[str,str]:
        return {"reg_dm": "연동일자", "mall_id": "쇼핑몰", "product_id": "품번코드", "buinfo_id": "부가정보코드"}


class SkuQuery(dict):
    def __init__(self, product_id: str, product_id_shop: str, shop_id: str, **kwargs):
        super().__init__(
            product_id = product_id,
            product_id_shop = product_id_shop,
            shop_id = shop_id,
        )


class MappingList(SabangnetAdmin):
    method = "POST"
    path = "/prod-api/customer/order/SkuCodeMapping/getMpngHisSkuCodeMappingLists"

    @property
    def default_options(self) -> dict:
        return dict(RequestEach = dict(request_delay=0.3))

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(self, query: Sequence[SkuQuery], **kwargs) -> JsonObject:
        return (self.request_each(self.request_json_safe)
                .expand(query=query)
                .run())

    def build_request_json(self, query: SkuQuery, **kwargs) -> dict:
        return {
            "dayOption": "001",
            "startDate": None,
            "endDate": None,
            "pageSize": 25,
            "sortOption": "001",
            "sort": "DESC",
            "searchCondition": None,
            "searchKeyword": None,
            "currentPage": 1,
            "selectExcelList": None,
            "shmaPrdNo": query["product_id_shop"],
            "shmaId": query["shop_id"],
            "prdNo": query["product_id"],
            "excelDownYn": "N",
            "popType": "sku-code-mapping-history",
        }

    def build_request_headers(self, **kwargs: str) -> dict[str,str]:
        from linkmerce.utils.headers import add_headers
        host = dict(host=self.origin, referer=self.origin, origin=self.origin)
        return add_headers(self.get_request_headers(), authorization=self.get_authorization(), **host)

    def set_request_headers(self, **kwargs: str):
        super().set_request_headers(contents="json", **kwargs)

    @property
    def option_type(self) -> dict[str,str]:
        return {"002": "판매", "004": "품절", "005": "미사용"}
