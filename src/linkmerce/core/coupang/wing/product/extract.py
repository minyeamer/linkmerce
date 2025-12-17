from __future__ import annotations
from linkmerce.core.coupang.wing import CoupangWing

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class ProductOption(CoupangWing):
    method = "POST"
    path = "/tenants/seller-web/v2/vendor-inventory/search"
    max_page_size = 500
    page_start = 1
    token_required = False

    @property
    def default_options(self) -> dict:
        return dict(PaginateAll = dict(request_delay=1))

    @CoupangWing.with_session
    def extract(self, is_deleted: bool = False, **kwargs) -> JsonObject:
        return (self.paginate_all(self.request_json_safe, self.count_total, self.max_page_size, self.page_start)
                .run(is_deleted=is_deleted))

    def count_total(self, response: JsonObject, **kwargs) -> int:
        from linkmerce.utils.map import hier_get
        return hier_get(response, ["data","pagination","totalCount"])

    def build_request_json(self, is_deleted: bool = False, page: int = 1, page_size: int = 500, **kwargs) -> dict:
        return {
            "searchKeywordType": "ALL",
            "searchKeywords": "",
            "salesMethod": "ALL",
            "productStatus": ["ALL"],
            "stockSearchType": "ALL",
            "shippingFeeSearchType": "ALL",
            "displayCategoryCodes": [],
            "listingStartTime": None,
            "listingEndTime": None,
            "saleEndDateSearchType": "ALL",
            "bundledShippingSearchType": "ALL",
            "displayDeletedProduct": is_deleted,
            "shippingMethod": "ALL",
            "exposureStatus": "ALL",
            "sortMethod": "SORT_BY_ITEM_LEVEL_UNIT_SOLD",
            "countPerPage": page_size,
            "page": page,
            "locale": "ko_KR",
            "coupangAttributeOptimized": False,
            "upBundleSearchOption": "ALL",
            "exposureStatuses": [],
            "qualityEnhanceTypes": []
        }

    def set_request_headers(self, cookies: str, **kwargs) -> str:
        return super().set_request_headers(
            authority = self.origin,
            contents = "json",
            cookies = cookies,
            origin = self.origin,
            referer = (self.origin + "/vendor-inventory/list"),
            Vdc = "ko_KR",
        )


class ProductDetail(CoupangWing):
    method = "GET"
    path = "/tenants/seller-web/v2/vendor-inventory/vendor-inventory-items-with-vendorItems/{}"
    max_page_size = 500
    page_start = 1
    token_required = False

    @property
    def default_options(self) -> dict:
        return dict(RequestEach = dict(request_delay=0.3))

    @CoupangWing.with_session
    def extract(self, vendor_inventory_id: Sequence[int | str], **kwargs) -> JsonObject:
        return (self.request_each(self.request_json_safe)
                .partial(referer=kwargs.get("referer")) # for Transformer
                .expand(vendor_inventory_id=vendor_inventory_id)
                .run())

    def build_request_message(self, vendor_inventory_id: int | str, **kwargs) -> dict:
        kwargs["url"] = self.url.format(vendor_inventory_id)
        return super().build_request_message(**kwargs)

    def build_request_params(self, **kwargs) -> dict[str,str]:
        return {"hasProgressiveDiscountRule": "true", "queryNonVariationJustificationProof": "true"}

    def set_request_headers(self, cookies: str, **kwargs) -> str:
        return super().set_request_headers(
            authority = self.origin,
            cookies = cookies,
            referer = (self.origin + "/vendor-inventory/list"),
        )


class ProductDownload(ProductOption):
    method = "POST"
    token_required = False

    @CoupangWing.with_session
    def extract(
            self,
            request_type = "VENDOR_INVENTORY_ITEM",
            fields: list[str] = list(),
            is_deleted: bool = False,
            vendor_id: str | None = None,
            wait_seconds: int = 60,
            wait_interval: int = 1,
            **kwargs
        ) -> dict[str,bytes]:
        report = self.request_report(request_type, fields, is_deleted)
        report_id = report["responseParam"]
        self.wait_report(report_id, request_type, wait_seconds, wait_interval)
        file_name = f"{self.description[request_type]}_{self.today}.xlsx"
        return {file_name: self.download_excel(report_id, is_deleted, vendor_id)}

    def request_report(
            self,
            request_type = "VENDOR_INVENTORY_ITEM",
            fields: list[str] = list(),
            is_deleted: bool = False,
            **kwargs
        ) -> dict:
        url = self.origin + "/tenants/seller-web/excel/request/download/create/vendor-inventory/all"
        body = self.build_request_json(request_type, fields, is_deleted)
        with self.request("POST", url, json=body, headers=self.build_request_headers()) as response:
            return response.json()

    def wait_report(
            self,
            report_id: str,
            request_type = "VENDOR_INVENTORY_ITEM",
            wait_seconds: int = 60,
            wait_interval: int = 1,
        ) -> bool:
        import time
        for _ in range(0, max(wait_seconds, 1), max(wait_interval, 1)):
            time.sleep(wait_interval)
            for report in self.list_report(request_type)["result"]:
                if isinstance(report, dict) and (str(report["sellerRequestDownloadExcelId"]) == report_id):
                    if report["status"] == "COMPLETED":
                        return True
        raise ValueError(f"Failed to create the {self.description[request_type]} report.")

    def list_report(self, request_type = "VENDOR_INVENTORY_ITEM", page: int = 1, page_size: int = 10) -> list[dict]:
        url = self.origin + "/tenants/seller-web/excel/request/download/list"
        params = {"requestType": request_type, "page": page, "countPerPage": page_size}
        with self.request("GET", url, params=params, headers=self.build_request_headers()) as response:
            return response.json()

    def download_excel(
            self,
            report_id: str,
            request_type = "VENDOR_INVENTORY_ITEM",
            is_deleted: bool = False,
            vendor_id: str | None = None
        ) -> bytes:
        url = self.origin + f"/tenants/seller-web/excel/request/download/file"
        params = {"requestType": request_type, "sellerRequestDownloadExcelId": report_id}
        with self.request("GET", url, params=params, headers=self.build_request_headers()) as response:
            return self.parse(response.content, request_type=request_type, is_deleted=is_deleted, vendor_id=vendor_id)

    def build_request_json(
            self,
            request_type = "VENDOR_INVENTORY_ITEM",
            fields: list[str] = list(),
            is_deleted: bool = False,
            **kwargs
        ):
        super().build_request_json(is_deleted)
        return {
            "requestType": request_type,
            "selectedTypes": fields,
            "comment": f"{self.request_type[request_type].replace('/', '_')} 변경({self.today})",
            "fileDescription": f"{self.description[request_type]}_{self.today}",
            "productSearchV2Condition": dict(
                super().build_request_json(is_deleted),
                totalCount = 0
            )
        }

    @property
    def today(self) -> dt.date:
        import datetime as dt
        return dt.datetime.today().strftime("%y%m%d")

    @property
    def request_type(self) -> dict[str,str]:
        return {
            "VENDOR_INVENTORY_ITEM": "가격/재고/판매상태",
            "EDITABLE_CATALOGUE": "쿠팡상품정보",
            "BULK_DELETE_INVENTORY": "상품삭제"
        }

    @property
    def description(self) -> dict[str,str]:
        return {
            "VENDOR_INVENTORY_ITEM": "price_inventory",
            "EDITABLE_CATALOGUE": "Coupang_detailinfo",
            "BULK_DELETE_INVENTORY": "delete_inventory"
        }

    @property
    def fields(self) -> dict[str,str]:
        return {
            "DISPLAY_PRODUCT_NAME": "노출상품명",
            "MANUFACTURE" : "제조사",
            "BRAND": "브랜드",
            "SEARCH_TAG": "검색어",
            "ADULT_ONLY": "성인상품여부",
            "EXPOSE_ATTRIBUTE": "구매옵션",
            "NON_EXPOSE_ATTRIBUTE": "검색옵션",
            "MODEL_NO": "모델번호",
            "BARCODE": "바코드"
        }


class RocketInventory(CoupangWing):
    method = "POST"
    path = "/tenants/rfm-inventory/inventory-health-dashboard/search"
    token_required = True

    @property
    def default_options(self) -> dict:
        return dict(CursorAll = dict(request_delay=1))

    @CoupangWing.with_session
    def extract(
            self,
            hidden_status: Literal["VISIBLE","HIDDEN"] | None = None, 
            vendor_id: str | None = None,
            **kwargs
        ) -> JsonObject:
        return (self.cursor_all(self.request_json_safe, self.get_next_cursor)
                .run(hidden_status=hidden_status, vendor_id=vendor_id, referer=kwargs.get("referer")))

    def get_next_cursor(self, response: JsonObject, **context) -> dict:
        from linkmerce.utils.map import hier_get
        pagination = hier_get(response, ["paginationResponse"]) or dict()
        if pagination.get("searchAfterSortValues"):
            return {
                "pageNumber": pagination["pageNumber"]+1,
                "searchAfterSortValues": pagination["searchAfterSortValues"]
            }
        else:
            return None

    # def count_total(self, response: JsonObject, **kwargs) -> int:
    #     from linkmerce.utils.map import hier_get
    #     return hier_get(response, ["paginationResponse","totalNumberOfElements"])

    def build_request_json(
            self,
            hidden_status: Literal["VISIBLE","HIDDEN"] | None = None,
            page: int = 0,
            page_size: int = 100,
            next_cursor: dict | None = None,
            **kwargs
        ) -> dict:
        return {
            "paginationRequest": {
                "pageSize": page_size,
                **(next_cursor if isinstance(next_cursor, dict) else {
                    "pageNumber": page,
                    "searchAfterSortValues": None
                })
            },
            **({"hiddenStatus": hidden_status} if hidden_status in ("VISIBLE","HIDDEN") else {}),
            "sort": [{
                "sortParameter": "VENDOR_INVENTORY_ID",
                "sortDirection": "ASCENDING"
            }]
        }

    def set_request_headers(self, cookies: str, **kwargs) -> str:
        return super().set_request_headers(
            authority = self.origin,
            contents = "json",
            cookies = cookies,
            origin = self.origin,
            referer = (self.origin + "/tenants/rfm-inventory/management/list"),
        )
