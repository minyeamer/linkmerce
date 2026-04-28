from __future__ import annotations
from linkmerce.core.coupang.wing import CoupangWing

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    import datetime as dt


class ProductOption(CoupangWing):
    """쿠팡 Wing 상품 목록을 조회하는 클래스.

    - **Menu**: 상품관리 > 상품조회/수정
    - **API**: https://wing.coupang.com/tenants/seller-web/v2/vendor-inventory/search
    - **Referer**: https://wing.coupang.com/vendor-inventory/list

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `1`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    path = "/tenants/seller-web/v2/vendor-inventory/search"
    max_page_size = 500
    page_start = 1
    token_required = False
    default_options = {"PaginateAll": {"request_delay": 1}}

    @CoupangWing.with_session
    def extract(self, is_deleted: bool = False, **kwargs) -> list[dict]:
        """상품 목록을 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        is_deleted: bool
            삭제된 상품 조회 여부
                - `True`: 삭제된 상품만 조회
                - `False`: 삭제되지 않은 전체 상품 조회 (기본값)

        Returns
        -------
        list[dict]
            전체 또는 삭제된 상품 목록
        """
        return (self.paginate_all(self.request_json_safe, self.count_total, self.max_page_size, self.page_start)
                .run(is_deleted=is_deleted))

    def count_total(self, response: dict, **kwargs) -> int:
        """HTTP 응답에서 전체 상품 수를 추출한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(response, "data.pagination.totalCount")

    def build_request_json(self, is_deleted: bool = False, page: int = 1, page_size: int = 500, **kwargs) -> dict:
        return {
            "searchKeywordType": "ALL",
            "searchKeywords": "",
            "salesMethod": "ALL",
            "productStatus": ["ALL"],
            "stockSearchType": "ALL",
            "shippingFeeSearchType": "ALL",
            "displayCategoryCodes": list(),
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
            "exposureStatuses": list(),
            "qualityEnhanceTypes": list()
        }

    def set_request_headers(self, **kwargs) -> str:
        """상품조회/수정 경로를 `referer` 헤더로 추가한다."""
        return super().set_request_headers(
            authority = self.origin,
            contents = "json",
            origin = self.origin,
            referer = (self.origin + "/vendor-inventory/list"),
            Vdc = "ko_KR",
        )


class ProductDetail(CoupangWing):
    """쿠팡 Wing 상품의 상세 정보를 조회하는 클래스.

    - **Menu**: 상품관리 > 상품조회/수정 > 상품 등록
    - **API**: https://wing.coupang.com/tenants/seller-web/v2/vendor-inventory/vendor-inventory-items-with-vendorItems/{vendor_inventory_id}
    - **Referer**: https://wing.coupang.com/tenants/seller-web/vendor-inventory/modify

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `0.3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    path = "/tenants/seller-web/v2/vendor-inventory/vendor-inventory-items-with-vendorItems/{}"
    max_page_size = 500
    page_start = 1
    token_required = False
    default_options = {"RequestEach": {"request_delay": 0.3}}

    @CoupangWing.with_session
    def extract(
            self,
            vendor_inventory_id: int | str | Sequence[int | str],
            **kwargs
        ) -> dict | list[dict]:
        """상품별 상세 정보를 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        vendor_inventory_id: int | str | Sequence[int | str]
            조회할 상품의 등록상품ID. 정수 또는 정수의 배열을 입력한다.

        Returns
        -------
        dict | list[dict]
            상품별 상세 정보. `vendor_inventory_id` 타입에 따라 반환 타입이 다르다.
                - `vendor_inventory_id`가 `int | str` 타입일 때 -> `dict`
                - `vendor_inventory_id`가 `Iterable[int | str]` 타입일 때 -> `list[dict]`
        """
        return (self.request_each(self.request_json_safe)
                .partial(referer=kwargs.get("referer")) # for Transformer
                .expand(vendor_inventory_id=vendor_inventory_id)
                .run())

    def build_request_message(self, vendor_inventory_id: int | str, **kwargs) -> dict:
        """각 HTTP 요청마다 URL에 등록상품ID를 포맷팅한다."""
        kwargs["url"] = self.url.format(vendor_inventory_id)
        return super().build_request_message(**kwargs)

    def build_request_params(self, **kwargs) -> dict[str, str]:
        return {"hasProgressiveDiscountRule": "true", "queryNonVariationJustificationProof": "true"}

    def set_request_headers(self, **kwargs) -> str:
        return super().set_request_headers(
            authority = self.origin,
            referer = (self.origin + "/vendor-inventory/list"),
        )


class ProductDownload(ProductOption):
    """쿠팡 Wing 상품 목록을 엑셀로 다운로드하는 클래스.

    - **Menu**: 상품관리 > 상품조회/수정 > 엑셀 대량 수정 > 엑셀 파일 다운로드
    - **API**: https://wing.coupang.com/tenants/seller-web/excel/request/download/create/vendor-inventory/all
    - **Referer**: https://wing.coupang.com/vendor-inventory/list

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.
    """

    method = "POST"
    token_required = False

    @CoupangWing.with_session
    def extract(
            self,
            request_type: str = "VENDOR_INVENTORY_ITEM",
            fields: list[str] = list(),
            is_deleted: bool = False,
            vendor_id: str | None = None,
            wait_seconds: int = 60,
            wait_interval: int = 1,
            **kwargs
        ) -> dict[str, bytes]:
        """상품 조회조건에 대한 상품 목록을 다운로드 요청하고 엑셀 파일로 다운로드한다.

        Parameters
        ----------
        request_type: str
            상품 조회조건
                - `"VENDOR_INVENTORY_ITEM"`: 가격/재고/판매상태 (기본값)
                - `"EDITABLE_CATALOGUE"`: 쿠팡상품정보
                - `"BULK_DELETE_INVENTORY"`: 상품삭제
        fields: list[str]
            엑셀 항목 코드 목록. 생략 시 기본 항목으로 조회한다.
        is_deleted: bool
            삭제된 상품만 조회할지 여부. 기본값은 `False`
        vendor_id: str | None
            업체 코드. 조회 시점에는 사용되지 않고 파서 함수에 전달된다.
        wait_seconds: int
            파일 생성 완료를 기다리는 최대 시간(초). 기본값은 `60`   
            시간 내 파일이 생성 완료되지 않으면 `ValueError`를 발생시킨다.
        wait_interval: int
            파일 완료 여부를 확인하는 조회 간격(초). 기본값은 `1`

        Returns
        -------
        dict[str, bytes]
            `{파일명: 엑셀 바이너리}` 구조의 상품 목록 다운로드 결과
            - 파일명은 `{description[request_type]}_{today}.xlsx` 명명 규칙에 따라 생성된다.
        """
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
        """엑셀 파일 생성을 요청한다."""
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
        """엑셀 파일 생성 요청 후 완료 여부를 주기적으로 확인하면서 대기한다."""
        import time
        for _ in range(0, max(wait_seconds, 1), max(wait_interval, 1)):
            time.sleep(wait_interval)
            for report in self.list_report(request_type)["result"]:
                if isinstance(report, dict) and (str(report["sellerRequestDownloadExcelId"]) == report_id):
                    if report["status"] == "COMPLETED":
                        return True
        raise ValueError(f"Failed to create the {self.description[request_type]} report.")

    def list_report(self, request_type = "VENDOR_INVENTORY_ITEM", page: int = 1, page_size: int = 10) -> list[dict]:
        """생성된 엑셀 파일을 조회한다."""
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
        """생성된 엑셀 파일을 다운로드한다."""
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
            "comment": f"{self.comment[request_type].replace('/', '_')} 변경({self.today})",
            "fileDescription": f"{self.description[request_type]}_{self.today}",
            "productSearchV2Condition": dict(
                super().build_request_json(is_deleted),
                totalCount = 0
            )
        }

    @property
    def today(self) -> dt.date:
        """오늘 날짜를 `YYMMDD` 형식의 문자열로 반환한다."""
        import datetime as dt
        return dt.datetime.today().strftime("%y%m%d")

    @property
    def comment(self) -> dict[str, str]:
        """`{상품 조회조건: 한글 명칭}` 매핑을 반환한다."""
        return {
            "VENDOR_INVENTORY_ITEM": "가격/재고/판매상태",
            "EDITABLE_CATALOGUE": "쿠팡상품정보",
            "BULK_DELETE_INVENTORY": "상품삭제"
        }

    @property
    def description(self) -> dict[str, str]:
        """`{상품 조회조건: 파일명 접두사}` 매핑을 반환한다."""
        return {
            "VENDOR_INVENTORY_ITEM": "price_inventory",
            "EDITABLE_CATALOGUE": "Coupang_detailinfo",
            "BULK_DELETE_INVENTORY": "delete_inventory"
        }

    @property
    def fields(self) -> dict[str, str]:
        """선택 가능한 엑셀 항목에 대한 `{코드: 한글 명칭}` 매핑을 반환한다."""
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
    """쿠팡 로켓그로스 재고현황을 조회하는 클래스.

    - **Menu**: 로켓그로스 > 재고현황
    - **API**: https://wing.coupang.com/tenants/rfm-inventory/inventory-health-dashboard/search
    - **Referer**: https://wing.coupang.com/tenants/rfm-inventory/management/list

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 `XSRF-TOKEN` 키값이 포함된 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `CursorAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `1`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    path = "/tenants/rfm-inventory/inventory-health-dashboard/search"
    token_required = True
    default_options = {"CursorAll": {"request_delay": 1}}

    @CoupangWing.with_session
    def extract(
            self,
            hidden_status: Literal["VISIBLE", "HIDDEN"] | None = None, 
            vendor_id: str | None = None,
            **kwargs
        ) -> list[dict]:
        """로켓그로스 재고현황을 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        hidden_status: str
            상품 상태 필터
                - `"VISIBLE"`: 노출 상품
                - `"HIDDEN"`: 숨겨진 상품
                - `None`: 전체 상품 (기본값)
        vendor_id: str | None
            업체 코드. 조회 시점에는 사용되지 않고 파서 함수에 전달된다.

        Returns
        -------
        list[dict]
            로켓그로스 상품 목록
        """
        return (self.cursor_all(self.request_json_safe, self.get_next_cursor)
                .run(hidden_status=hidden_status, vendor_id=vendor_id, referer=kwargs.get("referer")))

    def get_next_cursor(self, response: dict, **context) -> dict:
        """다음 페이지를 가리키는 `searchAfterSortValues` 커서를 추출한다."""
        from linkmerce.utils.nested import hier_get
        pagination = hier_get(response, "paginationResponse") or dict()
        if pagination.get("searchAfterSortValues"):
            return {
                "pageNumber": pagination["pageNumber"]+1,
                "searchAfterSortValues": pagination["searchAfterSortValues"]
            }
        else:
            return None

    # def count_total(self, response: dict, **kwargs) -> int:
    #     """HTTP 응답에서 전체 쿠팡 재고 개수를 추출한다."""
    #     from linkmerce.utils.nested import hier_get
    #     return hier_get(response, "paginationResponse.totalNumberOfElements")

    def build_request_json(
            self,
            hidden_status: Literal["VISIBLE", "HIDDEN"] | None = None,
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
            **({"hiddenStatus": hidden_status} if hidden_status in ("VISIBLE", "HIDDEN") else dict()),
            "sort": [{
                "sortParameter": "VENDOR_INVENTORY_ID",
                "sortDirection": "ASCENDING"
            }]
        }

    def set_request_headers(self, **kwargs) -> str:
        return super().set_request_headers(
            authority = self.origin,
            contents = "json",
            origin = self.origin,
            referer = (self.origin + "/tenants/rfm-inventory/management/list"),
        )
