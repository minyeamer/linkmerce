from __future__ import annotations
from linkmerce.core.sabangnet.admin import SabangnetAdmin

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class Product(SabangnetAdmin):
    """사방넷 상품 목록을 페이지네이션으로 조회하는 클래스.

    - **Menu**: 상품관리 > 상품조회
    - **API URL**: `POST` https://sbadmin{domain}.sabangnet.co.kr/prod-api/customer/product/getProductInquirySearchList

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    path = "/prod-api/customer/product/getProductInquirySearchList"
    max_page_size = 500
    page_start = 1
    date_format = "%Y%m%d"

    @property
    def default_options(self) -> dict:
        return {"PaginateAll": {"request_delay": 1}}

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(
            self,
            start_date: dt.date | str | Literal[":base_date:", ":today:"] = ":base_date:",
            end_date: dt.date | str | Literal[":start_date:", ":today:"] = ":today:",
            date_type: str = "001",
            sort_type: str = "001",
            sort_asc: bool = True,
            is_deleted: bool = False,
            product_status: str | None = None,
            **kwargs
        ) -> JsonObject:
        """상품 목록을 페이지네이션으로 조회해 JSON 형식으로 반환한다."""
        from linkmerce.core.sabangnet.admin import get_product_date_pair
        kwargs = dict(
            dict(zip(["start_date", "end_date"], get_product_date_pair(start_date, end_date))),
            date_type=date_type, sort_type=sort_type, sort_asc=sort_asc, is_deleted=is_deleted, product_status=product_status)

        return (self.paginate_all(self.request_json_safe, self.count_total, self.max_page_size, self.page_start)
                .run(**kwargs))

    def count_total(self, response: JsonObject, **kwargs) -> int:
        """HTTP 응답에서 전체 상품 건수를 추출한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(response, "data.metaData.total")

    def build_request_json(
            self,
            start_date: str,
            end_date: str,
            date_type: str = "001",
            sort_type: str = "001",
            sort_asc: bool = True,
            is_deleted: bool = False,
            product_status: str | None = None,
            page: int = 1,
            size: int = 500,
            **kwargs
        ) -> dict:
        return {
            "dayOption": date_type,
            "startDate": start_date,
            "endDate": end_date,
            "pageSize": size,
            "sortOption": sort_type,
            "sort": ("ASC" if sort_asc else "DESC"),
            "searchCondition": None,
            "searchKeyword": None,
            "currentPage": page,
            "noOption": False,
            "mngrMemoTextExist": "",
            "nonExposureYn": "",
            "prdSplyStsCd": ("006" if is_deleted else product_status),
        }

    @property
    def date_type(self) -> dict[str, str]:
        """기간 코드와 한글명 매핑을 반환한다."""
        return {
            "001": "상품등록일", "002": "상품수정일",
            "003": "상품삭제일", "004": "상품상태변경일",
        }

    @property
    def sort_type(self) -> dict[str, str]:
        """정렬 기준 코드와 한글명 매핑을 반환한다."""
        return {
            "001": "등록일", "002": "품번코드", "003": "자체상품코드", "004": "모델명", "005": "모델NO",
            "006": "상품명", "007": "판매가", "008": "수정일", "009": "브랜드명", "010": "원가"
        }

    @property
    def product_status(self) -> dict[str, str]:
        """상품 상태 코드와 한글명 매핑을 반환한다."""
        return {
            "001": "대기중", "002": "공급중", "003": "일시중지", "004": "완전품절", "005": "미사용",
            "006": "삭제", "007": "자료없음", "008": "비노출"
        }

    @property
    def search_condition(self) -> dict[str, str]:
        """상품 검색 조건 코드와 한글명 매핑을 반환한다."""
        return {
            "PRD_NO": "품번코드", "PRD_NM": "상품명", "ENG_PRD_NM": "영문상품명", "PRD_ABBR_RMRK": "상품약어",
            "MODL_NM": "모델명", "MODL_NO_NM": "모델NO", "BRND_NM": "브랜드명", "ONSF_PRD_CD": "자체상품코드",
            "MKCP_NM": "제조사", "SEPR": "판매가", "FST_REGS_USER_NM": "등록자", "MNGR_MEMO_TEXT": "관리자메모",
            "ADD_PRD_GRP_ID": "추가상품그룹코드", "VRTL_STOC_QT": "총가상재고합", "ORGPL_NTN_DIV_CD": "원산지",
            "ADD_PRD_GRP_ID_G": "연결상품코드(G코드)"
        }


class Option(SabangnetAdmin):
    """사방넷 단품 옵션 목록을 조회하는 클래스.

    - **Menu**: 상품관리 > 상품조회 > 단품 옵션
    - **API URL**: `POST` https://sbadmin{domain}.sabangnet.co.kr/prod-api/customer/product/getOptionInfoList

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    path = "/prod-api/customer/product/getOptionInfoList"

    @property
    def default_options(self) -> dict:
        return {"RequestEach": {"request_delay": 0.3}}

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(self, product_id: Sequence[str], **kwargs) -> JsonObject:
        """단품코드(`product_id`)에 대한 단품 옵션 목록을 조회해 JSON 형식으로 반환한다."""
        return (self.request_each(self.request_json_safe)
                .expand(product_id=product_id)
                .run())

    def build_request_json(self, product_id: str, **kwargs) -> dict:
        return {"prdNo": product_id, "skuNo": None, "optDivCd": "basic"}

    @property
    def option_type(self) -> dict[str, str]:
        """옵션 구분 코드와 한글명 매핑을 반환한다."""
        return {"002": "판매", "004": "품절", "005": "미사용"}


class OptionDownload(SabangnetAdmin):
    """사방넷 단품 대량 수정 메뉴의 옵션 목록을 다운로드하는 클래스.

    - **Menu**: 상품관리 > 단품대량수정 > 엑셀 다운로드
    - **API URL**: `POST` https://sbadmin{domain}.sabangnet.co.kr/prod-api/customer/product/getSkuBulkModifyExcel
    """

    method = "POST"
    path = "/prod-api/customer/product/getSkuBulkModifyExcel"

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(
            self,
            start_date: dt.date | str | Literal[":base_date:", ":today:"] = ":base_date:",
            end_date: dt.date | str | Literal[":start_date:", ":today:"] = ":today:",
            date_type: str = "prdFstRegsDt",
            sort_type: str = "prdNo",
            sort_asc: bool = True,
            is_deleted: bool = False,
            product_status: list[str] = list(),
            **kwargs
        ) -> dict[str, bytes]:
        """사방넷 단품 대량 수정 메뉴의 옵션 목록을 다운로드한다."""
        from linkmerce.core.sabangnet.admin import get_product_date_pair
        dates = get_product_date_pair(start_date, end_date)
        headers = self.build_request_headers()
        body = self.build_request_json(*dates, date_type, sort_type, sort_asc, is_deleted, product_status)
        response = self.request(self.method, self.url, headers=headers, json=body)
        file_name = self.get_file_name(response.headers.get("Content-Disposition"))
        return {file_name: self.parse(response.content)}

    def get_file_name(self, content_disposition: str) -> str:
        """`Content-Disposition` 헤더에서 파일명을 추출한다."""
        default = "사방넷단품대량수정_수정파일.xlsx"
        return default
        # if not isinstance(content_disposition, str):
        #     return default
        # from linkmerce.utils.regex import regexp_extract
        # from urllib.parse import unquote
        # return regexp_extract(r"([^']+\.xlsx)", unquote(content_disposition)) or default

    def build_request_json(
            self,
            start_date: str,
            end_date: str,
            date_type: str = "prdFstRegsDt",
            sort_type: str = "prdNo",
            sort_asc: bool = True,
            is_deleted: bool = False,
            product_status: list[str] = list(),
            **kwargs
        ) -> dict:
        return {
            "dayOption": date_type,
            "startDate": start_date,
            "endDate": end_date,
            "pageSize": 25,
            "currentPage": 1,
            "sortOption": sort_type,
            "sortValue": ("ASC" if sort_asc else "DESC"),
            "productStatus": (["006"] if is_deleted else product_status),
            "searchCondition": "",
            "searchKeyword": "",
            "searchKeywordList": list(),
            "downloadScale": "ALL",
            "nonExposureYn": "N",
            "prdNoSkuNoList": list()
        }

    @property
    def date_type(self) -> dict[str, str]:
        """기간 코드와 한글명 매핑을 반환한다."""
        return {
            "prdFstRegsDt": "상품등록일", "prdFnlChgDt": "상품수정일", "skuFnlChgDt": "옵션수정일",
            "prdStsUpdDt": "상품상태변경일", "skuFstRegsDt": "옵션생성일"
        }

    @property
    def sort_type(self) -> dict[str, str]:
        """정렬 기준 코드와 한글명 매핑을 반환한다."""
        return {
            "prdNo": "품번코드", "skuNo": "사방넷상품코드", "onsfPrdCd": "자체상품코드", "modlNm": "모델명",
            "prdNm": "상품명", "fstRegsDt": "등록일", "fnlChgDt": "수정일"
        }

    @property
    def product_status(self) -> dict[str, str]:
        """상품 상태 코드와 한글명 매핑을 반환한다."""
        return {
            "001": "대기중", "002": "공급중", "003": "일시중지", "004": "완전품절", "005": "미사용",
            "006": "삭제", "007": "자료없음", "008": "비노출"
        }


class AddProductGroup(SabangnetAdmin):
    """사방넷 추가상품 관리 메뉴의 추가상품 목록을 페이지네이션으로 조회하는 클래스.

    - **Menu**: 상품관리 > 추가상품관리
    - **API URL**: `POST` https://sbadmin{domain}.sabangnet.co.kr/prod-api/customer/product/getAddProductList

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    path = "/prod-api/customer/product/getAddProductList"
    max_page_size = 500
    page_start = 1
    date_format = "%Y%m%d"

    @property
    def default_options(self) -> dict:
        return {"PaginateAll": {"request_delay": 1}}

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(
            self,
            start_date: dt.date | str | Literal[":base_date:", ":today:"] = ":base_date:",
            end_date: dt.date | str | Literal[":start_date:", ":today:"] = ":today:",
            shop_id: str = str(),
            **kwargs
        ) -> JsonObject:
        """사방넷 추가상품 관리 메뉴의 추가상품 목록을 조회해 JSON 형식으로 반환한다."""
        from linkmerce.core.sabangnet.admin import get_product_date_pair
        start_date, end_date = get_product_date_pair(start_date, end_date)
        return (self.paginate_all(self.request_json_safe, self.count_total, self.max_page_size, self.page_start)
                .run(start_date=start_date, end_date=end_date, shop_id=shop_id))

    def count_total(self, response: JsonObject, **kwargs) -> int:
        """HTTP 응답에서 전체 추가상품 그룹 건수를 추출한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(response, "data.0.total")

    def build_request_json(
            self,
            start_date: str,
            end_date: str,
            shop_id: str = str(),
            page: int = 1,
            size: int = 500,
            **kwargs
        ) -> dict:
        return {
            "dayOption": "FST_REGS_DT",
            "startDate": start_date,
            "endDate": end_date,
            "pageSize": size,
            "shmaId": shop_id,
            "sortOption": "ADD_PRD_GRP_ID",
            "sort": "ASC",
            "searchCondition": "",
            "searchKeyword": "",
            "currentPage": page,
        }


class AddProduct(SabangnetAdmin):
    """사방넷 추가 상품 그룹 내 상품 목록을 조회하는 클래스.

    - **Menu**: 상품관리 > 추가상품관리 > 그룹 상세
    - **API URL**: `POST` https://sbadmin{domain}.sabangnet.co.kr/prod-api/customer/product/getAddProductListInGroup

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    path = "/prod-api/customer/product/getAddProductListInGroup"

    @property
    def default_options(self) -> dict:
        """RequestEach Task 기본 옵션을 반환한다."""
        return {"RequestEach": {"request_delay": 0.3}}

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(self, group_id: Sequence[str], **kwargs) -> JsonObject:
        """그룹코드(`group_id`)에 대한 추가상품 목록을 조회해 JSON 형식으로 반환한다."""
        return (self.request_each(self.request_json_safe)
                .expand(group_id=group_id)
                .run())

    def build_request_json(self, group_id: str, **kwargs) -> dict:
        return {
            "addPrdGrpId": group_id,
            "addPrdGrpNm": None,
            "dayOption": "FST_REGS_DT",
            "startDate": None,
            "endDate": None,
            "pageSize": 25,
            "sortOption": "ADD_PRD_GRP_ID",
            "sort": "ASC",
            "currentPage": 1,
            "excelDownYn": "N",
        }
