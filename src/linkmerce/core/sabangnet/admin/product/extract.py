from __future__ import annotations
from linkmerce.core.sabangnet.admin import SabangnetAdmin

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class Product(SabangnetAdmin):
    """사방넷상품조회수정 메뉴의 상품 목록을 페이지 단위로 조회하는 클래스.

    - **Menu**: 상품관리 > 사방넷상품조회수정
    - **API**: https://sbadmin{domain}.sabangnet.co.kr/prod-api/customer/product/getProductInquirySearchList
    - **Referer**: https://sbadmin{domain}.sabangnet.co.kr/#/product/product-inquiry

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    userid: str
        사방넷 아이디
    passwd: str
        사방넷 비밀번호
    domain: int
        사방넷 시스템 도메인 번호

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `1`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    path = "/prod-api/customer/product/getProductInquirySearchList"
    max_page_size = 500
    page_start = 1
    date_format = "%Y%m%d"
    default_options = {"PaginateAll": {"request_delay": 1}}

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
        """사방넷상품조회수정 화면에서 검색 조건에 대한 상품 목록을 페이지 단위로 조회한다.

        Parameters
        ----------
        start_date: dt.date | str | Literal[":base_date:", ":today:"]
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식 문자열을 전달한다.
                - `":base_date:"`: 사방넷 설립일, "1986-01-09" (기본값)
                - `":today:"`: 오늘 날짜
        end_date: dt.date | str | Literal[":start_date:", ":today:"]
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식 문자열을 전달한다.
                - `":start_date:"`: `start_date`와 동일한 날짜
                - `":today:"`: 오늘 날짜 (기본값)
        date_type: str
            일자 유형. `date_type` 속성의 키 중 하나를 전달할 수 있다. 기본값은 상품등록일
        sort_type: str
            정렬순서 코드. `sort_type` 속성의 키 중 하나를 전달할 수 있다. 기본값은 등록일
        sort_asc: bool
            정렬순서 방식. `True`면 오름차순, `False`면 내림차순으로 조회한다. 기본값은 오름차순
        is_deleted: bool
            삭제된 상품만 조회할지 여부. `True`면 상품상태 조건 대신 삭제 상태만 조회한다.
            기본값은 `False`
        product_status: str | None
            상품상태 코드. `product_status` 속성의 키 중 하나를 전달할 수 있다.

        Returns
        -------
        list[dict]
            사방넷 상품 목록
        """
        from linkmerce.core.sabangnet.admin import get_product_date_pair
        start_date, end_date = get_product_date_pair(start_date, end_date)
        return (self.paginate_all(
                    self.request_json_safe,
                    counter = self.count_total,
                    max_page_size = self.max_page_size,
                    page_start = self.page_start,
                ).run(
                    start_date = start_date,
                    end_date = end_date,
                    date_type = date_type,
                    sort_type = sort_type,
                    sort_asc = sort_asc,
                    is_deleted = is_deleted,
                    product_status = product_status,
                ))

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
        """일자 유형 코드와 한글명 매핑을 반환한다."""
        return {
            "001": "상품등록일", "002": "상품수정일", "003": "상품삭제일", "004": "상품상태변경일",
        }

    @property
    def sort_type(self) -> dict[str, str]:
        """정렬순서 코드와 한글명 매핑을 반환한다."""
        return {
            "001": "등록일", "002": "품번코드", "003": "자체상품코드", "004": "모델명", "005": "모델NO",
            "006": "상품명", "007": "판매가", "008": "수정일", "009": "브랜드명", "010": "원가"
        }

    @property
    def product_status(self) -> dict[str, str]:
        """상품상태 코드와 한글명 매핑을 반환한다."""
        return {
            "001": "대기중", "002": "공급중", "003": "일시중지", "004": "완전품절", "005": "미사용",
            "006": "삭제", "007": "자료없음", "008": "비노출"
        }

    @property
    def search_condition(self) -> dict[str, str]:
        """검색항목 코드와 한글명 매핑을 반환한다."""
        return {
            "PRD_NO": "품번코드", "PRD_NM": "상품명", "ENG_PRD_NM": "영문상품명", "PRD_ABBR_RMRK": "상품약어",
            "MODL_NM": "모델명", "MODL_NO_NM": "모델NO", "BRND_NM": "브랜드명", "ONSF_PRD_CD": "자체상품코드",
            "MKCP_NM": "제조사", "SEPR": "판매가", "FST_REGS_USER_NM": "등록자", "MNGR_MEMO_TEXT": "관리자메모",
            "ADD_PRD_GRP_ID": "추가상품그룹코드", "VRTL_STOC_QT": "총가상재고합", "ORGPL_NTN_DIV_CD": "원산지",
            "ADD_PRD_GRP_ID_G": "연결상품코드(G코드)"
        }


class Option(SabangnetAdmin):
    """사방넷상품조회수정 메뉴의 옵션관리 팝업에서 옵션 목록을 조회하는 클래스.

    - **Menu**: 상품관리 > 사방넷상품조회수정 > 상품수정 > 옵션관리
    - **API**: https://sbadmin{domain}.sabangnet.co.kr/prod-api/customer/product/getOptionInfoList
    - **Referer**: https://sbadmin{domain}.sabangnet.co.kr/#/popup/views/pages/product/component/product-common/product-option-manage.vue

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    userid: str
        사방넷 아이디
    passwd: str
        사방넷 비밀번호
    domain: int
        사방넷 시스템 도메인 번호

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `1`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    path = "/prod-api/customer/product/getOptionInfoList"
    default_options = {"RequestEach": {"request_delay": 0.3}}

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(self, product_id: Sequence[str], **kwargs) -> JsonObject:
        """사방넷상품조회수정 화면의 옵션관리 팝업에서 상품별 옵션 목록을 조회한다.

        Parameters
        ----------
        product_id: Sequence[str]
            조회할 품번코드 목록. 각 품번코드에 대해 옵션 목록을 각각 조회한다.

        Returns
        -------
        list[dict]
            사방넷 옵션 목록
        """
        return (self.request_each(self.request_json_safe)
                .expand(product_id=product_id)
                .run())

    def build_request_json(self, product_id: str, **kwargs) -> dict:
        return {"prdNo": product_id, "skuNo": None, "optDivCd": "basic"}

    @property
    def option_type(self) -> dict[str, str]:
        """공급상태 코드와 한글명 매핑을 반환한다."""
        return {"002": "판매", "004": "품절", "005": "미사용"}


class OptionDownload(SabangnetAdmin):
    """사방넷단품대량수정 메뉴의 옵션 목록을 엑셀로 다운로드하는 클래스.

    - **Menu**: 상품관리 > 사방넷단품대량수정 > 수정파일
    - **API**: https://sbadmin{domain}.sabangnet.co.kr/prod-api/customer/product/getSkuBulkModifyExcel
    - **Referer**: https://sbadmin{domain}.sabangnet.co.kr/#/product/product-update-sku

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    userid: str
        사방넷 아이디
    passwd: str
        사방넷 비밀번호
    domain: int
        사방넷 시스템 도메인 번호
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
        """사방넷단품대량수정 화면의 검색 결과를 엑셀 파일로 다운로드한다.

        Parameters
        ----------
        start_date: dt.date | str | Literal[":base_date:", ":today:"]
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식 문자열을 전달한다.
                - `":base_date:"`: 사방넷 설립일, "1986-01-09" (기본값)
                - `":today:"`: 오늘 날짜
        end_date: dt.date | str | Literal[":start_date:", ":today:"]
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식 문자열을 전달한다.
                - `":start_date:"`: `start_date`와 동일한 날짜
                - `":today:"`: 오늘 날짜 (기본값)
        date_type: str
            일자 유형. `date_type` 속성의 키 중 하나를 전달할 수 있다. 기본값은 상품등록일
        sort_type: str
            정렬순서 코드. `sort_type` 속성의 키 중 하나를 전달할 수 있다. 기본값은 품번코드
        sort_asc: bool
            정렬순서 방식. `True`면 오름차순, `False`면 내림차순으로 조회한다. 기본값은 오름차순
        is_deleted: bool
            삭제된 상품만 조회할지 여부. `True`면 상품상태 조건 대신 삭제 상태만 조회한다.
            기본값은 `False`
        product_status: list[str]
            상품상태 코드. `product_status` 속성의 키 중 하나를 전달할 수 있다.

        Returns
        -------
        dict[str, bytes]
            `{파일명: 엑셀 바이너리}` 형식의 다운로드 결과
        """
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
        """정렬순서 코드와 한글명 매핑을 반환한다."""
        return {
            "prdNo": "품번코드", "skuNo": "사방넷상품코드", "onsfPrdCd": "자체상품코드", "modlNm": "모델명",
            "prdNm": "상품명", "fstRegsDt": "등록일", "fnlChgDt": "수정일"
        }

    @property
    def product_status(self) -> dict[str, str]:
        """상품상태 코드와 한글명 매핑을 반환한다."""
        return {
            "001": "대기중", "002": "공급중", "003": "일시중지", "004": "완전품절", "005": "미사용",
            "006": "삭제", "007": "자료없음", "008": "비노출"
        }


class AddProductGroup(SabangnetAdmin):
    """사방넷추가상품관리 메뉴의 추가상품 그룹 목록을 페이지 단위로 조회하는 클래스.

    - **Menu**: 상품관리 > 사방넷추가상품관리
    - **API**: https://sbadmin{domain}.sabangnet.co.kr/prod-api/customer/product/getAddProductList
    - **Referer**: https://sbadmin{domain}.sabangnet.co.kr/#/product/product-add

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    userid: str
        사방넷 아이디
    passwd: str
        사방넷 비밀번호
    domain: int
        사방넷 시스템 도메인 번호

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `1`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    path = "/prod-api/customer/product/getAddProductList"
    max_page_size = 500
    page_start = 1
    date_format = "%Y%m%d"
    default_options = {"PaginateAll": {"request_delay": 1}}

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(
            self,
            start_date: dt.date | str | Literal[":base_date:", ":today:"] = ":base_date:",
            end_date: dt.date | str | Literal[":start_date:", ":today:"] = ":today:",
            shop_id: str = str(),
            **kwargs
        ) -> JsonObject:
        """사방넷추가상품관리 화면의 검색 조건에 대한 추가상품 그룹 목록을 페이지 단위로 조회한다.

        Parameters
        ----------
        start_date: dt.date | str | Literal[":base_date:", ":today:"]
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식 문자열을 전달한다.
                - `":base_date:"`: 사방넷 설립일, "1986-01-09" (기본값)
                - `":today:"`: 오늘 날짜
        end_date: dt.date | str | Literal[":start_date:", ":today:"]
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식 문자열을 전달한다.
                - `":start_date:"`: `start_date`와 동일한 날짜
                - `":today:"`: 오늘 날짜 (기본값)
        shop_id: str
            쇼핑몰ID 검색 조건을 선택적으로 전달할 수 있다.

        Returns
        -------
        list[dict]
            사방넷 추가상품 그룹 목록
        """
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
    """사방넷추가상품관리 메뉴의 추가상품그룹관리 팝업에서 추가상품 목록을 조회하는 클래스.

    - **Menu**: 상품관리 > 사방넷추가상품관리 > 추가상품그룹관리
    - **API**: https://sbadmin{domain}.sabangnet.co.kr/prod-api/customer/product/getAddProductListInGroup
    - **Referer**: https://sbadmin{domain}.sabangnet.co.kr/#/popup/views/pages/product/component/product-additional-prd-grp-mng

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    userid: str
        사방넷 아이디
    passwd: str
        사방넷 비밀번호
    domain: int
        사방넷 시스템 도메인 번호

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `0.3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    path = "/prod-api/customer/product/getAddProductListInGroup"
    default_options = {"RequestEach": {"request_delay": 0.3}}

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(self, group_id: Sequence[str], **kwargs) -> JsonObject:
        """사방넷추가상품관리 화면의 추가상품그룹관리 팝업에서 선택한 그룹별 추가상품 목록을 조회한다.

        Parameters
        ----------
        group_id: Sequence[str]
            조회할 추가상품 그룹코드 목록

        Returns
        -------
        list[dict]
            사방넷 추가상품 목록
        """
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
