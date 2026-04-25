from __future__ import annotations
from linkmerce.core.sabangnet.admin import SabangnetAdmin

from typing import TypedDict, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class Order(SabangnetAdmin):
    """사방넷 주문서확인처리 메뉴의 주문 내역을 페이지 단위로 조회하는 클래스.

    - **Menu**: 주문관리 > 주문서확인처리
    - **API**: https://sbadmin{domain}.sabangnet.co.kr/prod-api/customer/order/OrderConfirm/searchOrders
    - **Referer**: https://sbadmin{domain}.sabangnet.co.kr/#/order/order-confirm

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
    path = "/prod-api/customer/order/OrderConfirm/searchOrders"
    max_page_size = 500
    page_start = 1
    datetime_format = "%Y%m%d%H%M%S"
    default_options = {"PaginateAll": {"request_delay": 1}}

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(
            self,
            start_date: dt.datetime | dt.date | str | Literal[":today:"] = ":today:",
            end_date: dt.datetime | dt.date | str | Literal[":start_date:", ":now:"] = ":start_date:",
            date_type: str = "reg_dm",
            order_status_div: str = str(),
            order_status: Sequence[str] = list(),
            shop_id: str = str(),
            sort_type: str = "ord_no_asc",
            **kwargs
        ) -> JsonObject:
        """주문서확인처리 화면에서 검색 조건에 대한 주문 내역을 페이지 단위로 조회한다.

        Parameters
        ----------
        start_date: dt.datetime | dt.date | str | Literal[":today:"]
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식 문자열을 전달한다.
                - `":today:"`: 오늘 날짜 (기본값)
        end_date: dt.datetime | dt.date | str | Literal[":start_date:", ":now:"]
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식 문자열을 전달한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
                - `":now:"`: 현재 시각
        date_type: str
            일자 유형. `date_type` 속성의 키 중 하나를 전달한다. 기본값은 수집일
        order_status_div: str
            주문구분 코드. `order_status_div` 속성의 키 중 하나를 전달할 수 있다.
        order_status: Sequence[str]
            주문상태 코드 목록. `order_status` 속성의 키를 하나 이상 전달할 수 있다.
        shop_id: str
            쇼핑몰ID 검색 조건을 선택적으로 전달할 수 있다.
        sort_type: str
            정렬순서 코드. `sort_type` 속성의 키를 하나 이상 전달할 수 있다.   
            `"<정렬기준>_<asc|desc>"` 형식이며, 기본값은 `"ord_no_asc"`

        Returns
        -------
        list[dict]
            사방넷 주문 내역
        """
        from linkmerce.core.sabangnet.admin import get_order_date_pair
        start_date, end_date = get_order_date_pair(start_date, end_date)
        return (self.paginate_all(
                    self.request_json_safe,
                    counter = self.count_total,
                    max_page_size = self.max_page_size,
                    page_start = self.page_start
                ).run(
                    start_date = start_date,
                    end_date = end_date,
                    date_type = date_type,
                    order_status_div = order_status_div,
                    order_status = order_status,
                    shop_id = shop_id,
                    sort_type = sort_type,
                ))

    def count_total(self, response: JsonObject, **kwargs) -> int:
        """HTTP 응답에서 전체 주문 건수를 추출한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(response, "data.totAmtSummary.totCnt")

    def build_request_json(
            self,
            start_date: str,
            end_date: str,
            date_type: str = "reg_dm",
            order_status_div: str = str(),
            order_status: Sequence[str] = list(),
            shop_id: str = str(),
            sort_type: str = "ord_no_asc",
            page: int = 1,
            size: int = 500,
            **kwargs
        ) -> dict:
        return {
            "fnlChgPrgmNm": "order-confirm",
            "chkOrdNo": list(),
            'currentPage': page,
            "dateDiv": date_type,
            "startDate": start_date,
            "endDate": end_date,
            "pageSize": size,
            "ordStsTpDivCd": order_status_div,
            "orderStrd": sort_type.rsplit('_', 1)[0],
            "orderDegreeStrd": sort_type.rsplit('_', 1)[1],
            "orderStatus": order_status,
            "shmaId": shop_id,
            "multiplexId": list(),
            "searchKeywordList": list(),
        }

    @property
    def date_type(self) -> dict[str, str]:
        """일자 유형 코드와 한글명 매핑을 반환한다."""
        return {
            "hope_delv_date": "배송희망일", "reg_dm": "수집일", "ord_dt": "주문일", "cancel_rcv_dt": "취소접수일",
            "cancel_dt": "취소완료일", "rtn_rcv_dt": "반품접수일", "rtn_dt": "반품완료일",
            "delivery_confirm_date": "출고완료일", "chng_rcv_dt": "교환접수일", "chng_dt": "교환완료일",
            "dlvery_rcv_dt": "송장등록일", "inv_send_dm": "송장송신일", "stock_confirm_dm": "입출고완료일"
        }

    @property
    def sort_type(self) -> dict[str, str]:
        """정렬순서 코드와 한글명 매핑을 반환한다."""
        return {
            "fst_regs_dt": "수집일", "shpmt_hope_ymd": "배송희망일", "ord_no": "사방넷주문번호", "shma_id": "쇼핑몰",
            "shma_ord_no": "쇼핑몰주문번호", "clct_prd_nm": "수집상품명", "dcd_prd_nm": "확정상품명", "prd_no": "품번코드",
            "bypc_svc_acnt_id": "매입처", "rmte_zipcd": "우편번호", "ord_sts_cd": "주문상태"
        }

    @property
    def order_status_div(self) -> dict[str, str]:
        """주문구분 코드와 한글명 매핑을 반환한다."""
        return {
            "001": "주문(진행)", "002": "주문(완료)", "003": "교발(진행)", "004": "교발(완료)",
            "005": "회수(진행)", "006": "회수(완료)"
        }

    @property
    def order_status(self) -> dict[str, str]:
        """주문상태 코드와 한글명 매핑을 반환한다."""
        return {
            "001": "신규주문", "002": "주문확인", "003": "출고대기", "004": "출고완료", "006": "배송보류",
            "007": "취소접수", "008": "교환접수", "009": "반품접수", "010": "취소완료", "011": "교환완료",
            "012": "반품완료", "021": "교환발송준비", "022": "교환발송완료", "023": "교환회수준비", "024": "교환회수완료",
            "025": "반품회수준비", "026": "반품회수완료", "999": "폐기"
        }


class OrderDownload(Order):
    """사방넷 주문서확인처리 메뉴의 주문 내역을 엑셀로 다운로드하는 클래스.

    - **Menu**: 주문관리 > 주문서확인처리 > 다운로드
    - **API**: https://sbadmin{domain}.sabangnet.co.kr/prod-api/customer/order/OrderConfirm/partner/downloadOrderConfirmExcelSearch
    - **Referer**: https://sbadmin{domain}.sabangnet.co.kr/#/order/order-confirm

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
    path = "/prod-api/customer/order/OrderConfirm/partner/downloadOrderConfirmExcelSearch"
    datetime_format = "%Y%m%d%H%M%S"
    default_options = dict()

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(
            self,
            download_no: int,
            start_date: dt.datetime | dt.date | str | Literal[":today:"] = ":today:",
            end_date: dt.datetime | dt.date | str | Literal[":start_date:", ":now:"] = ":start_date:",
            date_type: str = "reg_dm",
            order_seq: list[int] = list(),
            order_status_div: str = str(),
            order_status: Sequence[str] = list(),
            shop_id: str = str(),
            sort_type: str = "ord_no_asc",
            **kwargs
        ) -> dict[str, bytes]:
        """주문서확인처리 화면의 검색 결과를 엑셀 파일로 다운로드한다.

        Parameters
        ----------
        download_no: int
            주문서 출력 양식 번호
        start_date: dt.datetime | dt.date | str | Literal[":today:"]
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식 문자열을 전달한다.
                - `":today:"`: 오늘 날짜 (기본값)
        end_date: dt.datetime | dt.date | str | Literal[":start_date:", ":now:"]
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식 문자열을 전달한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
                - `":now:"`: 현재 시각
        date_type: str
            일자 유형. `date_type` 속성의 키 중 하나를 전달한다. 기본값은 수집일
        order_seq: list[int]
            사방넷 주문 번호를 목록으로 선택할 수 있다.
        order_status_div: str
            주문구분 코드. `order_status_div` 속성의 키 중 하나를 전달할 수 있다.
        order_status: Sequence[str]
            주문상태 코드 목록. `order_status` 속성의 키를 하나 이상 전달할 수 있다.
        shop_id: str
            쇼핑몰ID 검색 조건을 선택적으로 전달할 수 있다.
        sort_type: str
            정렬순서 코드. `sort_type` 속성의 키를 하나 이상 전달할 수 있다.   
            `"<정렬기준>_<asc|desc>"` 형식이며, 기본값은 `"ord_no_asc"`

        Returns
        -------
        dict[str, bytes]
            `{파일명: 엑셀 바이너리}` 형식의 다운로드 결과
        """
        from linkmerce.core.sabangnet.admin import get_order_date_pair
        dates = get_order_date_pair(start_date, end_date)
        headers = self.build_request_headers()
        body = self.build_request_json(download_no, *dates, date_type, order_seq, order_status_div, order_status, shop_id, sort_type)
        response = self.request(self.method, self.url, headers=headers, json=body)
        file_name = self.get_file_name(response.headers.get("Content-Disposition"))
        return {file_name: self.parse(response.content)}

    def get_file_name(self, content_disposition: str) -> str:
        """`Content-Disposition` 헤더에서 파일명을 추출한다."""
        default = "주문서확인처리.xlsx"
        if not isinstance(content_disposition, str):
            return default
        from linkmerce.utils.regex import regexp_extract
        from urllib.parse import unquote
        return regexp_extract(r"(\d{8}_.*\.xlsx)", unquote(content_disposition)) or default

    def build_request_json(
            self,
            download_no: int,
            start_date: str,
            end_date: str,
            date_type: str = "reg_dm",
            order_seq: list[int] = list(),
            order_status_div: str = str(),
            order_status: Sequence[str] = list(),
            shop_id: str = str(),
            sort_type: str = "ord_no_asc",
            page: int = 1,
            size: int = 25,
            **kwargs
        ) -> dict:
        body = super().build_request_json(start_date, end_date, date_type, order_status_div, order_status, shop_id, sort_type, page, size)
        return dict(body, **{
            "chkOrdNo": order_seq,
            "downloadScale": ("" if order_seq else "all"),
            "exclFormDivCd": "01",
            "exclFormSrno": str(download_no),
            "excelTotalCount": 1,
            "excelPassword": None,
            "opaExcelDownloadName": "주문서확인처리",
        })


class OrderStatus(OrderDownload):
    """사방넷 주문서확인처리 메뉴의 주문 내역을 일자 유형별로 엑셀 다운로드하는 클래스.

    - **Menu**: 주문관리 > 주문서확인처리 > 다운로드
    - **API**: https://sbadmin{domain}.sabangnet.co.kr/prod-api/customer/order/OrderConfirm/partner/downloadOrderConfirmExcelSearch
    - **Referer**: https://sbadmin{domain}.sabangnet.co.kr/#/order/order-confirm

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

    default_options = {"RequestEach": {"request_delay": 1}}

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(
            self,
            download_no: int,
            start_date: dt.datetime | dt.date | str | Literal[":today:"] = ":today:",
            end_date: dt.datetime | dt.date | str | Literal[":start_date:", ":now:"] = ":start_date:",
            date_type: list[str] = ["delivery_confirm_date", "cancel_dt", "rtn_dt", "chng_dt"],
            order_seq: list[int] = list(),
            order_status_div: str = str(),
            order_status: Sequence[str] = list(),
            shop_id: str = str(),
            sort_type: str = "ord_no_asc",
            **kwargs
        ) -> dict[str, bytes]:
        """주문서확인처리 화면의 검색 결과를 여러 일자 유형별 엑셀 파일로 다운로드한다.

        Parameters
        ----------
        download_no: int
            주문서 출력 양식 번호
        start_date: dt.datetime | dt.date | str | Literal[":today:"]
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식 문자열을 전달한다.
                - `":today:"`: 오늘 날짜 (기본값)
        end_date: dt.datetime | dt.date | str | Literal[":start_date:", ":now:"]
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식 문자열을 전달한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
                - `":now:"`: 현재 시각
        date_type: list[str]
            일자 유형 목록. `date_type` 속성의 키를 하나 이상 전달한다.   
            기본값은 "출고완료일", "취소완료일", "반품완료일", "교환완료일"에 해당하는 코드 목록이다.
        order_seq: list[int]
            사방넷 주문 번호를 목록으로 선택할 수 있다.
        order_status_div: str
            주문구분 코드. `order_status_div` 속성의 키 중 하나를 전달할 수 있다.
        order_status: Sequence[str]
            주문상태 코드 목록. `order_status` 속성의 키를 하나 이상 전달할 수 있다.
        shop_id: str
            쇼핑몰ID 검색 조건을 선택적으로 전달할 수 있다.
        sort_type: str
            정렬순서 코드. `sort_type` 속성의 키를 하나 이상 전달할 수 있다.   
            `"<정렬기준>_<asc|desc>"` 형식이며, 기본값은 `"ord_no_asc"`

        Returns
        -------
        dict[str, bytes]
            `{일자 유형: 엑셀 바이너리}` 형식의 다운로드 결과
        """
        from linkmerce.core.sabangnet.admin import get_order_date_pair
        start_date, end_date = get_order_date_pair(start_date, end_date)
        keys = [self.date_type[dt] for dt in date_type]
        return dict(zip(keys, self.request_each(self.request_content)
                .partial(
                    download_no = download_no,
                    start_date = start_date,
                    end_date = end_date,
                    date_type = date_type,
                    order_seq = order_seq,
                    order_status_div = order_status_div,
                    order_status = order_status,
                    shop_id = shop_id,
                    sort_type = sort_type,
                ).expand(date_type=date_type).run()))


class ProductMapping(SabangnetAdmin):
    """사방넷 품번코드매핑관리 메뉴의 매핑 내역을 페이지 단위로 조회하는 클래스.

    - **Menu**: 주문관리 > 품번코드매핑관리
    - **API**: https://sbadmin{domain}.sabangnet.co.kr/prod-api/customer/order/SkuCodeMapping/getSkuCodeMappingSearch
    - **Referer**: https://sbadmin{domain}.sabangnet.co.kr/#/order/product-code-mapping

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
    # path = "/prod-api/customer/order/ProductCodeMapping/getProductCodeMappingSearch"
    path = "/prod-api/customer/order/SkuCodeMapping/getSkuCodeMappingSearch"
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
        """품번코드매핑관리 화면의 매핑 내역을 페이지 단위로 조회한다.

        Parameters
        ----------
        start_date: dt.date | str | Literal[":base_date:", ":today:"]
            생성일자 조건의 조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식 문자열을 전달한다.
                - `":base_date:"`: 사방넷 설립일, "1986-01-09" (기본값)
                - `":today:"`: 오늘 날짜
        end_date: dt.date | str | Literal[":start_date:", ":today:"]
            생성일자 조건의 조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식 문자열을 전달한다.
                - `":start_date:"`: `start_date`와 동일한 날짜
                - `":today:"`: 오늘 날짜 (기본값)
        shop_id: str
            쇼핑몰ID 검색 조건을 선택적으로 전달할 수 있다.

        Returns
        -------
        list[dict]
            사방넷 품번코드 매핑 내역
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
                    shop_id = shop_id,
                ))

    def count_total(self, response: JsonObject, **kwargs) -> int:
        """HTTP 응답에서 전체 매핑 건수를 추출한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(response, "data.metaData.total")

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
            "dayOption": "001",
            "startDate": start_date,
            "endDate": end_date,
            # "excelDownYn": "N",
            "pageSize": size,
            "shmaId": shop_id,
            "sortOption": "001",
            "sort": "DESC",
            "searchCondition": "",
            "searchKeyword": "",
            "currentPage": page,
        }


class SkuQuery(TypedDict):
    """사방넷 단품코드 매핑 내역 조회에 필요한 파라미터."""

    product_id_shop: str
    shop_id: str
    product_id: str


class SkuMapping(SabangnetAdmin):
    """사방넷 단품코드매핑관리 메뉴의 매핑문자열 보기 팝업에서 매핑문자열을 조회하는 클래스.

    - **Menu**: 주문관리 > 단품코드매핑관리 > 매핑문자열 보기
    - **API**: https://sbadmin{domain}.sabangnet.co.kr/prod-api/customer/order/SkuCodeMapping/getMpngHisSkuCodeMappingLists
    - **Referer**: https://sbadmin{domain}.sabangnet.co.kr/#/popup/views/pages/order/sku-code-mapping/mpng-his-sku-code-mapping-popup.vue

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
    path = "/prod-api/customer/order/SkuCodeMapping/getMpngHisSkuCodeMappingLists"
    default_options = {"RequestEach": {"request_delay": 0.3}}

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(self, query: Sequence[SkuQuery], **kwargs) -> JsonObject:
        """단품코드매핑관리 화면의 매핑문자열 보기 팝업에서 상품별 매핑문자열을 조회한다.

        Parameters
        ----------
        query: Sequence[SkuQuery]
            조회할 상품 식별 정보 목록. 각 항목은 아래 키를 포함해야 한다.
                - `product_id_shop`: 쇼핑몰상품코드
                - `shop_id`: 쇼핑몰ID
                - `product_id`: 품번코드

        Returns
        -------
        list[dict]
            사방넷 단품코드 매핑문자열 목록
        """
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
