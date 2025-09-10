from __future__ import annotations
from linkmerce.core.sabangnet.admin import SabangnetAdmin

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class Order(SabangnetAdmin):
    method = "POST"
    path = "/prod-api/customer/order/OrderConfirm/searchOrders"
    max_page_size = 500
    page_start = 1
    date_format = "%Y%m%d"

    @property
    def default_options(self) -> dict:
        return dict(PaginateAll = dict(delay=1), RequestEachPages = dict(delay=1))

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            date_type: str = "ord_dt",
            order_status_div: str = str(),
            order_status: Sequence[str] = list(),
            sort_type: str = "ord_no_asc",
            **kwargs
        ) -> JsonObject:
        kwargs = dict(kwargs,
            start_date=start_date, end_date=(start_date if end_date == ":start_date:" else end_date),
            date_type=date_type, order_status_div=order_status_div, order_status=order_status, sort_type=sort_type)

        return (self.paginate_all(self.request_json_safe, self.count_total, self.max_page_size, self.page_start)
                .run(**kwargs))

    def count_total(self, response: JsonObject, **kwargs) -> int:
        from linkmerce.utils.map import hier_get
        return hier_get(response, ["data","totAmtSummary","totCnt"])

    def build_request_json(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str,
            date_type: str = "ord_dt",
            order_status_div: str = str(),
            order_status: Sequence[str] = list(),
            sort_type: str = "ord_no_asc",
            page: int = 1,
            size: int = 500,
            **kwargs
        ) -> dict:
        return {
            "regDmStartTime": "00:00",
            "regDmEndTime": "23:00",
            "fnlChgPrgmNm": "order-confirm",
            "chkOrdNo": [],
            'currentPage': page,
            "dateDiv": date_type,
            "startDate": str(start_date).replace('-',''),
            "endDate": str(end_date).replace('-',''),
            "pageSize": size,
            "ordStsTpDivCd": order_status_div,
            "orderStrd": sort_type.rsplit('_', 1)[0],
            "orderDegreeStrd": sort_type.rsplit('_', 1)[1],
            'orderStatus': order_status,
            'multiplexId': [],
            'searchKeywordList': [],
        }

    def build_request_headers(self, **kwargs: str) -> dict[str,str]:
        from linkmerce.utils.headers import add_headers
        host = dict(host=self.origin, referer=self.origin, origin=self.origin)
        return add_headers(self.get_request_headers(), authorization=self.get_authorization(), **host)

    def set_request_headers(self, **kwargs: str):
        super().set_request_headers(contents="json", metadata="cors", **kwargs)

    @property
    def date_type(self) -> dict[str,str]:
        return {
            "hope_delv_date": "배송희망일", "reg_dm": "수집일", "ord_dt": "주문일", "cancel_rcv_dt": "취소접수일",
            "cancel_dt": "취소완료일", "rtn_rcv_dt": "반품접수일", "rtn_dt": "반품완료일",
            "delivery_confirm_date": "출고완료일", "chng_rcv_dt": "교환접수일", "chng_dt": "교환완료일",
            "dlvery_rcv_dt": "송장등록일", "inv_send_dm": "송장송신일", "stock_confirm_dm": "입출고완료일"
        }

    @property
    def sort_type(self) -> dict[str,str]:
        return {
            "fst_regs_dt": "수집일", "shpmt_hope_ymd": "배송희망일", "ord_no": "사방넷주문번호", "shma_id": "쇼핑몰",
            "shma_ord_no": "쇼핑몰주문번호", "clct_prd_nm": "수집상품명", "dcd_prd_nm": "확정상품명", "prd_no": "품번코드",
            "bypc_svc_acnt_id": "매입처", "rmte_zipcd": "우편번호", "ord_sts_cd": "주문상태"
        }

    @property
    def order_status_div(self) -> dict[str,str]:
        return {
            "001": "주문(진행)", "002": "주문(완료)", "003": "교발(진행)", "004": "교발(완료)",
            "005": "회수(진행)", "006": "회수(완료)"
        }

    @property
    def order_status(self) -> dict[str,str]:
        return {
            "001": "신규주문", "002": "주문확인", "003": "출고대기", "004": "출고완료", "006": "배송보류",
            "007": "취소접수", "008": "교환접수", "009": "반품접수", "010": "취소완료", "011": "교환완료",
            "012": "반품완료", "021": "교환발송준비", "022": "교환발송완료", "023": "교환회수준비", "024": "교환회수완료",
            "025": "반품회수준비", "026": "반품회수완료", "999": "폐기"
        }


class OrderDownload(Order):
    method = "POST"
    path = "/prod-api/customer/order/OrderConfirm/partner/downloadOrderConfirmExcelSearch"
    date_format = "%Y%m%d"

    @property
    def default_options(self) -> dict:
        return dict()

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(
            self,
            excel_form: int,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            date_type: str = "ord_dt",
            order_seq: list[int] = list(),
            order_status_div: str = str(),
            order_status: Sequence[str] = list(),
            sort_type: str = "ord_no_asc",
            **kwargs
        ) -> dict[str,bytes]:
        end_date = (start_date if end_date == ":start_date:" else end_date)
        headers = self.build_request_headers()
        body = self.build_request_json(excel_form, start_date, end_date, date_type, order_seq, order_status_div, order_status, sort_type)
        response = self.request(self.method, self.url, headers=headers, json=body)
        file_name = self.get_file_name(response.headers.get("Content-Disposition"))
        return {file_name: self.parse(response.content)}

    def get_file_name(self, content_disposition: str) -> str:
        if not isinstance(content_disposition, str):
            return "주문서확인처리.xlsx"
        from linkmerce.utils.regex import regexp_extract
        from urllib.parse import unquote
        return regexp_extract(r"(\d{8}_.*\.xlsx)", unquote(content_disposition)) or "주문서확인처리.xlsx"

    def build_request_json(
            self,
            excel_form: int,
            start_date: dt.date | str,
            end_date: dt.date | str,
            date_type: str = "ord_dt",
            order_seq: list[int] = list(),
            order_status_div: str = str(),
            order_status: Sequence[str] = list(),
            sort_type: str = "ord_no_asc",
            page: int = 1,
            size: int = 25,
            **kwargs
        ) -> dict:
        body = super().build_request_json(start_date, end_date, date_type, order_status_div, order_status, sort_type, page, size)
        return dict(body, **{
            "chkOrdNo": order_seq,
            "downloadScale": ("" if order_seq else "all"),
            "exclFormDivCd": "01",
            "exclFormSrno": str(excel_form),
            "excelTotalCount": 1,
            "excelPassword": None,
            "opaExcelDownloadName": "주문서확인처리",
        })


class OrderStatus(OrderDownload):

    @property
    def default_options(self) -> dict:
        return dict(RequestEach = dict(delay=1))

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(
            self,
            excel_form: int,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            date_type: list[str] = ["delivery_confirm_date", "cancel_dt", "rtn_dt", "chng_dt"],
            order_seq: list[int] = list(),
            order_status_div: str = str(),
            order_status: Sequence[str] = list(),
            sort_type: str = "ord_no_asc",
            **kwargs
        ) -> dict[str,bytes]:
        kwargs = dict(kwargs,
            excel_form=excel_form, start_date=start_date, end_date=(start_date if end_date == ":start_date:" else end_date),
            order_seq=order_seq, order_status_div=order_status_div, order_status=order_status, sort_type=sort_type)

        keys = [self.date_type[dt] for dt in date_type]
        return dict(zip(keys, self.request_each(self.request_content).partial(**kwargs).expand(date_type=date_type).run()))
