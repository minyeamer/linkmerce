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
    datetime_format = "%Y%m%d%H%M%S"

    @property
    def default_options(self) -> dict:
        return dict(PaginateAll = dict(request_delay=1))

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(
            self,
            start_date: dt.datetime | dt.date | str | Literal[":today:"] = ":today:",
            end_date: dt.datetime | dt.date | str | Literal[":start_date:",":now:"] = ":start_date:",
            date_type: str = "reg_dm",
            order_status_div: str = str(),
            order_status: Sequence[str] = list(),
            shop_id: str = str(),
            sort_type: str = "ord_no_asc",
            **kwargs
        ) -> JsonObject:
        from linkmerce.core.sabangnet.admin import get_order_date_pair
        kwargs = dict(
            dict(zip(["start_date","end_date"], get_order_date_pair(start_date, end_date))),
            date_type=date_type, order_status_div=order_status_div, order_status=order_status,
            shop_id=shop_id, sort_type=sort_type)

        return (self.paginate_all(self.request_json_safe, self.count_total, self.max_page_size, self.page_start)
                .run(**kwargs))

    def count_total(self, response: JsonObject, **kwargs) -> int:
        from linkmerce.utils.map import hier_get
        return hier_get(response, ["data","totAmtSummary","totCnt"])

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
            "chkOrdNo": [],
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
            "multiplexId": [],
            "searchKeywordList": [],
        }

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
    datetime_format = "%Y%m%d%H%M%S"

    @property
    def default_options(self) -> dict:
        return dict()

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(
            self,
            download_no: int,
            start_date: dt.datetime | dt.date | str | Literal[":today:"] = ":today:",
            end_date: dt.datetime | dt.date | str | Literal[":start_date:",":now:"] = ":start_date:",
            date_type: str = "reg_dm",
            order_seq: list[int] = list(),
            order_status_div: str = str(),
            order_status: Sequence[str] = list(),
            shop_id: str = str(),
            sort_type: str = "ord_no_asc",
            **kwargs
        ) -> dict[str,bytes]:
        from linkmerce.core.sabangnet.admin import get_order_date_pair
        dates = get_order_date_pair(start_date, end_date)
        headers = self.build_request_headers()
        body = self.build_request_json(download_no, *dates, date_type, order_seq, order_status_div, order_status, shop_id, sort_type)
        response = self.request(self.method, self.url, headers=headers, json=body)
        file_name = self.get_file_name(response.headers.get("Content-Disposition"))
        return {file_name: self.parse(response.content)}

    def get_file_name(self, content_disposition: str) -> str:
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

    @property
    def default_options(self) -> dict:
        return dict(RequestEach = dict(request_delay=1))

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(
            self,
            excel_form: int,
            start_date: dt.datetime | dt.date | str | Literal[":today:"] = ":today:",
            end_date: dt.datetime | dt.date | str | Literal[":start_date:",":now:"] = ":start_date:",
            date_type: list[str] = ["delivery_confirm_date", "cancel_dt", "rtn_dt", "chng_dt"],
            order_seq: list[int] = list(),
            order_status_div: str = str(),
            order_status: Sequence[str] = list(),
            shop_id: str = str(),
            sort_type: str = "ord_no_asc",
            **kwargs
        ) -> dict[str,bytes]:
        from linkmerce.core.sabangnet.admin import get_order_date_pair
        kwargs = dict(
            dict(zip(["start_date","end_date"], get_order_date_pair(start_date, end_date))),
            excel_form=excel_form, order_seq=order_seq, order_status_div=order_status_div, order_status=order_status,
            shop_id=shop_id, sort_type=sort_type)

        keys = [self.date_type[dt] for dt in date_type]
        return dict(zip(keys, self.request_each(self.request_content).partial(**kwargs).expand(date_type=date_type).run()))


class ProductMapping(SabangnetAdmin):
    method = "POST"
    # path = "/prod-api/customer/order/ProductCodeMapping/getProductCodeMappingSearch"
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
            **kwargs
        ) -> JsonObject:
        from linkmerce.core.sabangnet.admin import get_product_date_pair
        start_date, end_date = get_product_date_pair(start_date, end_date)
        return (self.paginate_all(self.request_json_safe, self.count_total, self.max_page_size, self.page_start)
                .run(start_date=start_date, end_date=end_date, shop_id=shop_id))

    def count_total(self, response: JsonObject, **kwargs) -> int:
        from linkmerce.utils.map import hier_get
        return hier_get(response, ["data","metaData","total"])

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


class SkuQuery(dict):
    def __init__(self, product_id_shop: str, shop_id: str, product_id: str, **kwargs):
        super().__init__(
            product_id_shop = product_id_shop,
            shop_id = shop_id,
            product_id = product_id,
        )


class SkuMapping(SabangnetAdmin):
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
