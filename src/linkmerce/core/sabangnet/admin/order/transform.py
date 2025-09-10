from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject


class OrderList(JsonTransformer):
    dtype = dict
    path = ["data", "orderList"]


class Order(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, **kwargs):
        orders = OrderList().transform(obj)
        if orders:
            return self.insert_into_table(orders)


class OrderDownload(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: bytes, **kwargs):
        from linkmerce.utils.openpyxl import excel2json
        orders = excel2json(obj, warnings=False)
        if orders:
            return self.insert_into_table(orders)


class OrderStatus(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: bytes, date_type: str, **kwargs):
        from linkmerce.utils.openpyxl import excel2json
        orders = excel2json(obj, warnings=False)
        if orders:
            date_format = self.date_format[date_type]
            time_format = "%Y%m%d" if date_format == "YYYYMMDD" else "%Y-%m-%d"
            render = dict(date_type=self.date_type[date_type], date_format=date_format, time_format=time_format)
            return self.insert_into_table(orders, render=render)

    @property
    def date_type(self) -> dict[str,str]:
        return {
            # "hope_delv_date": "배송희망일", "reg_dm": "수집일", "ord_dt": "주문일",
            "cancel_rcv_dt": "취소접수일", "cancel_dt": "취소완료일", "rtn_rcv_dt": "반품접수일", "rtn_dt": "반품완료일",
            "delivery_confirm_date": "출고완료일", "chng_rcv_dt": "교환접수일", "chng_dt": "교환완료일",
            # "dlvery_rcv_dt": "송장등록일", "inv_send_dm": "송장전송일"
        }

    @property
    def date_format(self) -> dict[str,str]:
        return {
            # "hope_delv_date": "YYYY-MM-DD", "reg_dm": "YYYY-MM-DD", "ord_dt": "YYYY-MM-DD",
            "cancel_rcv_dt": "YYYYMMDD", "cancel_dt": "YYYYMMDD", "rtn_rcv_dt": "YYYY-MM-DD", "rtn_dt": "YYYY-MM-DD",
            "delivery_confirm_date": "YYYYMMDD", "chng_rcv_dt": "YYYY-MM-DD", "chng_dt": "YYYY-MM-DD",
            # "dlvery_rcv_dt": "YYYYMMDD", "inv_send_dm": "YYYYMMDD"
        }
