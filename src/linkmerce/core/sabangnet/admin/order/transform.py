from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal


class Order(DuckDBTransformer):
    tables = {"table": "sabangnet_order_detail"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data.orderList",
        fields = [
            "ordNo", "orgnOrdNo", "shmaOrdNo", "ordStsTpDivCd", "ordStsCd", "shmaId", "shmaNm",
            "shmaCnctnLoginId", "acntRegsSrno", "prdNo", "skuNo", "onsfPrdCd", "prdSplyStsNm",
            "clctPrdNm", "dcdPrdNm", "prdAbbrRmrk", "clctSkuNm", "dcdSkuNm", "modlNm", "wyblNo",
            "pcscpNm", "ordQt", "skuQt", "ordSumAmt", "shmaSplyUprc", "cprcSumAmt",
            "fstRegsDt", "shpmtHopeYmd", "wyblTrnmDt"
        ],
    )


class OrderDownload(DuckDBTransformer):
    queries = [
        "create", "bulk_insert_order", "bulk_insert_option", "bulk_insert_invoice", "bulk_insert_dispatch"
    ]
    tables = {
        "order": "sabangnet_order",
        "option": "sabangnet_option",
        "invoice": "sabangnet_invoice",
        "dispatch": "sabangnet_dispatch"
    }
    parser = "excel"

    def pre_init(self, download_type: Literal["order", "option", "invoice", "dispatch"], **kwargs):
        if download_type == "order":
            fields = [
                "주문번호(사방넷)", "원주문번호(사방넷)", "주문번호(쇼핑몰)", "부주문번호", "계정등록순번",
                "상품코드(사방넷)", "상품코드(쇼핑몰)", "수량", "EA(확정)", "결제금액", "주문금액",
                "주문일시(YYYY-MM-DD HH:MM)", "수집일시(YYYY-MM-DD HH:MM:SS)"
            ]
        elif download_type == "option":
            fields = [
                "상품코드(사방넷)", "상품코드(쇼핑몰)", "계정등록순번", "모델명", "자체상품코드",
                "상품명(확정)", "상품명(수집)", "상품약어", "옵션(확정)", "옵션(수집)", "옵션별칭",
                "판매가(상품)", "주문번호(쇼핑몰)", "주문일시(YYYY-MM-DD HH:MM)"
            ]
        elif download_type == "invoice":
            fields = [
                "주문번호(사방넷)", "계정등록순번", "송장번호", "택배사", "주문구분", "주문상태",
                "주문일시(YYYY-MM-DD HH:MM)", "송장등록일자(YYYY-MM-DD)"
            ]
        elif download_type == "dispatch":
            fields = [
                "주문번호(사방넷)", "주문번호(쇼핑몰)", "계정등록순번", "상품코드(사방넷)", "EA(확정)",
                "주문자명", "수취인명", "수취인우편번호1", "수취인주소1", "수취인전화번호1", "수취인전화번호2",
                "배송메세지1", "박스타입", "운임구분", "수집일시(YYYY-MM-DD HH:MM:SS)", "주문일시(YYYY-MM-DD HH:MM)"
            ]
        else:
            self.raise_parse_error(f"Parsing for download type '{download_type}' is not supported.")

        self.download_type = download_type
        self.parser_config = dict(fields=fields)

    def bulk_insert(self, result: list[dict], **kwargs):
        render, params, total = self.prepare_bulk_params(result, **kwargs)
        if total > 0:
            query = self.prepare_query(key=f"bulk_insert_{self.download_type}", render=render)
            return self.execute(query, **params)


class OrderStatus(DuckDBTransformer):
    tables = {"table": "sabangnet_order_status"}
    parser = "excel"
    # parser_config = dict(
    #     fields = None, # ["주문번호(사방넷)", "주문일시(YYYY-MM-DD HH:MM)", ...],
    # )

    def prepare_bulk_params(self, result: list[dict], date_type: str, **kwargs) -> tuple[dict, dict, int]:
        render, params, total = super().prepare_bulk_params(result, **kwargs)

        date_format = self.date_format[date_type]
        time_format = "%Y%m%d" if date_format == "YYYYMMDD" else "%Y-%m-%d"
        render.update(date_type=self.date_type[date_type], date_format=date_format, time_format=time_format)

        return render, params, total

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


class ProductMapping(DuckDBTransformer):
    tables = {"table": "sabangnet_product_mapping"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data.list",
        fields = [
            "shmaPrdNo", "prdNo", "acntRegsSrno", "shmaId", "shmaNm", "shmaCnctnLoginId",
            "prdNm", "onsfPrdCd", "sepr", "mpngCnt"
        ],
    )


class SkuMapping(DuckDBTransformer):
    tables = {"table": "sabangnet_sku_mapping"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data",
        fields = ["shmaPrdNo", "prdNo", "skuNo", "prdNm", "optDtlNm", "rn", "skuDscr", "fstRegsDt"],
        defaults = {"shopId": "$query.shop_id"},
    )
