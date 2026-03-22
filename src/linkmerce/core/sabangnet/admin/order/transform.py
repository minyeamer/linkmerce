from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.transform import TableKey


class Order(DuckDBTransformer):
    """사방넷 주문서 확인 처리 조회 결과를 `sabangnet_order_detail` 테이블에 적재하는 클래스."""

    extractor = "Order"
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


ORDER_TABLE_KEYS: list[TableKey] = ["order", "option", "invoice", "dispatch"]

class OrderDownload(DuckDBTransformer):
    """사방넷 주문서 확인 처리 다운로드 결과를 `download_type`에 따라 정해진 테이블에 변환 및 적재하는 클래스.

    테이블 키 | 테이블명 | 설명
    - `order` | `sabangnet_order` | 사방넷 주문 내역
    - `option` | `sabangnet_option` | 사방넷 주문 옵션 목록
    - `invoice` | `sabangnet_invoice` | 사방넷 발주 내역
    - `dispatch` | `sabangnet_dispatch` | 사방넷 발송 내역"""

    extractor = "OrderDownload"
    queries = ["create", *[f"bulk_insert_{key}" for key in ORDER_TABLE_KEYS]]
    tables = {key: f"sabangnet_{key}" for key in ORDER_TABLE_KEYS}
    parser = "excel"

    def pre_init(self, download_type: Literal["order", "option", "invoice", "dispatch"], **kwargs):
        """`download_type`에 따라 필드 스키마를 선택해 `parser_config`를 설정한다."""
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

    def bulk_insert(self, result: list[dict], query_key: str = "bulk_insert", **kwargs) -> list:
        """`download_type`에 맞는 `bulk_insert` 삽입 쿼리를 선택해 실행한다."""
        query_key = f"bulk_insert_{self.download_type}"
        return super().bulk_insert(result, query_key, **kwargs)


class OrderStatus(DuckDBTransformer):
    """사방넷 주문서 확인 처리 다운로드 결과로부터 주문 상태에 따른 변경 날짜를 파싱해
    `sabangnet_order_status` 테이블에 적재하는 클래스."""

    extractor = "OrderStatus"
    tables = {"table": "sabangnet_order_status"}
    parser = "excel"
    # parser_config = dict(
    #     fields = None, # ["주문번호(사방넷)", "주문일시(YYYY-MM-DD HH:MM)", ...],
    # )

    def prepare_bulk_params(self, result: list[dict], date_type: str, **kwargs) -> tuple[dict, dict, int]:
        """`date_type`에 따라 한글 날짜 칼럼 명칭을 구성하는 값을 렌더 컨텍스트에 추가한다."""
        render, params, total = super().prepare_bulk_params(result, **kwargs)

        date_format = self.date_format[date_type]
        time_format = "%Y%m%d" if date_format == "YYYYMMDD" else "%Y-%m-%d"
        render.update({"date_type": self.date_type[date_type], "date_format": date_format, "time_format": time_format})

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
    """사방넷 품번코드 매핑 내역을 `sabangnet_product_mapping` 테이블에 적재하는 클래스."""

    extractor = "ProductMapping"
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
    """사방넷 단품코드 매핑 내역을 `sabangnet_sku_mapping` 테이블에 적재하는 클래스."""

    extractor = "SkuMapping"
    tables = {"table": "sabangnet_sku_mapping"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data",
        fields = ["shmaPrdNo", "prdNo", "skuNo", "prdNm", "optDtlNm", "rn", "skuDscr", {"fstRegsDt": None}],
    )
    params = {"shop_id": "$query.shop_id"}
