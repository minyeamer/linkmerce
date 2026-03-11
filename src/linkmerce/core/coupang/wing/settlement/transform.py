from __future__ import annotations

from linkmerce.common.transform import ExcelTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal


class RocketSettlement(DuckDBTransformer):
    tables = {"table": "coupang_rocket_settlement"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "settlementStatusReports",
        fields = {
            ".": [
                "settlementGroupKey", "settlementRatio", "finalSettlementAmount",
                "settlementPeriodStartDate", "settlementPeriodEndDate"
            ],
            "settlementStatusReportDetail": [
                "totalSalesAmount", "totalRefundedAmount", "totalTakeRateAmountWithVat", "totalSellerDiscount",
                "totalSellerFundedInstantDiscount", "totalSellerFundedDownloadDiscount", "totalPayableAmount",
                "totalMilkRunDeductionAmount", "totalAdSalesDeductionAmount", "totalAdditionalDeductionAmount",
                "totalNegativeDeductionAmount", "totalFinalCfsFeeDeductionAmount", "totalWarehousingFeeDeductionAmount",
                "totalFulfillmentFeeDeductionAmount", "totalStorageFeeDeductionAmount",
                "totalCreturnReverseShippingFeeDeductionAmount", "totalCreturnGradingFeeDeductionAmount",
                "totalVreturnHandlingFeeDeductionAmount", "totalBarcodeLabelingFeeDeductionAmount",
                "totalLastSettlementUnpaidCfsDeductionAmount", "totalPastCfsDeductionAmount",
                "totalCarryOverSettlementDeductionAmount", "totalCfsInventoryCompensationAmount"
            ]
        },
        defaults = {"vendorId": "$vendor_id"},
        on_missing = "raise",
    )


class RocketSalesParser(ExcelTransformer):
    header = 2
    fields = [
        "주문ID", "등록상품 ID", "옵션ID", "SKU ID", "등록상품명", "옵션명", "카테고리ID", "카테고리명",
        "거래유형", "정산유형", "판매가(A)", "판매수량(B)", "판매액(A*B)", "쿠팡지원할인(C)", "매출금액(A*B-C)",
        "즉시할인쿠폰(D)", "다운로드쿠폰(E)", "판매자할인쿠폰(D+E)", "정산대상액", "판매수수료", "판매수수료 VAT",
        "매출인식일", "정산주기(종료일)"
    ]
    defaults = {"vendorId": "$vendor_id"}
    on_missing = "raise"


class RocketShippingParser(ExcelTransformer):
    header = None
    fields = [
        "주문ID", "배송ID", "등록상품 ID", "옵션ID", "SKU ID", "등록상품명", "옵션명", "1차", "2차",
        "개별포장 상품 사이즈", "물류센터", "거래유형", "정산유형", "단품 판매가", "단품 기준 구매 수량",
        "판매수량", "발생비용(A)", "할인가(B)", "추가비용", "주문일", "매출인식일", "정산주기(종료일)"
    ]
    defaults = {"vendorId": "$vendor_id"}
    on_missing = "ignore"

    def parse(self, obj: bytes, **kwargs) -> list[dict]:
        from linkmerce.utils.excel import filter_warnings
        from io import BytesIO
        import openpyxl
        filter_warnings()

        wb = openpyxl.load_workbook(BytesIO(obj))
        report = list()

        for sheet_name in ["입출고비", "배송비"]:
            ws = wb[sheet_name]
            headers1 = [cell.value for cell in next(ws.iter_rows(min_row=7, max_row=7))]
            headers2 = [cell.value for cell in next(ws.iter_rows(min_row=8, max_row=8))]
            headers = [(header2 if header2 else header1) for header1, header2 in zip(headers1, headers2)]
            report += [dict(zip(headers, row)) for row in ws.iter_rows(min_row=9, values_only=True)]

        return report


class RocketSettlementDownload(DuckDBTransformer):
    queries = ["create", "bulk_insert_sales", "bulk_insert_shipping"]
    tables = {"sales": "coupang_rocket_sales", "shipping": "coupang_rocket_shipping"}

    def parse(self, obj: bytes, report_type: Literal["CATEGORY_TR","WAREHOUSING_SHIPPING"], **kwargs) -> list[dict]:
        config = self.parser_config or dict()
        if report_type == "CATEGORY_TR":
            RocketSalesParser(**config).transform(obj, **kwargs)
        elif report_type == "WAREHOUSING_SHIPPING":
            RocketShippingParser(**config).transform(obj, **kwargs)
        else:
            self.raise_parse_error(f"Parsing for report type '{report_type}' is not supported.")

    def bulk_insert(self, result: list[dict], report_type: Literal["CATEGORY_TR","WAREHOUSING_SHIPPING"], **kwargs):
        if len(result) > 0:
            table = "sales" if report_type == "CATEGORY_TR" else "shipping"
            render = {table: self.tables[table], f"{table}_rows": self.expr_rows(f"{table}_rows")}
            query = self.prepare_query(key=f"bulk_insert_{table}", render=render)
            return self.execute(query, **{f"{table}_rows": result})
