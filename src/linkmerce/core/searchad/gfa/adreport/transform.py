from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject


class Content(JsonTransformer):
    path = ["content"]


class Campaign(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, **kwargs):
        campaigns = Content().transform(obj)
        if campaigns:
            self.insert_into_table(campaigns)


class AdSet(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, account_no: int | str, **kwargs):
        adsets = Content().transform(obj)
        if adsets:
            self.insert_into_table(adsets, params=dict(account_no=account_no))


class Creative(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, account_no: int | str, **kwargs):
        adsets = Content().transform(obj)
        if adsets:
            self.insert_into_table(adsets, params=dict(account_no=account_no))


class PerformanceDetail(JsonTransformer):
    path = ["reportPerformanceDetailResponseList"]

    def transform(self, obj: JsonObject, **kwargs) -> JsonObject:
        report = [row for row in obj["reportPerformanceDetailResponseList"]]
        if report:
            self.validate_row(report[0])
        return report

    def validate_row(self, row: dict):
        for key in ["reachCount", "impCount", "clickCount", "spend"]:
            if key not in row:
                row.update({key: None})
        row.update(conversion=self.validate_conversion(row.get("conversion") or dict()))

    def validate_conversion(self, conversion: dict) -> dict:
        for key in ["convCount", "convSalesKRW"]:
            if key not in conversion:
                conversion[key] = None
        return conversion


class PerformanceReport(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, **kwargs):
        report_json = PerformanceDetail().transform(obj)
        if report_json:
            self.insert_into_table(report_json)
