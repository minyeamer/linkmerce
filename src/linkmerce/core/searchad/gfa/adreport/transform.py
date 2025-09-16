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


class PerformanceReport(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: bytes, account_no: int | str, **kwargs):
        from linkmerce.utils.excel import csv2json
        report_json = csv2json(self.unzip(obj), header=0, encoding="utf-8-sig")
        if report_json:
            self.insert_into_table(report_json, params=dict(account_no=account_no))

    def unzip(self, obj: bytes) -> bytes:
        from io import BytesIO
        import zipfile
        with zipfile.ZipFile(BytesIO(obj)) as zf:
            for name in zf.namelist():
                if name.endswith(".csv"):
                    return zf.read(name)
