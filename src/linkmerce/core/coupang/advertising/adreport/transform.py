from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject


class MarketingReport(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, vendor_id: str | None = None, **kwargs):
        from linkmerce.utils.excel import excel2json
        reports = excel2json(obj, warnings=False)
        if reports:
            return self.insert_into_table(reports, params=dict(vendor_id=vendor_id))
