from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.transform import JsonObject


class PageViewItems(JsonTransformer):
    dtype = dict
    path = ["data","storePageView","items"]

    def parse(self, obj: JsonObject, **kwargs) -> JsonObject:
        from linkmerce.utils.map import hier_get
        items = list()
        for daily in hier_get(obj, self.path, default=list()):
            date = daily["period"]["date"]
            for item in daily["items"]:
                items.append(dict(item, ymd=date))
        return items

    def is_valid_response(self, obj: dict) -> bool:
        if "error" in obj:
            from linkmerce.utils.map import hier_get
            msg = hier_get(obj, ["error","error"]) or "null"
            if msg == "Unauthorized":
                from linkmerce.common.exceptions import UnauthorizedError
                raise UnauthorizedError("Unauthorized request")
            super().raise_request_error(f"An error occurred during the request: {msg}")
        return True


class _PageView(DuckDBTransformer):
    aggregate_by: Literal["Device","Url"]
    queries: list[str] = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, mall_seq: int | str, **kwargs):
        items = PageViewItems().transform(obj)
        if items:
            self.insert_into_table(items, params=dict(mall_seq=mall_seq))


class PageViewByDevice(_PageView):
    sales_type = "Device"
    queries = ["create", "select", "insert"]


class PageViewByProduct(_PageView):
    sales_type = "Url"
    queries = ["create", "select", "insert"]


class PageViewByUrl(_PageView):
    sales_type = "Url"
    queries = ["create", "select", "insert"]
