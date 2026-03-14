from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


class PageViewParser(JsonTransformer):
    dtype = dict
    scope = "data.storePageView.items"

    def assert_valid_response(self, obj: dict, **kwargs):
        super().assert_valid_response(obj)
        if "error" in obj:
            from linkmerce.utils.nested import hier_get
            msg = hier_get(obj, ["error","error"]) or "null"
            if msg == "Unauthorized":
                from linkmerce.common.exceptions import UnauthorizedError
                raise UnauthorizedError("Unauthorized request")
            super().raise_request_error(f"An error occurred during the request: {msg}")

    def parse(self, obj: list[dict], **kwargs) -> list[dict]:
        from linkmerce.utils.nested import hier_get
        items = list()
        for daily in obj:
            date = daily["period"]["date"]
            for item in daily["items"]:
                items.append(dict(item, ymd=date))
        return items


class PageViewByDevice(DuckDBTransformer):
    tables = {"table": "naver_pv_by_device"}
    parser = PageViewParser
    parser_config = dict(
        fields = ["measuredThrough.device", {"visit": ["pageClick", "userClick", "timeOnSite"]}, "ymd"],
        defaults = {"mallSeq": "$mall_seq"}
    )


class PageViewByProduct(DuckDBTransformer):
    tables = {"table": "naver_pv_by_product"}
    parser = PageViewParser
    parser_config = dict(
        fields = ["measuredThrough.url", {"visit": ["pageClick", "userClick", "timeOnSite"]}, "ymd"],
        defaults = {"mallSeq": "$mall_seq"}
    )


class PageViewByUrl(DuckDBTransformer):
    tables = {"table": "naver_pv_by_url"}
    parser = PageViewParser
    parser_config = dict(
        fields = ["measuredThrough.url", {"visit": ["pageClick", "userClick", "timeOnSite"]}, "ymd"],
        defaults = {"mallSeq": "$mall_seq"}
    )
