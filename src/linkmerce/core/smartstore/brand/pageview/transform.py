from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


class PageViewParser(JsonTransformer):
    """네이버 브랜드 스토어의 일별 페이지뷰 데이터를 추출하는 파서 클래스."""

    dtype = dict
    scope = "data.storePageView.items"

    def assert_valid_response(self, obj: dict, **kwargs):
        """`error` 필드가 있으면 `UnauthorizedError` 또는 `RequestError`를 발생시킨다."""
        super().assert_valid_response(obj)
        if "error" in obj:
            from linkmerce.utils.nested import hier_get
            msg = hier_get(obj, "error.error") or "null"
            if msg == "Unauthorized":
                from linkmerce.common.exceptions import UnauthorizedError
                raise UnauthorizedError("Unauthorized request")
            super().raise_request_error(f"An error occurred during the request: {msg}")

    def parse(self, obj: list[dict], **kwargs) -> list[dict]:
        """일자(period.date)를 각 항목에 추가해 평탄화된 페이지뷰 목록을 반환한다."""
        items = list()
        for daily in obj:
            date = daily["period"]["date"]
            for item in daily["items"]:
                items.append(dict(item, ymd=date))
        return items


class PageViewByDevice(DuckDBTransformer):
    """네이버 브랜드 스토어의 일별/기기별 페이지뷰 데이터를 `naver_pv_by_device` 테이블에 적재하는 클래스."""

    tables = {"table": "naver_pv_by_device"}
    parser = PageViewParser
    parser_config = dict(
        fields = ["measuredThrough.device", {"visit": ["pageClick", "userClick", "timeOnSite"]}, "ymd"],
    )
    params = {"mall_seq": "$mall_seq"}


class PageViewByUrl(DuckDBTransformer):
    """네이버 브랜드 스토어의 일별/URL별 페이지뷰 데이터를 `naver_pv_by_url` 테이블에 적재하는 클래스."""

    tables = {"table": "naver_pv_by_url"}
    parser = PageViewParser
    parser_config = dict(
        fields = ["measuredThrough.url", {"visit": ["pageClick", "userClick", "timeOnSite"]}, "ymd"],
    )
    params = {"mall_seq": "$mall_seq"}


class PageViewByProduct(DuckDBTransformer):
    """네이버 브랜드 스토어의 일별/상품별 페이지뷰 데이터를 `naver_pv_by_product` 테이블에 적재하는 클래스."""

    tables = {"table": "naver_pv_by_product"}
    parser = PageViewParser
    parser_config = dict(
        fields = ["measuredThrough.url", {"visit": ["pageClick", "userClick", "timeOnSite"]}, "ymd"],
    )
    params = {"mall_seq": "$mall_seq"}
