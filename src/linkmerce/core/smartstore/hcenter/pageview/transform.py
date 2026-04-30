from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


class PageViewParser(JsonTransformer):
    """네이버 브랜드 스토어의 방문 통계 데이터를 파싱하는 클래스."""

    dtype = dict
    scope = "data.storePageView.items"

    def parse(self, obj: list[dict], **kwargs) -> list[dict]:
        """일자(`period.date`)를 각 항목에 추가하면서 평탄화한 페이지뷰 목록을 반환한다."""
        items = list()
        for daily in obj:
            date = daily["period"]["date"]
            for item in daily["items"]:
                items.append(dict(item, ymd=date))
        return items


class PageViewByDevice(DuckDBTransformer):
    """네이버 브랜드 스토어의 일별/기기별 방문 통계 데이터를 변환 및 적재하는 클래스.

    - **Extractor**: `PageViewByDevice`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `PageViewParser: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: naver_pv_by_device`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    mall_seq: int | str
        쇼핑몰 순번
    """

    extractor = "PageViewByDevice"
    tables = {"table": "naver_pv_by_device"}
    parser = PageViewParser
    parser_config = dict(
        fields = ["measuredThrough.device", {"visit": ["pageClick", "userClick", "timeOnSite"]}, "ymd"],
    )
    params = {"mall_seq": "$mall_seq"}


class PageViewByUrl(DuckDBTransformer):
    """네이버 브랜드 스토어의 일별/URL별 방문 통계 데이터를 변환 및 적재하는 클래스.

    - **Extractor**: `PageViewByUrl`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `PageViewParser: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: naver_pv_by_url`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    mall_seq: int | str
        쇼핑몰 순번
    """

    extractor = "PageViewByUrl"
    tables = {"table": "naver_pv_by_url"}
    parser = PageViewParser
    parser_config = dict(
        fields = ["measuredThrough.url", {"visit": ["pageClick", "userClick", "timeOnSite"]}, "ymd"],
    )
    params = {"mall_seq": "$mall_seq"}


class PageViewByProduct(PageViewByUrl):
    """네이버 브랜드 스토어의 일별/URL별 방문 통계 데이터를 일별/상품별 데이터로 변환 및 적재하는 클래스.

    - **Extractor**: `PageViewByUrl`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `PageViewParser`: `dict` -> `list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table`: `naver_pv_by_product`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    mall_seq: int | str
        쇼핑몰 순번
    """

    extractor = "PageViewByUrl"
    tables = {"table": "naver_pv_by_product"}
    parser = PageViewParser
    parser_config = dict(
        fields = ["measuredThrough.url", {"visit": ["pageClick", "userClick", "timeOnSite"]}, "ymd"],
    )
    params = {"mall_seq": "$mall_seq"}
