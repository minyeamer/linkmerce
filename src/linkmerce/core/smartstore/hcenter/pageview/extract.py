from __future__ import annotations
from linkmerce.core.smartstore.hcenter import PartnerCenter

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class _PageView(PartnerCenter):
    """네이버 브랜드 스토어의 일별 페이지뷰 데이터를 조회하는 공통 클래스.

    `RequestEachLoop` Task를 사용하여 판매처번호(`mall_seq`)에 대한 GraphQL API 요청으로   
    기기별/URL별 페이지뷰를 조회한다. 조회 기간은 최대 90일로 제한된다."""

    method = "POST"
    path = "/brand/content"
    date_format = "%Y-%m-%d"
    days_limit = 90
    aggregate_by: Literal["Device", "Url"]

    @property
    def default_options(self) -> dict:
        return {
            "RequestLoop": {"max_retries": 5, "ignored_errors": Exception},
            "RequestEachLoop": {"request_delay": 1, "max_concurrent": 3},
        }

    def is_valid_response(self, response: JsonObject) -> bool:
        return isinstance(response, dict)

    def count_total(self, response: JsonObject, **kwargs) -> int:
        """HTTP 응답에서 전체 항목 수를 추출한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(response, "data.storePageView.count")

    def build_request_json(
            self,
            mall_seq: int | str,
            start_date: dt.date,
            end_date: dt.date,
            date_type: Literal["daily", "weekly", "monthly"] = "daily",
            page: int = 1,
            page_size: int = 10000,
            **kwargs
        ) -> dict:
        pageable = (self.aggregate_by == "Url")
        return dict(self.get_request_body(),
            variables={
                "queryRequest": {
                    "mallSequence": str(mall_seq),
                    "dateType": date_type.capitalize(),
                    "startDate": str(start_date),
                    "endDate": str(end_date),
                    "aggregateBy": self.aggregate_by,
                    **({"pageable": {"page":int(page), "size":int(page_size)}} if pageable else dict())
                }
            })

    def set_request_body(self):
        from linkmerce.utils.graphql import GraphQLOperation, GraphQLSelection
        super().set_request_body(
            GraphQLOperation(
                operation = "getStorePageView",
                variables = {"queryRequest": dict()},
                types = {"queryRequest": "StoreTrafficRequest"},
                selection = GraphQLSelection(
                    name = "storePageView",
                    variables = ["queryRequest"],
                    fields = ["count", self.fields],
                    typename = False,
                )
            ).generate_body(query_options = {
                "selection": {"variables": {"linebreak": False}, "fields": {"linebreak": True}},
                "suffix": '\n',
            }))

    @PartnerCenter.cookies_required
    def set_request_headers(self, **kwargs):
        contents = {"type": "text", "charset": "UTF-8"}
        referer = self.origin + "/iframe/brand-analytics/store/pageView"
        super().set_request_headers(contents=contents, origin=self.origin, referer=referer, **kwargs)

    @property
    def fields(self) -> list[dict]:
        """페이지뷰 GraphQL 응답 필드 목록을 반환한다."""
        metrics = ["pageClick", "userClick", "timeOnSite", "pageClickPerUser", "timeOnSitePerClick", "timeOnSitePerUser"]
        return [{
            "items": [
                {"period": ["date"]},
                {"items": [
                    {"visit": metrics},
                    {"measuredThrough": ["device", "url"]}]},
            ]
        }]


class PageViewByDevice(_PageView):
    """네이버 브랜드 스토어의 일별/기기별 페이지뷰 데이터를 조회하는 클래스."""

    aggregate_by = "Device"

    @PartnerCenter.with_session
    def extract(
            self,
            mall_seq: int | str | Iterable[int | str],
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            **kwargs
        ) -> JsonObject:
        """네이버 브랜드 스토어의 일별/기기별 페이지뷰 데이터를 동기 방식으로 순차 조회해 JSON 형식으로 반환한다."""
        context = self.split_date_context(start_date, end_date, delta=self.days_limit, format=self.date_format)
        return (self.request_each_loop(self.request_json, context=context)
                .expand(mall_seq=mall_seq)
                .loop(self.is_valid_response)
                .run())

    @PartnerCenter.async_with_session
    async def extract_async(
            self,
            mall_seq: int | str | Iterable[int | str],
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            **kwargs
        ) -> JsonObject:
        """네이버 브랜드 스토어의 일별/기기별 페이지뷰 데이터를 비동기 방식으로 병렬 조회해 JSON 형식으로 반환한다."""
        context = self.split_date_context(start_date, end_date, delta=self.days_limit, format=self.date_format)
        return await (self.request_each_loop(self.request_async_json, context=context)
                .expand(mall_seq=mall_seq)
                .loop(self.is_valid_response)
                .run_async())


class PageViewByUrl(_PageView):
    """네이버 브랜드 스토어의 일별/URL별 페이지뷰 데이터를 조회하는 클래스."""

    aggregate_by = "Url"
    page_size = 10000
    page_start = 1

    @PartnerCenter.with_session
    def extract(
            self,
            mall_seq: int | str | Iterable[int | str],
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            **kwargs
        ) -> JsonObject:
        """네이버 브랜드 스토어의 일별/URL별 페이지뷰 데이터를 동기 방식으로 순차 조회해 JSON 형식으로 반환한다."""
        context = self.split_date_context(start_date, end_date, delta=self.days_limit, format=self.date_format)
        return (self.request_each_loop(self.request_json, context=context)
                .expand(mall_seq=mall_seq)
                .loop(self.is_valid_response)
                .run())

    @PartnerCenter.async_with_session
    async def extract_async(
            self,
            mall_seq: int | str | Iterable[int | str],
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            **kwargs
        ) -> JsonObject:
        """네이버 브랜드 스토어의 일별/URL별 페이지뷰 데이터를 비동기 방식으로 병렬 조회해 JSON 형식으로 반환한다."""
        context = self.split_date_context(start_date, end_date, delta=self.days_limit, format=self.date_format)
        return await (self.request_each_loop(self.request_async_json, context=context)
                .expand(mall_seq=mall_seq)
                .loop(self.is_valid_response)
                .run_async())
