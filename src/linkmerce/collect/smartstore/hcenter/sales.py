from __future__ import annotations
from linkmerce.collect.smartstore.hcenter import PartnerCenter

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.types import JsonObject, TaskOptions
    import datetime as dt


class _SalesCollector(PartnerCenter):
    method = "POST"
    path = "/brand/content"
    args = ["mall_seq", "start_date", "end_date", "date_type", "page", "page_size"]
    date_format = "%Y-%m-%d"
    sales_type: Literal["store","category","product"]
    fields: list[dict]

    def set_options(self, options: TaskOptions = dict()):
        super().set_options(options or dict(RequestEach=dict(delay=1, limit=3)))

    @PartnerCenter.with_session
    def collect(
            self,
            mall_seq: int | str | Iterable[int | str],
            start_date: dt.date | str,
            end_date: dt.date | str,
            date_type: Literal["daily","weekly","monthly"] = "daily",
            page: int | Iterable[int] = 1,
            page_size: int = 1000,
            **kwargs
        ) -> JsonObject:
        date_pairs = self.generate_date_pairs(start_date, end_date, freq=date_type[0], format=self.date_format)
        return (self.request_each(self._build_and_request)
                .partial(date_type=date_type, page_size=page_size).expand(mall_seq=mall_seq, date_pairs=date_pairs, page=page)
                .parse(**self.update_parser(**kwargs)).concat("auto").run())

    @PartnerCenter.async_with_session
    async def collect_async(
            self,
            mall_seq: int | str,
            start_date: dt.date | str,
            end_date: dt.date | str,
            date_type: Literal["daily","weekly","monthly"] = "daily",
            page: int = 1,
            page_size: int = 1000,
            **kwargs
        ) -> JsonObject:
        date_pairs = self.generate_date_pairs(start_date, end_date, freq=date_type[0], format=self.date_format)
        return await (self.request_each(self._build_and_request_async)
                .partial(date_type=date_type, page_size=page_size).expand(mall_seq=mall_seq, date_pairs=date_pairs, page=page)
                .parse(**self.update_parser(**kwargs)).concat("auto").run_async())

    def _build_and_request(self, **json) -> JsonObject:
        message = self.build_request(json=json)
        return self.request_json(**message)

    async def _build_and_request_async(self, **json) -> JsonObject:
        message = self.build_request(json=json)
        return await self.request_async_json(**message)

    def get_request_body(
            self,
            mall_seq: int | str,
            date_pairs: tuple[dt.date,dt.date],
            date_type: Literal["daily","weekly","monthly"] = "daily",
            page: int = 1,
            page_size: int = 1000,
            **kwargs
        ) -> dict:
        return super().get_request_body(
            variables={
                "queryRequest": {
                    "mallSequence": str(mall_seq),
                    "dateType": date_type.capitalize(),
                    "startDate": str(date_pairs[0]),
                    "endDate": str(date_pairs[1]),
                    **({"sortBy": "PaymentAmount"} if self.sales_type != "store" else dict()),
                    **({"pageable": {"page":int(page), "size":int(page_size)}} if self.sales_type != "store" else dict()),
                }
            })

    def set_request_body(self, *args, **kwargs):
        from linkmerce.utils.graphql import GraphQLOperation, GraphQLSelection
        super().set_request_body(
            GraphQLOperation(
                operation = f"get{self.sales_type.capitalize()}Sale",
                variables = {"queryRequest": dict()},
                types = {"queryRequest": "StoreTrafficRequest"},
                selection = GraphQLSelection(
                    name = f"{self.sales_type}Sales",
                    variables = ["queryRequest"],
                    fields = self.fields,
                )
            ).generate_data(query_options = dict(
                selection = dict(variables=dict(linebreak=False), fields=dict(linebreak=True)),
                suffix = '\n')))

    @PartnerCenter.cookies_required
    def set_request_headers(self, **kwargs):
        contents = dict(type="text", charset="UTF-8")
        referer = self.origin + "/iframe/brand-analytics/store/productSales"
        super().set_request_headers(contents=contents, origin=self.origin, referer=referer, **kwargs)


class StoreSales(_SalesCollector):
    sales_type = "store"

    @property
    def fields(self) -> list[dict]:
        return [
            {"period": ["date"]},
            {"sales": [
                "paymentAmount", "paymentCount", "paymentUserCount", "refundAmount",
                "paymentAmountPerPaying", "paymentAmountPerUser", "refundRate"]}
        ]


class CategorySales(_SalesCollector):
    sales_type = "category"

    @property
    def fields(self) -> list[dict]:
        return [
            {"product": [{"category": ["identifier", "fullName"]}]},
            {"sales": ["paymentAmount", "paymentCount", "purchaseConversionRate", "paymentAmountPerPaying"]},
            {"visit": ["click"]},
            {"measuredThrough": ["type"]},
        ]


class ProductSales(_SalesCollector):
    sales_type = "product"

    @property
    def fields(self) -> list[dict]:
        return [
            {"product": ["identifier", "name", {"category": ["identifier", "name", "fullName"]}]},
            {"sales": ["paymentAmount", "paymentCount", "purchaseConversionRate"]},
            {"visit": ["click"]},
            {"rest": [{"comparePreWeek": ["isNewlyAdded"]}]},
        ]
