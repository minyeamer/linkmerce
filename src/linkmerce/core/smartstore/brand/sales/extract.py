from __future__ import annotations
from linkmerce.core.smartstore.brand import PartnerCenter

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class _SalesExtractor(PartnerCenter):
    method = "POST"
    path = "/brand/content"
    date_format = "%Y-%m-%d"
    sales_type: Literal["store","category","product"]
    fields: list[dict]

    @property
    def default_options(self) -> dict:
        return dict(RequestEach = dict(request_delay=1, max_concurrent=3))

    @PartnerCenter.with_session
    def extract(
            self,
            mall_seq: int | str | Iterable[int | str],
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            date_type: Literal["daily","weekly","monthly"] = "daily",
            page: int | Iterable[int] = 1,
            page_size: int = 1000,
            **kwargs
        ) -> JsonObject:
        context = self.generate_date_context(start_date, end_date, freq=date_type[0].upper(), format=self.date_format)
        return (self.request_each(self.request_json_safe, context=context)
                .partial(date_type=date_type, page_size=page_size)
                .expand(mall_seq=mall_seq, page=page)
                .run())

    @PartnerCenter.async_with_session
    async def extract_async(
            self,
            mall_seq: int | str | Iterable[int | str],
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            date_type: Literal["daily","weekly","monthly"] = "daily",
            page: int = 1,
            page_size: int = 1000,
            **kwargs
        ) -> JsonObject:
        context = self.generate_date_context(start_date, end_date, freq=date_type[0].upper(), format=self.date_format)
        return await (self.request_each(self.request_async_json_safe, context=context)
                .partial(date_type=date_type, page_size=page_size)
                .expand(mall_seq=mall_seq, page=page)
                .run_async())

    def build_request_json(
            self,
            mall_seq: int | str,
            start_date: dt.date,
            end_date: dt.date,
            date_type: Literal["daily","weekly","monthly"] = "daily",
            page: int = 1,
            page_size: int = 1000,
            **kwargs
        ) -> dict:
        return dict(self.get_request_body(),
            variables={
                "queryRequest": {
                    "mallSequence": str(mall_seq),
                    "dateType": date_type.capitalize(),
                    "startDate": str(start_date),
                    "endDate": str(end_date),
                    **({"sortBy": "PaymentAmount"} if self.sales_type != "store" else dict()),
                    **({"pageable": {"page":int(page), "size":int(page_size)}} if self.sales_type != "store" else dict()),
                }
            })

    def set_request_body(self):
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
            ).generate_body(query_options = dict(
                selection = dict(variables=dict(linebreak=False), fields=dict(linebreak=True)),
                suffix = '\n')))

    @PartnerCenter.cookies_required
    def set_request_headers(self, **kwargs):
        contents = dict(type="text", charset="UTF-8")
        referer = self.origin + "/iframe/brand-analytics/store/productSales"
        super().set_request_headers(contents=contents, origin=self.origin, referer=referer, **kwargs)


class StoreSales(_SalesExtractor):
    sales_type = "store"

    @property
    def fields(self) -> list[dict]:
        return [
            {"period": ["date"]},
            {"sales": [
                "paymentAmount", "paymentCount", "paymentUserCount", "refundAmount",
                "paymentAmountPerPaying", "paymentAmountPerUser", "refundRate"]}
        ]


class CategorySales(_SalesExtractor):
    sales_type = "category"

    @property
    def fields(self) -> list[dict]:
        return [
            {"product": [{"category": ["identifier", "fullName"]}]},
            {"sales": ["paymentAmount", "paymentCount", "purchaseConversionRate", "paymentAmountPerPaying"]},
            {"visit": ["click"]},
            {"measuredThrough": ["type"]},
        ]


class ProductSales(_SalesExtractor):
    sales_type = "product"

    @property
    def fields(self) -> list[dict]:
        return [
            {"product": ["identifier", "name", {"category": ["identifier", "name", "fullName"]}]},
            {"sales": ["paymentAmount", "paymentCount", "purchaseConversionRate"]},
            {"visit": ["click"]},
            {"rest": [{"comparePreWeek": ["isNewlyAdded"]}]},
        ]


class AggregatedSales(ProductSales):
    sales_type = "product"
