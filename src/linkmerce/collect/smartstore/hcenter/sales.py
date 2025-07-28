from __future__ import annotations
from linkmerce.collect.smartstore.hcenter import PartnerCenter

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Dict, List, Literal
    from linkmerce.types import JsonObject
    import datetime as dt


class _SalesCollector(PartnerCenter):
    method = "POST"
    path = "/brand/content"
    date_format = "%Y-%m-%d"
    sales_type: Literal["store","category","product"]
    fields: List[Dict]

    @PartnerCenter.with_session
    def collect(
            self,
            mall_seq: int | str,
            start_date: dt.date | str,
            end_date: dt.date | str,
            date_type: Literal["daily","weekly","monthly"] = "daily",
            page: int = 1,
            page_size: int = 1000,
        ) -> JsonObject:
        kwargs = self.build_kwargs(mall_seq, start_date, end_date, date_type, page, page_size)
        message = self.build_request(json=kwargs)
        response = self.request_json(**message)
        return self.parse(response, **kwargs)

    @PartnerCenter.async_with_session
    async def collect_async(
            self,
            mall_seq: int | str,
            start_date: dt.date | str,
            end_date: dt.date | str,
            date_type: Literal["daily","weekly","monthly"] = "daily",
            page: int = 1,
            page_size: int = 1000,
        ) -> JsonObject:
        kwargs = self.build_kwargs(mall_seq, start_date, end_date, date_type, page, page_size)
        message = self.build_request(json=kwargs)
        response = await self.request_async_json(**message)
        return self.parse(response, **kwargs)

    def build_kwargs(self, mall_seq, start_date, end_date, date_type, page, page_size) -> Dict:
        return dict(mall_seq=mall_seq, start_date=start_date, end_date=end_date, date_type=date_type, page=page, page_size=page_size)

    def get_request_body(
            self,
            mall_seq: int | str,
            start_date: dt.date | str,
            end_date: dt.date | str,
            date_type: Literal["daily","weekly","monthly"] = "daily",
            page: int = 1,
            page_size: int = 1000,
            **kwargs
        ) -> Dict:
        return super().get_request_body(
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

    def set_request_body(self, **kwargs):
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
    def fields(self) -> List[Dict]:
        return [
            {"period": ["date"]},
            {"sales": [
                "paymentAmount", "paymentCount", "paymentUserCount", "refundAmount",
                "paymentAmountPerPaying", "paymentAmountPerUser", "refundRate"]}
        ]


class CategorySales(_SalesCollector):
    sales_type = "category"

    @property
    def fields(self) -> List[Dict]:
        return [
            {"product": [{"category": ["identifier", "fullName"]}]},
            {"sales": ["paymentAmount", "paymentCount", "purchaseConversionRate", "paymentAmountPerPaying"]},
            {"visit": ["click"]},
            {"measuredThrough": ["type"]},
        ]


class ProductSales(_SalesCollector):
    sales_type = "product"

    @property
    def fields(self) -> List[Dict]:
        return [
            {"product": ["identifier", "name", {"category": ["identifier", "name", "fullName"]}]},
            {"sales": ["paymentAmount", "paymentCount", "purchaseConversionRate"]},
            {"visit": ["click"]},
            {"rest": [{"comparePreWeek": ["isNewlyAdded"]}]},
        ]
