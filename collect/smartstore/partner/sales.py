from __future__ import annotations
from collect import Collector, JsonObject, POST

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Dict, List, Literal
    import datetime as dt


class _SalesViewer(Collector):
    method = POST
    url = "https://hcenter.shopping.naver.com/brand/content"

    def init_constant(self, **kwargs):
        self.set_request_headers(**kwargs)
        self.fields = dict()
        self.fields["store"] = self._get_store_fields()
        self.fields["category"] = self._get_category_fields()
        self.fields["product"] = self._get_product_fields()

    def set_request_headers(self, **kwargs) -> Dict[str,str]:
        referer = "https://hcenter.shopping.naver.com/iframe/brand-analytics/store/productSales"
        origin = "https://hcenter.shopping.naver.com"
        super().set_request_headers(contents=dict(type="text", charset="UTF-8"), origin=origin, referer=referer, **kwargs)

    @Collector.with_session
    def collect(self,
            startDate: dt.date | str,
            endDate: dt.date | str,
            mallSeq: int | str,
            dateType: Literal["daily","weekly","monthly"],
            salesType: Literal["store","category","product"],
            page: int = 1,
            pageSize: int = 1000,
        **kwargs) -> JsonObject:
        message = self.get_request_message(startDate, endDate, mallSeq, dateType, salesType, page, pageSize)
        response = self.request_json(**message)
        return self.parse(response)

    @Collector.with_client_session
    async def collect_async(self,
            startDate: dt.date | str,
            endDate: dt.date | str,
            mallSeq: int | str,
            dateType: Literal["daily","weekly","monthly"],
            salesType: Literal["store","category","product"],
            page: int = 1,
            pageSize: int = 1000,
        **kwargs) -> JsonObject:
        message = self.get_request_message(startDate, endDate, mallSeq, dateType, salesType, page, pageSize)
        response = await self.request_async_json(**message)
        return self.parse(response)

    def parse(self, response: Dict) -> JsonObject:
        return response

    def get_request_message(self, startDate, endDate, mallSeq, dateType: str, salesType: str, page=1, pageSize=1000) -> Dict:
        body = self._get_request_body(startDate, endDate, mallSeq, dateType, salesType, page, pageSize)
        headers = self.get_request_headers()
        return dict(method=self.method, url=self.url, json=body, headers=headers)

    def _get_request_body(self, startDate, endDate, mallSeq, dateType: str, salesType: str, page=1, pageSize=1000):
        from utils.graphql import GraphQLOperation, GraphQLSelection
        return GraphQLOperation(
            operation=f"get{salesType.capitalize()}Sale",
            variables={
                "queryRequest": {
                    "mallSequence": str(mallSeq),
                    "dateType": dateType.capitalize(),
                    "startDate": str(startDate),
                    "endDate": str(endDate),
                    **({"sortBy": "PaymentAmount"} if salesType != "store" else dict()),
                    **({"pageable": {"page":int(page), "size":int(pageSize)}} if salesType != "store" else dict()),
                }
            },
            types={"queryRequest":"StoreTrafficRequest"},
            selection=GraphQLSelection(
                name=f"{salesType}Sales",
                variables=["queryRequest"],
                fields=self.fields[salesType],
            )
        ).generate_data(query_options=dict(
            selection=dict(variables=dict(linebreak=False), fields=dict(linebreak=True)),
            suffix='\n'))

    def _get_store_fields(self) -> List[Dict]:
        return [
            {"period": ["date"]},
            {"sales": [
                "paymentAmount", "paymentCount", "paymentUserCount", "refundAmount",
                "paymentAmountPerPaying", "paymentAmountPerUser", "refundRate"]}
        ]

    def _get_category_fields(self) -> List[Dict]:
        return [
            {"product": [{"category": ["identifier", "fullName"]}]},
            {"sales": ["paymentAmount", "paymentCount", "purchaseConversionRate", "paymentAmountPerPaying"]},
            {"visit": ["click"]},
            {"measuredThrough": ["type"]},
        ]

    def _get_product_fields(self) -> List[Dict]:
        return [
            {"product": ["identifier", "name", {"category": ["identifier", "name", "fullName"]}]},
            {"sales": ["paymentAmount", "paymentCount", "purchaseConversionRate"]},
            {"visit": ["click"]},
            {"rest": [{"comparePreWeek": ["isNewlyAdded"]}]},
        ]


class ProductSales(_SalesViewer):
    def init_constant(self, **kwargs):
        self.set_request_headers(**kwargs)
        self.fields = dict(product=self._get_product_fields())

    @Collector.with_session
    def collect(self,
            startDate: dt.date | str,
            endDate: dt.date | str,
            mallSeq: int | str,
            page: int = 1,
            pageSize: int = 1000,
        **kwargs) -> JsonObject:
        message = self.get_request_message(startDate, endDate, mallSeq, "daily", "product", page, pageSize)
        response = self.request_json(**message)
        return self.parse(response)

    @Collector.with_client_session
    async def collect_async(self,
            startDate: dt.date | str,
            endDate: dt.date | str,
            mallSeq: int | str,
            page: int = 1,
            pageSize: int = 1000,
        **kwargs) -> JsonObject:
        message = self.get_request_message(startDate, endDate, mallSeq, "daily", "product", page, pageSize)
        response = await self.request_async_json(**message)
        return self.parse(response)
