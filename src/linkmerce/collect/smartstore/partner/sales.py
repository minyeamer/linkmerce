from __future__ import annotations
from linkmerce.collect import Collector, JsonObject, Parser, POST
from linkmerce.parse.smartstore.partner.sales import ProductSalesList

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Dict, List, Literal
    from requests import Session
    from aiohttp import ClientSession
    import datetime as dt


class StoreSales(Collector):
    method = POST
    url = "https://hcenter.shopping.naver.com/brand/content"

    def __init__(self,
            session: Session | ClientSession | None = None,
            headers: Dict = dict(),
            salesType: Literal["store","category","product"] = "store"
        ):
        self.set_session(session)
        self.set_request_headers(**headers)
        self.set_request_body(salesType)

    @Collector.with_session
    def collect(self,
            startDate: dt.date | str,
            endDate: dt.date | str,
            mallSeq: int | str,
            dateType: Literal["daily","weekly","monthly"] = "daily",
            page: int = 1,
            pageSize: int = 1000
        ) -> JsonObject:
        message = self.build_request(startDate, endDate, mallSeq, dateType, page, pageSize)
        response = self.request_json(**message)
        return self.parse(response)

    @Collector.with_client_session
    async def collect_async(self,
            startDate: dt.date | str,
            endDate: dt.date | str,
            mallSeq: int | str,
            dateType: Literal["daily","weekly","monthly"] = "daily",
            page: int = 1,
            pageSize: int = 1000,
        ) -> JsonObject:
        message = self.build_request(startDate, endDate, mallSeq, dateType, page, pageSize)
        response = await self.request_async_json(**message)
        return self.parse(response)

    def build_request(self,
            startDate: dt.date | str,
            endDate: dt.date | str,
            mallSeq: int | str,
            dateType: Literal["daily","weekly","monthly"] = "daily",
            page: int = 1,
            pageSize: int = 1000
        ) -> Dict:
        body = self.get_request_body(startDate, endDate, mallSeq, dateType, page, pageSize)
        headers = self.get_request_headers()
        return dict(method=self.method, url=self.url, json=body, headers=headers)

    def get_request_body(self,
            startDate: dt.date | str,
            endDate: dt.date | str,
            mallSeq: int | str,
            dateType: Literal["daily","weekly","monthly"] = "daily",
            page: int = 1,
            pageSize: int = 1000
        ) -> Dict:
        return dict(self.__body, variables={
                "queryRequest": {
                    "mallSequence": str(mallSeq),
                    "dateType": dateType.capitalize(),
                    "startDate": str(startDate),
                    "endDate": str(endDate),
                    **({"sortBy": "PaymentAmount"} if self.salesType != "store" else dict()),
                    **({"pageable": {"page":int(page), "size":int(pageSize)}} if self.salesType != "store" else dict()),
                }
            })

    def set_request_body(self, salesType: Literal["store","category","product"] = "store"):
        from linkmerce.utils.graphql import GraphQLOperation, GraphQLSelection
        self.__body = GraphQLOperation(
            operation=f"get{salesType.capitalize()}Sale",
            variables={"queryRequest": dict()},
            types={"queryRequest": "StoreTrafficRequest"},
            selection=GraphQLSelection(
                name=f"{salesType}Sales",
                variables=["queryRequest"],
                fields=getattr(self, f"_{salesType}_fields"),
            )
        ).generate_data(query_options=dict(
            selection=dict(variables=dict(linebreak=False), fields=dict(linebreak=True)),
            suffix='\n'))
        self.salesType = salesType

    @Collector.cookies_required
    def set_request_headers(self, **kwargs) -> Dict[str,str]:
        contents = dict(type="text", charset="UTF-8")
        referer = "https://hcenter.shopping.naver.com/iframe/brand-analytics/store/productSales"
        origin = "https://hcenter.shopping.naver.com"
        super().set_request_headers(contents=contents, origin=origin, referer=referer, **kwargs)

    @property
    def _store_fields(self) -> List[Dict]:
        return [
            {"period": ["date"]},
            {"sales": [
                "paymentAmount", "paymentCount", "paymentUserCount", "refundAmount",
                "paymentAmountPerPaying", "paymentAmountPerUser", "refundRate"]}
        ]

    @property
    def _category_fields(self) -> List[Dict]:
        return [
            {"product": [{"category": ["identifier", "fullName"]}]},
            {"sales": ["paymentAmount", "paymentCount", "purchaseConversionRate", "paymentAmountPerPaying"]},
            {"visit": ["click"]},
            {"measuredThrough": ["type"]},
        ]

    @property
    def _product_fields(self) -> List[Dict]:
        return [
            {"product": ["identifier", "name", {"category": ["identifier", "name", "fullName"]}]},
            {"sales": ["paymentAmount", "paymentCount", "purchaseConversionRate"]},
            {"visit": ["click"]},
            {"rest": [{"comparePreWeek": ["isNewlyAdded"]}]},
        ]


class ProductSales(StoreSales):
    def __init__(self, session: Session | ClientSession | None = None, headers: Dict = dict()):
        super().__init__(session, headers, salesType="product")

    @Collector.with_session
    def collect(self,
            startDate: dt.date | str,
            endDate: dt.date | str,
            mallSeq: int | str,
            dateType: Literal["daily","weekly","monthly"] = "daily",
            page: int = 1,
            pageSize: int = 1000,
            parser: Literal["sales"] | Parser | None = "sales"
        ) -> JsonObject:
        message = self.build_request(startDate, endDate, mallSeq, dateType, page, pageSize)
        response = self.request_json(**message)
        return self.parse(response, parser)

    @Collector.with_client_session
    async def collect_async(self,
            startDate: dt.date | str,
            endDate: dt.date | str,
            mallSeq: int | str,
            dateType: Literal["daily","weekly","monthly"] = "daily",
            page: int = 1,
            pageSize: int = 1000,
            parser: Literal["sales"] | Parser | None = "sales"
        ) -> JsonObject:
        message = self.build_request(startDate, endDate, mallSeq, dateType, page, pageSize)
        response = await self.request_async_json(**message)
        return self.parse(response, parser)

    def select_parser(self, parser: Literal["sales"] | Parser | None = "sales") -> Parser:
        if parser == "sales":
            return ProductSalesList
        else:
            return parser
