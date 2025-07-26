from __future__ import annotations
from linkmerce.collect import Collector

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Callable, Dict, List, Literal
    from linkmerce.types import JsonObject
    from requests import Session
    from aiohttp import ClientSession
    import datetime as dt


class SalesCollector(Collector):
    method = "POST"
    url = "https://hcenter.shopping.naver.com/brand/content"
    date_format = "%Y-%m-%d"

    def __init__(
            self,
            session: Session | ClientSession | None = None,
            headers: Dict = dict(),
            sales_type: Literal["store","category","product"] = "store",
        ):
        self.set_session(session)
        self.set_request_headers(**headers)
        self.set_request_body(sales_type)

    @Collector.with_session
    def collect(
            self,
            mall_seq: int | str,
            start_date: dt.date | str,
            end_date: dt.date | str,
            date_type: Literal["daily","weekly","monthly"] = "daily",
            page: int = 1,
            page_size: int = 1000,
            parser: Literal["Sales"] | Callable | None = "Sales",
        ) -> JsonObject:
        message = self.build_request(mall_seq, start_date, end_date, date_type, page, page_size)
        response = self.request_json(**message)
        return self.parse(response, parser, mall_seq, start_date, end_date, date_type)

    @Collector.with_client_session
    async def collect_async(
            self,
            mall_seq: int | str,
            start_date: dt.date | str,
            end_date: dt.date | str,
            date_type: Literal["daily","weekly","monthly"] = "daily",
            page: int = 1,
            page_size: int = 1000,
            parser: Literal["Sales"] | Callable | None = None,
        ) -> JsonObject:
        message = self.build_request(mall_seq, start_date, end_date, date_type, page, page_size)
        response = await self.request_async_json(**message)
        return self.parse(response, parser, mall_seq, start_date, end_date, date_type)

    def parse(
            self,
            response: JsonObject,
            parser: Literal["Sales"] | Callable | None = None,
            mall_seq: int | str | None = None,
            start_date: dt.date | str | None = None,
            end_date: dt.date | str | None = None,
            date_type: Literal["daily","weekly","monthly"] = "daily",
        ) -> JsonObject:
        if isinstance(parser, str) and (parser == "Sales"):
            parser = self.import_parser(self.sales_type.capitalize() + parser)
        return super().parse(response, parser, mall_seq, start_date, end_date, date_type)

    def build_request(
            self,
            mall_seq: int | str,
            start_date: dt.date | str,
            end_date: dt.date | str,
            date_type: Literal["daily","weekly","monthly"] = "daily",
            page: int = 1,
            page_size: int = 1000,
        ) -> Dict:
        body = self.get_request_body(mall_seq, start_date, end_date, date_type, page, page_size)
        headers = self.get_request_headers()
        return dict(method=self.method, url=self.url, json=body, headers=headers)

    def get_request_body(
            self,
            mall_seq: int | str,
            start_date: dt.date | str,
            end_date: dt.date | str,
            date_type: Literal["daily","weekly","monthly"] = "daily",
            page: int = 1,
            page_size: int = 1000,
        ) -> Dict:
        return dict(self.__body, variables={
                "queryRequest": {
                    "mallSequence": str(mall_seq),
                    "dateType": date_type.capitalize(),
                    "startDate": str(start_date),
                    "endDate": str(end_date),
                    **({"sortBy": "PaymentAmount"} if self.sales_type != "store" else dict()),
                    **({"pageable": {"page":int(page), "size":int(page_size)}} if self.sales_type != "store" else dict()),
                }
            })

    def set_request_body(self, sales_type: Literal["store","category","product"] = "store"):
        from linkmerce.utils.graphql import GraphQLOperation, GraphQLSelection
        self.__body = GraphQLOperation(
            operation=f"get{sales_type.capitalize()}Sale",
            variables={"queryRequest": dict()},
            types={"queryRequest": "StoreTrafficRequest"},
            selection=GraphQLSelection(
                name=f"{sales_type}Sales",
                variables=["queryRequest"],
                fields=getattr(self, f"{sales_type}_fields"),
            )
        ).generate_data(query_options=dict(
            selection=dict(variables=dict(linebreak=False), fields=dict(linebreak=True)),
            suffix='\n'))
        self.sales_type = sales_type

    @Collector.cookies_required
    def set_request_headers(self, **kwargs) -> Dict[str,str]:
        contents = dict(type="text", charset="UTF-8")
        referer = "https://hcenter.shopping.naver.com/iframe/brand-analytics/store/productSales"
        origin = "https://hcenter.shopping.naver.com"
        super().set_request_headers(contents=contents, origin=origin, referer=referer, **kwargs)

    @property
    def store_fields(self) -> List[Dict]:
        return [
            {"period": ["date"]},
            {"sales": [
                "paymentAmount", "paymentCount", "paymentUserCount", "refundAmount",
                "paymentAmountPerPaying", "paymentAmountPerUser", "refundRate"]}
        ]

    @property
    def category_fields(self) -> List[Dict]:
        return [
            {"product": [{"category": ["identifier", "fullName"]}]},
            {"sales": ["paymentAmount", "paymentCount", "purchaseConversionRate", "paymentAmountPerPaying"]},
            {"visit": ["click"]},
            {"measuredThrough": ["type"]},
        ]

    @property
    def product_fields(self) -> List[Dict]:
        return [
            {"product": ["identifier", "name", {"category": ["identifier", "name", "fullName"]}]},
            {"sales": ["paymentAmount", "paymentCount", "purchaseConversionRate"]},
            {"visit": ["click"]},
            {"rest": [{"comparePreWeek": ["isNewlyAdded"]}]},
        ]


class StoreSales(SalesCollector):
    def __init__(self, session: Session | ClientSession | None = None, headers: Dict = dict()):
        super().__init__(session, headers, sales_type="store")


class CategorySales(SalesCollector):
    def __init__(self, session: Session | ClientSession | None = None, headers: Dict = dict()):
        super().__init__(session, headers, sales_type="category")


class ProductSales(SalesCollector):
    def __init__(self, session: Session | ClientSession | None = None, headers: Dict = dict()):
        super().__init__(session, headers, sales_type="product")
