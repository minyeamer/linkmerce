from __future__ import annotations
from linkmerce.collect import Collector

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Callable, Dict, List, Literal
    from linkmerce.types import JsonObject
    from requests import Session
    from aiohttp import ClientSession
    import datetime as dt


class StoreSales(Collector):
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
            start_date: dt.date | str,
            end_date: dt.date | str,
            mall_seq: int | str,
            date_type: Literal["daily","weekly","monthly"] = "daily",
            page: int = 1,
            page_size: int = 1000,
            parser: Literal["sales"] | Callable | None = "sales",
        ) -> JsonObject:
        message = self.build_request(start_date, end_date, mall_seq, date_type, page, page_size)
        response = self.request_json(**message)
        return self.parse(response, parser, start_date, end_date, mall_seq, date_type)

    @Collector.with_client_session
    async def collect_async(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str,
            mall_seq: int | str,
            date_type: Literal["daily","weekly","monthly"] = "daily",
            page: int = 1,
            page_size: int = 1000,
            parser: Literal["sales"] | Callable | None = None,
        ) -> JsonObject:
        message = self.build_request(start_date, end_date, mall_seq, date_type, page, page_size)
        response = await self.request_async_json(**message)
        return self.parse(response, parser, start_date, end_date, mall_seq, date_type)

    def parse(
            self,
            response: JsonObject,
            parser: Literal["sales"] | Callable | None = None,
            start_date: dt.date | str | None = None,
            end_date: dt.date | str | None = None,
            mall_seq: int | str | None = None,
            date_type: Literal["daily","weekly","monthly"] = "daily",
        ) -> JsonObject:
        if isinstance(parser, str) and (parser == "sales"):
            parser = self.sales_type.capitalize() + parser.capitalize()
        context = self.build_parse_context(start_date, end_date, mall_seq, date_type)
        return super().parse(response, parser, context=context)

    def build_request(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str,
            mall_seq: int | str,
            date_type: Literal["daily","weekly","monthly"] = "daily",
            page: int = 1,
            page_size: int = 1000,
        ) -> Dict:
        body = self.get_request_body(start_date, end_date, mall_seq, date_type, page, page_size)
        headers = self.get_request_headers()
        return dict(method=self.method, url=self.url, json=body, headers=headers)

    def get_request_body(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str,
            mall_seq: int | str,
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
                fields=getattr(self, f"_{sales_type}_fields"),
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

    def build_parse_context(
            self,
            start_date: dt.date | str | None = None,
            end_date: dt.date | str | None = None,
            mall_seq: int | str | None = None,
            date_type: Literal["daily","weekly","monthly"] = "daily",
        ) -> Dict:
            import datetime as dt
            def parse_date(date: dt.date | str) -> dt.date:
                return date if isinstance(date, dt.date) else dt.datetime.strptime(str(date), self.date_format).date()
            if date_type == "daily":
                return dict(mall_seq=int(mall_seq), period=dict(paymentDate=parse_date(end_date)))
            else:
                return dict(mall_seq=int(mall_seq), period=dict(startDate=parse_date(start_date), endDate=parse_date(end_date)))

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


class CategorySales(StoreSales):
    def __init__(self, session: Session | ClientSession | None = None, headers: Dict = dict()):
        super().__init__(session, headers, sales_type="category")


class ProductSales(StoreSales):
    def __init__(self, session: Session | ClientSession | None = None, headers: Dict = dict()):
        super().__init__(session, headers, sales_type="product")
