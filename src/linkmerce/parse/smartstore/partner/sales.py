from __future__ import annotations

from linkmerce.parse import QueryParser
import functools

from typing import Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import List, Literal
    from linkmerce.types import JsonObject
    import datetime as dt


class _SalesParser(QueryParser):
    sales_type: Literal["store","category","product"]

    def check_errors(func):
        @functools.wraps(func)
        def wrapper(self: _SalesParser, response: JsonObject, *args, **kwargs):
            if isinstance(response, Dict):
                if "error" not in response:
                    return func(self, response, *args, **kwargs)
                else:
                    self.raise_request_error(response)
            else:
                self.raise_parse_error("The HTTP response is not of dictionary type.")
        return wrapper

    def raise_request_error(self, response: Dict):
        from linkmerce.utils.map import hier_get
        msg = hier_get(response, ["error","error"]) or "null"
        if msg == "Unauthorized":
            from linkmerce.exceptions import UnauthorizedError
            raise UnauthorizedError("Unauthorized request")
        else:
            from linkmerce.exceptions import RequestError
            raise RequestError(f"An error occurred during the request: {msg}")

    @check_errors
    def parse(
            self,
            response: JsonObject, 
            mall_seq: int | str | None = None,
            start_date: dt.date | str | None = None,
            end_date: dt.date | str | None = None,
            date_type: Literal["daily","weekly","monthly"] = "daily",
            **kwargs
        ) -> List[Dict]:
        def build_query_params(params: Dict = dict()) -> Dict:
            params["mall_seq"] = s if (s := str(mall_seq)).isdigit() else "NULL"
            if date_type == "daily":
                date_fields = [("paymentDate", end_date)]
            else:
                date_fields = [("startDate", start_date), ("endDate", end_date)]
            params["date_part"] = self.build_date_part(*date_fields, safe=True)
            return params
        data = response["data"][f"{self.sales_type}Sales"]
        return self.select(data, **build_query_params(), format="json") if data else list()


class StoreSales(_SalesParser):
    sales_type = "store"

    def make_query(self, mall_seq: str, date_part: str, **kwargs) -> str:
        query = """
        SELECT
            {{ mall_seq }} AS mallSeq,
            sales.paymentCount AS paymentCount,
            sales.paymentAmount AS paymentAmount,
            sales.refundAmount AS refundAmount,
            {{ date_part }}
        FROM {{ table }}
        """
        return self.render_query(query, mall_seq=mall_seq, date_part=date_part)


class CategorySales(_SalesParser):
    sales_type = "category"

    def make_query(self, mall_seq: str, date_part: str, **kwargs) -> str:
        query = """
        SELECT
            {{ mall_seq }} AS mallSeq,
            TRY_CAST(product.category.identifier AS INT32) AS categoryId3,
            product.category.fullName AS wholeCategoryName,
            visit.click AS clickCount,
            sales.paymentCount AS paymentCount,
            sales.paymentAmount AS paymentAmount,
            {{ date_part }}
        FROM {{ table }};
        """
        return self.render_query(query, mall_seq=mall_seq, date_part=date_part)


class ProductSales(_SalesParser):
    sales_type = "product"

    def make_query(self, mall_seq: str, date_part: str, **kwargs) -> str:
        query = """
        SELECT
            {{ mall_seq }} AS mallSeq,
            TRY_CAST(product.identifier AS INT64) AS mallPid,
            product.name AS productName,
            TRY_CAST(product.category.identifier AS INT32) AS categoryId3,
            product.category.name AS categoryName,
            product.category.fullName AS wholeCategoryName,
            visit.click AS clickCount,
            sales.paymentCount AS paymentCount,
            sales.paymentAmount AS paymentAmount,
            {{ date_part }}
        FROM {{ table }};
        """
        return self.render_query(query, mall_seq=mall_seq, date_part=date_part)
