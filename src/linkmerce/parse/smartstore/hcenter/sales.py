from __future__ import annotations

from linkmerce.parse import QueryParser
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common import JsonObject
    import datetime as dt


class _SalesParser(QueryParser):
    sales_type: Literal["store","category","product"]

    def check_errors(func):
        @functools.wraps(func)
        def wrapper(self: _SalesParser, response: JsonObject, *args, **kwargs):
            if isinstance(response, dict):
                if "error" not in response:
                    return func(self, response, *args, **kwargs)
                else:
                    self.raise_request_error(response)
            else:
                self.raise_parse_error("The HTTP response is not of dictionary type.")
        return wrapper

    def raise_request_error(self, response: dict):
        from linkmerce.utils.map import hier_get
        msg = hier_get(response, ["error","error"]) or "null"
        if msg == "Unauthorized":
            from linkmerce.common import UnauthorizedError
            raise UnauthorizedError("Unauthorized request")
        else:
            from linkmerce.common import RequestError
            raise RequestError(f"An error occurred during the request: {msg}")

    @check_errors
    def parse(
            self,
            response: JsonObject,
            mall_seq: int | str | None = None,
            start_date: dt.date | None = None,
            end_date: dt.date | None = None,
            date_type: Literal["daily","weekly","monthly"] = "daily",
            **kwargs
        ) -> list[dict]:
        data = response["data"][f"{self.sales_type}Sales"]
        mall_seq = string if (string := str(mall_seq)).isdigit() else "NULL"
        date_part = self.build_date_part(start_date, end_date, date_type)
        kwargs = dict(mall_seq=mall_seq, date_part=date_part, start_date=start_date, end_date=end_date)
        return self.select(data, self.make_query(**kwargs)) if data else list()

    def build_date_part(self, start_date = None, end_date = None, date_type = "daily") -> str:
        if date_type == "daily":
            return super().build_date_part(("paymentDate", end_date), safe=True)
        else:
            return super().build_date_part(("startDate", start_date), ("endDate", end_date), safe=True)


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


class AggregatedSales(ProductSales):
    object_type = "products"

    def make_query(self, mall_seq: str, date_part: str, **kwargs) -> str:
        query = """
        WITH product_sales AS (
            SELECT
                TRY_CAST(product.identifier AS INT64) AS mallPid,
                {{ mall_seq }} AS mallSeq,
                TRY_CAST(product.category.identifier AS INT32) AS categoryId3,
                visit.click AS clickCount,
                sales.paymentCount AS paymentCount,
                sales.paymentAmount AS paymentAmount,
                {{ date_part }}
            FROM {{ table }}
        )

        SELECT
            mallPid,
            MAX(mallSeq) AS mallSeq,
            MAX(categoryId3) AS categoryId3,
            SUM(clickCount) AS clickCount,
            SUM(paymentCount) AS paymentCount,
            SUM(paymentAmount) AS paymentAmount,
            {{ date_group }}
        FROM product_sales
        WHERE mallPid IS NOT NULL
        GROUP BY mallPid, {{ date_group }};
        """
        date_group = "paymentDate" if "paymentDate" in date_part else "startDate, endDate"
        return self.render_query(query, mall_seq=mall_seq, date_part=date_part, date_group=date_group)


class ProductList(ProductSales):
    object_type = "products"

    def make_query(self, mall_seq: str, start_date: dt.date | None = None, **kwargs) -> str:
        query = """
        SELECT * EXCLUDE (seq)
        FROM (
            SELECT
                TRY_CAST(product.identifier AS INT64) AS mallPid,
                {{ mall_seq }} AS mallSeq,
                NULL AS categoryId,
                TRY_CAST(product.category.identifier AS INT32) AS categoryId3,
                product.name AS productName,
                NULL AS salesPrice,
                {{ start_date }} AS registerDate,
                {{ today }} AS updateDate,
                ROW_NUMBER() OVER (PARTITION BY mallPid) AS seq
            FROM {{ table }}
        ) WHERE (mallPid IS NOT NULL) AND (seq = 1);
        """
        start_date = f"DATE '{start_date}'" if start_date is not None else "NULL"
        today = self.curret_date()
        return self.render_query(query, mall_seq=mall_seq, start_date=start_date, today=today)
