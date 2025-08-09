from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import JsonObject
    from duckdb import DuckDBPyRelation
    import datetime as dt


class _SalesTransformer(DuckDBTransformer):
    sales_type: Literal["store","category","product"]
    queries: list[str] = ["create", "select", "insert"]
    start_date: bool = False

    def transform(
            self,
            obj: JsonObject,
            mall_seq: int | str | None = None,
            start_date: dt.date | None = None,
            end_date: dt.date | None = None,
            **kwargs
        ):
        if isinstance(obj, dict):
            if "error" not in obj:
                params = dict(mall_seq=mall_seq, end_date=end_date)
                if self.start_date:
                    sales = obj["data"][f"{self.sales_type}Sales"]
                    params.update(start_date=start_date)
                return self.insert_into_table(sales, params=params) if sales else None
            else:
                self.raise_request_error(obj)
        else:
            self.raise_parse_error()

    def raise_request_error(self, obj: JsonObject):
        from linkmerce.utils.map import hier_get
        msg = hier_get(obj, ["error","error"]) or "null"
        if msg == "Unauthorized":
            from linkmerce.common.exceptions import UnauthorizedError
            raise UnauthorizedError("Unauthorized request")
        else:
            super().raise_request_error(f"An error occurred during the request: {msg}")


class StoreSales(_SalesTransformer):
    sales_type = "store"
    queries = ["create", "select", "insert"]


class CategorySales(_SalesTransformer):
    sales_type = "category"
    queries = ["create", "select", "insert"]


class ProductSales(_SalesTransformer):
    sales_type = "product"
    queries = ["create", "select", "insert"]


class AggregatedSales(ProductSales):
    object_type = "products"
    queries = ["create_sales", "select_sales", "insert_sales", "create_product", "select_product", "upsert_product"]
    start_date = True

    def create_table(
            self,
            sales_table: str = ":default:",
            product_table: str = "product",
            **kwargs
        ) -> tuple[DuckDBPyRelation,DuckDBPyRelation]:
        sales = super().create_table(key="create_sales", table=sales_table)
        product = super().create_table(key="create_product", table=product_table)
        return sales, product

    def insert_into_table(
            self,
            obj: list,
            sales_table: str = ":default:",
            product_table: str = "product",
            params: dict = dict(),
            **kwargs
        ) -> tuple[DuckDBPyRelation,DuckDBPyRelation]:
        def split_params(mall_seq: int | str, start_date: dt.date, end_date: dt.date, **kwargs) -> tuple[dict,dict]:
            return dict(mall_seq=mall_seq, end_date=end_date), dict(mall_seq=mall_seq, start_date=start_date)
        sales_params, product_params = split_params(**params)
        sales = super().insert_into_table(obj, key="insert_sales", table=sales_table, values=":select_sales:", params=sales_params)
        product = super().insert_into_table(obj, key="upsert_product", table=product_table, values=":select_product:", params=product_params)
        return sales, product
