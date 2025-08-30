from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.transform import JsonObject
    import datetime as dt


class SalesList(JsonTransformer):
    dtype = dict

    def is_valid_response(self, obj: dict) -> bool:
        if "error" in obj:
            from linkmerce.utils.map import hier_get
            msg = hier_get(obj, ["error","error"]) or "null"
            if msg == "Unauthorized":
                from linkmerce.common.exceptions import UnauthorizedError
                raise UnauthorizedError("Unauthorized request")
            super().raise_request_error(f"An error occurred during the request: {msg}")
        return True


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
        sales = SalesList(path=["data",f"{self.sales_type}Sales"]).transform(obj)
        if sales:
            params = dict(mall_seq=mall_seq, end_date=end_date)
            if self.start_date:
                params.update(start_date=start_date)
            self.insert_into_table(sales, params=params)


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

    def set_tables(self, tables: dict | None = None):
        base = dict(sales="naver_brand_sales", product="naver_brand_product")
        super().set_tables(dict(base, **(tables or dict())))

    def create_table(self, **kwargs):
        super().create_table(key="create_sales", table=":sales:")
        super().create_table(key="create_product", table=":product:")

    def insert_into_table(self, obj: list[dict], params: dict = dict(), **kwargs):
        sales_params, product_params = self.split_params(**params)
        super().insert_into_table(obj, key="insert_sales", table=":sales:", values=":select_sales:", params=sales_params)
        super().insert_into_table(obj, key="upsert_product", table=":product:", values=":select_product:", params=product_params)

    def split_params(self, mall_seq: int | str, start_date: dt.date, end_date: dt.date, **kwargs) -> tuple[dict,dict]:
        return dict(mall_seq=mall_seq, end_date=end_date), dict(mall_seq=mall_seq, start_date=start_date)
