from __future__ import annotations
from linkmerce.parse import Map, Array

from typing import Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import List, Literal
    from linkmerce.types import JsonObject
    import datetime as dt


def parse_product(product: Dict) -> Dict:
    from linkmerce.utils.cast import safe_int
    return dict(
        mallPid = safe_int(product.get("identifier")),
        productName = product.get("name"),
        **parse_category(product.get("category", dict()), sales_type="product"),
    )


def parse_category(category: Dict, sales_type: Literal["category","product"]) -> Dict:
    from linkmerce.utils.cast import safe_int
    return dict(
        categoryId = safe_int(category.get("identifier")),
        **(dict(categoryName = category.get("name")) if sales_type == "product" else dict()),
        wholeCategoryName = category.get("fullName"),
    )


def parse_visit(visit: Dict) -> Dict:
    return dict(
        clickCount = visit.get("click"),
    )


def parse_sales(sales: Dict, sales_type: Literal["store","category","product"]) -> Dict:
    return dict(
        paymentCount = sales.get("paymentCount"),
        paymentAmount = sales.get("paymentAmount"),
        **(dict(refundAmount = sales.get("refundAmount")) if sales_type == "store" else dict()),
    )


class StoreSalesItem(Map):
    notnull = []

    @Map.ensure_dict(timing="both")
    def parse(
            self,
            object_: Dict,
            mall_seq: int | None = None,
            period: Dict[str,dt.date] = dict(),
            **context
        ) -> Dict:

        return self.map(
            mallSeq = mall_seq,
            **parse_sales(object_.get("sales", dict()), sales_type="store"),
            **period,
        )


class CategorySalesItem(Map):
    notnull = ["categoryId"]

    @Map.ensure_dict(timing="both")
    def parse(
            self,
            object_: Dict,
            mall_seq: int | None = None,
            period: Dict[str,dt.date] = dict(),
            **context
        ) -> Dict:
        from linkmerce.utils.map import hier_get

        return self.map(
            mallSeq = mall_seq,
            **parse_category(hier_get(object_, ["product","category"], default=dict()), sales_type="category"),
            **parse_visit(object_.get("visit", dict())),
            **parse_sales(object_.get("sales", dict()), sales_type="category"),
            **period,
        )


class ProductSalesItem(Map):
    notnull = ["mallPid"]

    @Map.ensure_dict(timing="both")
    def parse(
            self,
            object_: Dict,
            mall_seq: int | None = None,
            period: Dict[str,dt.date] = dict(),
            **context
        ) -> Dict:

        return self.map(
            mallSeq = mall_seq,
            **parse_product(object_.get("product", dict())),
            **parse_visit(object_.get("visit", dict())),
            **parse_sales(object_.get("sales", dict()), sales_type="product"),
            **period,
        )


class StoreSales(Array):
    dtype = StoreSalesItem
    sales_type = "store"

    @Array.ensure_sequence(timing="after")
    def parse(self, object_: JsonObject, **kwargs) -> List[Dict]:
        if isinstance(object_, Dict):
            if "error" not in object_:
                return object_["data"][f"{self.sales_type}Sales"]
            else:
                self._raise_request_error(object_)
        else:
            raise ValueError("Unable to parse the object.")

    def _raise_request_error(object_: Dict):
        from linkmerce.utils.map import hier_get
        msg = hier_get(object_, ["error","error"]) or "null"
        if msg == "Unauthorized":
            from linkmerce.exceptions import UnauthorizedError
            raise UnauthorizedError("Unauthorized request")
        else:
            raise ValueError(f"An error occurred during the request: {msg}")


class CategorySales(StoreSales):
    dtype = CategorySalesItem
    sales_type = "category"


class ProductSales(StoreSales):
    dtype = ProductSalesItem
    sales_type = "product"
