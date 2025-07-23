from __future__ import annotations
from linkmerce.parse import Map, Array

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Dict, List


class ProductSales(Map):
    def parse(self, object_, **kwargs):
        return object_


class ProductSalesList(Array):
    dtype = ProductSales

    def parse(self, object_, **kwargs) -> List[Dict]:
        return object_["data"]["productSales"]
