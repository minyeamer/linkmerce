from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, HtmlTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject
    from bs4 import BeautifulSoup, Tag


def parse_int(text: str) -> int | None:
    if text is None:
        return None
    import re
    groups = re.findall(r"\d{1,3}(?:,\d{3})+", text)
    return int(str(groups[0]).replace(',', '')) if groups else None


###################################################################
########################## Mobile Search ##########################
###################################################################

class MobileProducts(HtmlTransformer):
    selector = 'div[class*="hoppingProductList"] > ul > li:all'

    def transform(self, obj: BeautifulSoup, start: int = 1, **kwargs) -> list[dict]:
        results = list()
        product_list = obj.select_one('div[class*="hoppingProductList"]')
        for page, ul in enumerate(product_list.select("ul"), start=1):
            products = ul.select("li")
            for rank, li in enumerate(products):
                results.append(self.parse(li, page, rank, start))
            start += len(products)
        return results

    def parse(self, li: Tag, page: int, rank: int, start: int) -> dict:
        return dict(
            id = x if (x := (self.select(li, "a > :attr(aria-labelledby):") or str()).rsplit('_', 1)[-1]).isdigit() else None,
            page = page,
            rank = (rank + start),
            ad_id = self.parse_ad_id(li),
            **self.check_ad_badge(li),
            product_name = self.select(li, 'strong[class^="productTitle"] > :text():'),
            mall_name = self.select(li, 'span[class^="shoppingProductMallInformation"] > :text():'),
            sales_price = parse_int(self.select(li, 'span[class^="shoppingProductPrice"] > :text():')),
            review_score = self.select(li, 'span[class^="shoppingProductStats-mobile-module__value"] > :text():'),
            review_count = parse_int(self.select(li, 'span[class^="shoppingProductStats-mobile-module__count"] > :text():')),
            purchase_count = parse_int(self.select(li, 'span[class*="shoppingProductStats-mobile-module__purchase"] > :text():')),
            keep_count = parse_int(self.select(li, 'span[class*="shoppingProductStats-mobile-module__keep"] > :text():')),
        )

    def check_ad_badge(self, li: Tag) -> dict[str,bool]:
        if li.select_one('a[class^="adBadge"]'):
            ad_badge = True
            ad_plus = any([svg for svg in (self.select(li, 'a[class^="adBadge"] > svg > :attr(class):') or list())
                        if "_advertisement_plus_" in svg])
            return dict(ad_badge=ad_badge, ad_plus=ad_plus)
        else:
            return dict(ad_badge=False, ad_plus=False)

    def parse_ad_id(self, li: Tag) -> str | None:
        content = li.attrs.get("data-slog-content", str())
        if content and ("nad-" in content):
            import re
            return re.findall(r"nad-[^\s]+", content)[0]


class MobileSearch(DuckDBTransformer):
    ...


###################################################################
########################## Shopping Page ##########################
###################################################################

class PagedProducts(JsonTransformer):
    path = ["data", 0, "products"]


class ShoppingProduct(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, query: str, **kwargs):
        products = [dict(cardType=product["cardType"]) for product in (PagedProducts().transform(obj) or list())]
        if products:
            params = dict(keyword=query)
            self.insert_into_table(products, params=params)


class ShoppingPage(ShoppingProduct):
    queries = ["create", "select", "insert"]
