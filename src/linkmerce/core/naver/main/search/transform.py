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
######################## Mobile Tab Search ########################
###################################################################

class CafeList(HtmlTransformer):
    selector = "div.view_wrap"

    def transform(self, obj: BeautifulSoup, query: str, **kwargs) -> list[dict]:
        results = list()
        for rank, div in enumerate(obj.select(self.selector), start=1):
            results.append(self.parse(div, query, rank))
        return results

    def parse(self, div: Tag, query: str, rank: int) -> dict:
        title_link = div.select_one("a.title_link")
        url = title_link.attrs.get("href")
        return dict(
            query = query,
            rank = rank,
            **dict(zip(["cafe_url","article_id"], self.get_ids_from_url(url))),
            ad_id = self.get_ad_id_from_attr(title_link.attrs.get("onclick")),
            cafe_name = self.get_text(div, "div.user_info > a.name"),
            title = title_link.get_text(strip=True),
            description = self.get_text(div, "div.dsc_area"),
            url = url,
            image_url = (tag.attrs.get("src") if (tag := div.select_one("a.thumb_link > img")) else None),
            next_url = (self.make_next_url(url, query) if url else None),
            replies = '\n'.join(self.parse_replies(div)) or None,
            write_date = self.get_text(div, "div.user_info > span.sub"),
        )

    def get_text(self, div: Tag, selector: str) -> str:
        return tag.get_text(strip=True) if (tag := div.select_one(selector)) else None

    def get_ids_from_url(self, url: str) -> tuple[str,str]:
        from linkmerce.utils.regex import regexp_groups
        return regexp_groups(r"/([^/]+)/(\d+)$", url.split('?')[0], indices=[0,1]) if url else (None, None)

    def get_ad_id_from_attr(self, onclick: str) -> str:
        from linkmerce.utils.regex import regexp_extract
        return regexp_extract(r"(nad-a\d+-\d+-\d+)", onclick) if onclick else None

    def make_next_url(self, url: str, query: str) -> str:
        cafe_url, article_id = self.get_ids_from_url(url)
        m_ = "m." if url.startswith("https://m.") else str()
        if (cafe_url is None) or (article_id is None):
            return None

        from urllib.parse import quote
        from uuid import uuid4
        params = (p+'&') if (p := (url.split('?')[1] if '?' in url else None)) else str()
        params = f"{params}useCafeId=false&tc=naver_search&or={m_}search.naver.com&query={quote(query)}&buid={uuid4()}"
        return f"https://article.cafe.naver.com/gw/v4/cafes/{cafe_url}/articles/{article_id}?{params}"

    def parse_replies(self, div: Tag, prefix: str = "[RE] ") -> list[str]:
        replies = list()
        for box in div.select("div.flick_bx"):
            ico_reply = box.select_one("i.ico_reply")
            if ico_reply:
                ico_reply.decompose()
                replies.append(prefix + box.get_text(strip=True))
        return replies


class CafeTab(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, query: str, **kwargs):
        articles = CafeList().transform(obj, query)
        if articles:
            self.insert_into_table(articles)


class CafeArticleJson(JsonTransformer):
    path = ["result"]

    def parse(self, obj: JsonObject, **kwargs) -> JsonObject:
        result = obj["result"]
        result["article"]["commenterCount"] = len({item["writer"]["memberKey"] for item in result["comments"]["items"]})
        result["tags"] = ", ".join(result["tags"]) or None
        result["content"] = self.parse_content(result["article"]["contentHtml"])
        result["article"]["contentHtml"] = None
        return result

    def parse_content(self, content: str) -> dict:
        from bs4 import BeautifulSoup
        import re

        source = BeautifulSoup(content.replace('\\\\', '\\'), "html.parser")
        for div in source.select("div.se-oglink"):
            div.decompose()
        return dict(
            contentLength = len(re.sub(r"\s+", ' ', source.get_text()).strip()),
            imageCount = len(source.select("img.se-image-resource")),
        )


class CafeArticle(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, **kwargs):
        if obj is not None:
            self.insert_into_table([CafeArticleJson().transform(obj)])


###################################################################
#################### Shopping Page (deprecated) ###################
###################################################################

# class PagedProducts(JsonTransformer):
#     path = ["data", 0, "products"]


# class ShoppingProduct(DuckDBTransformer):
#     queries = ["create", "select", "insert"]

#     def transform(self, obj: JsonObject, query: str, **kwargs):
#         products = [dict(cardType=product["cardType"]) for product in (PagedProducts().transform(obj) or list())]
#         if products:
#             self.insert_into_table(products, params=dict(keyword=query))


# class ShoppingPage(ShoppingProduct):
#     queries = ["create", "select", "insert"]
