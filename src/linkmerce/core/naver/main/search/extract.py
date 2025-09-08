from __future__ import annotations
from linkmerce.common.extract import Extractor

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable
    from bs4 import BeautifulSoup
    from linkmerce.common.extract import JsonObject


###################################################################
########################## Mobile Search ##########################
###################################################################

class MobileSearch(Extractor):
    method = "GET"
    url = "https://m.search.naver.com/search.naver"

    @property
    def default_options(self) -> dict:
        return dict(RequestEach=dict(delay=1.01))

    @Extractor.with_session
    def extract(self, query: str | Iterable[str]) -> JsonObject | BeautifulSoup:
        return (self.request_each(self.request_html)
                .expand(query=query)
                .run())

    def build_request_params(self, query: str, **kwargs) -> dict:
        return {"sm": "mtp_hty.top", "where": 'm', "query": query}


###################################################################
######################### Shopping Product ########################
###################################################################

class ShoppingProduct(Extractor):
    method = "GET"
    url = "https://ns-portal.shopping.naver.com/api/v1/shopping-paged-product"

    @property
    def default_options(self) -> dict:
        return dict(
            RequestLoop = dict(count=5, ignored_errors=ConnectionError),
            RequestEachLoop = dict(delay=1.01, limit=3))

    @Extractor.with_session
    def extract(self, query: str | Iterable[str], mobile: bool = True, **kwargs) -> JsonObject:
        return (self.request_each_loop(self.request_json_safe)
                .partial(mobile=mobile)
                .expand(query=query)
                .loop(lambda x: True)
                .run())

    @Extractor.async_with_session
    async def extract_async(self, query: str | Iterable[str], mobile: bool = True, **kwargs) -> JsonObject:
        return await (self.request_each(self.request_async_json_safe)
                .partial(mobile=mobile)
                .expand(query=query)
                .run_async())

    def build_request_params(self, query: str, mobile: bool = True, **kwargs) -> dict:
        if mobile:
            params = {"ssc": "tab.m.all", "sm": "mtb_hty.top", "source": "shp_tli"}
        else:
            params = {"ssc": "tab.nx.all", "sm": "top_hty", "source": "shp_gui"}
        params.update({"adDepth": 'H', "adPosition": 'T', "query": query})
        return params

    def build_request_headers(self, mobile: bool = True, **kwargs: str) -> dict[str,str]:
        ns = {"x-ns-device-type": ("mobile" if mobile else "pc"), "x-ns-page-id": self.generate_page_id()}
        return dict(self.get_request_headers(), **ns)

    def set_request_headers(self, mobile: bool = True, **kwargs: str):
        origin = "https://m.search.naver.com" if mobile else "https://search.naver.com"
        super().set_request_headers(contents="json", origin=origin, referer=origin, metadata="cors", **kwargs)

    def generate_page_id(self) -> str:
        import random
        import string
        ascii_chars = string.digits + string.ascii_letters

        a = ''.join([random.choice(ascii_chars) for _ in range(8)])
        b = ''.join([random.choice(ascii_chars) for _ in range(6)])
        c = ''.join([random.choice(ascii_chars) for _ in range(2)])
        d = ''.join([random.choice(string.digits) for _ in range(6)])
        return f"j6b{a}ss{b}ssssss{c}-{d}"


class ShoppingPage(ShoppingProduct):
    ...
