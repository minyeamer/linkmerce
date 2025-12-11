from __future__ import annotations
from linkmerce.common.extract import Extractor

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
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
        return dict(RequestEach = dict(request_delay=1.01))

    @Extractor.with_session
    def extract(self, query: str | Iterable[str]) -> JsonObject | BeautifulSoup:
        return (self.request_each(self.request_html)
                .expand(query=query)
                .run())

    def build_request_params(self, query: str, **kwargs) -> dict:
        return {"sm": "mtp_hty.top", "where": 'm', "query": query}


###################################################################
######################## Mobile Tab Search ########################
###################################################################

class MobileTabSearch(Extractor):
    method = "GET"
    url = "https://m.search.naver.com/search.naver"

    @property
    def default_options(self) -> dict:
        return dict(RequestEach = dict(request_delay=1.01))

    @Extractor.with_session
    def extract(
            self,
            query: str | Iterable[str],
            tab_type: Literal["image","blog","cafe","kin","influencer","clip","video","news","surf","shortents"],
            **kwargs
        ) -> JsonObject | BeautifulSoup:
        tab_type = tab_type if tab_type in self.tab_type.values() else self.tab_type[tab_type]
        return (self.request_each(self.request_html)
                .partial(tab_type=tab_type, **kwargs)
                .expand(query=query)
                .run())

    def build_request_params(self, query: str, tab_type: str, **kwargs) -> dict:
        return {"ssc": tab_type, "sm": "mtb_jum", "query": query}

    def set_request_headers(self, **kwargs):
        kwargs.update(authority=self.url, encoding="gzip, deflate", metadata="navigate", https=True)
        return super().set_request_headers(**kwargs)

    @property
    def tab_type(self) -> dict[str,str]:
        return {
            "image": "tab.m_image.all", # "이미지"
            "blog": "tab.m_blog.all", # "블로그"
            "cafe": "tab.m_cafe.all", # "카페"
            "kin": "tab.m_kin.all", # "지식iN"
            "influencer": "tab.m_influencer.chl", # "인플루언서"
            "clip": "tab.m_clip.all", # "클립"
            "video": "tab.m_video.all", # "동영상"
            "news": "tab.m_news.all", # "뉴스"
            "surf": "tab.m_surf.tab1", # "서치피드"
            "shortents": "tab.m_shortents.all" # "숏텐츠"
        }


class CafeArticle(Extractor):
    method = "GET"
    url = "https://article.cafe.naver.com/gw/v4/cafes/{cafe_url}/articles/{article_id}"
    referer = "https://{m_}cafe.naver.com/{cafe_url}/{article_id}"

    @property
    def default_options(self) -> dict:
        return dict(RequestEach = dict(request_delay=1.01))

    @Extractor.with_session
    def extract(
            self,
            url: str | Iterable[str],
            domain: Literal["article","cafe","m"] = "article",
            **kwargs
        ) -> JsonObject | BeautifulSoup:
        return (self.request_each(self.request_json_safe)
                .partial(domain=domain)
                .expand(url=url)
                .run())

    def build_request_message(
            self,
            url: str | Iterable[str],
            domain: Literal["article","cafe","m"] = "article",
            **kwargs
        ) -> dict:
        if domain != "article":
            from linkmerce.utils.regex import regexp_groups
            cafe_url, article_id = regexp_groups(r"/([^/]+)/(\d+)$", url.split('?')[0], indices=[0,1])
            params = ('?'+p) if (p := (url.split('?')[1] if '?' in url else None)) else str()
            url = self.url.format(cafe_url=cafe_url, article_id=article_id) + params
        return super().build_request_message(url=url, **kwargs)

    def build_request_headers(
            self,
            url: str | Iterable[str],
            domain: Literal["article","cafe","m"] = "article",
            **kwargs
        ) -> dict[str,str]:
        headers = self.get_request_headers()
        if domain == "article":
            from linkmerce.utils.regex import regexp_groups
            cafe_url, article_id = regexp_groups(r"/([^/]+)/articles/(\d+)$", url.split('?')[0], indices=[0,1])
            m_ = "m." if "m.search" in url else str()
            params = ('?'+p) if (p := (url.split('?')[1].split('&')[0] if '?' in url else None)) else str()
            headers["referer"] = self.referer.format(m_=m_, cafe_url=cafe_url, article_id=article_id) + params
        else:
            headers["referer"] = url
        return headers

    def set_request_headers(self, domain: Literal["cafe","m"] = "m", **kwargs):
        origin = "https://cafe.naver.com" if domain == "cafe" else "https://m.cafe.naver.com"
        kwargs.update(authority=self.url, origin=origin, **{"x-cafe-product": "mweb"})
        return super().set_request_headers(**kwargs)


###################################################################
################## Shopping Product (deprecated) ##################
###################################################################

# class ShoppingProduct(Extractor):
#     method = "GET"
#     url = "https://ns-portal.shopping.naver.com/api/v1/shopping-paged-product"

#     @property
#     def default_options(self) -> dict:
#         return dict(
#             RequestLoop = dict(max_retries=5, ignored_errors=ConnectionError),
#             RequestEachLoop = dict(request_delay=1.01, max_concurrent=3),
#         )

#     @Extractor.with_session
#     def extract(self, query: str | Iterable[str], mobile: bool = True, **kwargs) -> JsonObject:
#         return (self.request_each_loop(self.request_json_safe)
#                 .partial(mobile=mobile)
#                 .expand(query=query)
#                 .loop(lambda x: True)
#                 .run())

#     @Extractor.async_with_session
#     async def extract_async(self, query: str | Iterable[str], mobile: bool = True, **kwargs) -> JsonObject:
#         return await (self.request_each_loop(self.request_async_json_safe)
#                 .partial(mobile=mobile)
#                 .expand(query=query)
#                 .run_async())

#     def build_request_params(self, query: str, mobile: bool = True, **kwargs) -> dict:
#         if mobile:
#             params = {"ssc": "tab.m.all", "sm": "mtb_hty.top", "source": "shp_tli"}
#         else:
#             params = {"ssc": "tab.nx.all", "sm": "top_hty", "source": "shp_gui"}
#         params.update({"adDepth": 'H', "adPosition": 'T', "query": query})
#         return params

#     def build_request_headers(self, mobile: bool = True, **kwargs: str) -> dict[str,str]:
#         ns = {"x-ns-device-type": ("mobile" if mobile else "pc"), "x-ns-page-id": self.generate_page_id()}
#         return dict(self.get_request_headers(), **ns)

#     def set_request_headers(self, mobile: bool = True, **kwargs: str):
#         origin = "https://m.search.naver.com" if mobile else "https://search.naver.com"
#         super().set_request_headers(contents="json", origin=origin, referer=origin, **kwargs)

#     def generate_page_id(self) -> str:
#         import random
#         import string
#         ascii_chars = string.digits + string.ascii_letters

#         a = ''.join([random.choice(ascii_chars) for _ in range(8)])
#         b = ''.join([random.choice(ascii_chars) for _ in range(6)])
#         c = ''.join([random.choice(ascii_chars) for _ in range(2)])
#         d = ''.join([random.choice(string.digits) for _ in range(6)])
#         return f"j6b{a}ss{b}ssssss{c}-{d}"


# class ShoppingPage(ShoppingProduct):
#     ...
