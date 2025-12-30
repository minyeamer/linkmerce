from __future__ import annotations
from linkmerce.common.extract import Extractor

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from bs4 import BeautifulSoup
    from linkmerce.common.extract import JsonObject


###################################################################
############################## Search #############################
###################################################################

class Search(Extractor):
    method = "GET"
    url = "https://{m}search.naver.com/search.naver"
    state = dict(oquery=None, tqi=None, ackey=None)

    @property
    def default_options(self) -> dict:
        return dict(RequestEach = dict(request_delay=1.01))

    @Extractor.with_session
    def extract(
            self,
            query: str | Iterable[str],
            mobile: bool = True,
            parse_html: bool = True,
        ) -> JsonObject | BeautifulSoup | str:
        return (self.request_each(self.search)
                .partial(mobile=mobile, parse_html=parse_html)
                .expand(query=query)
                .run())

    def search(self, mobile: bool = True, parse_html: bool = True, **kwargs) -> BeautifulSoup | str:
        kwargs["url"] = self.url.format(m=("m." if mobile else str()))
        response = self.request_text(mobile=mobile, **kwargs)
        self.save_search_query(response, kwargs.get("query"))
        if parse_html:
            from bs4 import BeautifulSoup
            return BeautifulSoup(response, "html.parser")
        else:
            return response

    def save_search_query(self, response: str, query: str):
        from linkmerce.utils.regex import regexp_extract
        self.state["oquery"] = query
        self.state["tqi"] = regexp_extract(r"tqi=([^&\"]+)", response)
        self.state["ackey"] = regexp_extract(r"ackey=([^&\"]+)", response)

    def build_request_params(self, query: str, mobile: bool = True, **kwargs) -> dict:
        params = {
            "sm": f"{'mtp_hty' if mobile else 'tab_hty'}.top",
            "where": ('m' if mobile else "nexearch"),
            "query": query,
            **{key: value for key, value in self.state.items() if value}
        }
        if "ackey" not in params:
            self.state["ackey"] = self.ackey
        return params

    @property
    def ackey(self) -> str:
        import random

        def _base36_encode(number):
            chars = "0123456789abcdefghijklmnopqrstuvwxyz"
            result = str()
            while number > 0:
                number, i = divmod(number, 36)
                result = chars[i] + result
            return result or '0'

        n = random.random()
        s = _base36_encode(int(n * 36**10))
        return s[2:10]


###################################################################
############################ Search Tab ###########################
###################################################################

class SearchTab(Extractor):
    method = "GET"
    url = "https://{m}search.naver.com/search.naver"

    @property
    def default_options(self) -> dict:
        return dict(RequestEach = dict(request_delay=1.01))

    @Extractor.with_session
    def extract(
            self,
            query: str | Iterable[str],
            tab_type: Literal["image","blog","cafe","kin","influencer","clip","video","news","surf","shortents"],
            mobile: bool = True,
            **kwargs
        ) -> JsonObject | BeautifulSoup:
        url = self.url.format(m=("m." if mobile else str()))
        tab_type = self.tab_type[tab_type].format(m=("m_" if mobile else str()))
        return (self.request_each(self.request_html)
                .partial(url=url, tab_type=tab_type, mobile=mobile, **kwargs)
                .expand(query=query)
                .run())

    def build_request_params(self, query: str, tab_type: str, mobile: bool = True, **kwargs) -> dict:
        return {"ssc": tab_type, "sm": ("mtb_jum" if mobile else "tab_jum"), "query": query}

    def set_request_headers(self, **kwargs):
        kwargs.update(authority=self.url, encoding="gzip, deflate", metadata="navigate", https=True)
        return super().set_request_headers(**kwargs)

    @property
    def tab_type(self) -> dict[str,str]:
        return {
            "image": "tab.{m}image.all", # "이미지"
            "blog": "tab.{m}blog.all", # "블로그"
            "cafe": "tab.{m}cafe.all", # "카페"
            "kin": "tab.{m}kin.all", # "지식iN"
            "influencer": "tab.{m}influencer.chl", # "인플루언서"
            "clip": "tab.{m}clip.all", # "클립"
            "video": "tab.{m}video.all", # "동영상"
            "news": "tab.{m}news.all", # "뉴스"
            "surf": "tab.{m}surf.tab1", # "서치피드"
            "shortents": "tab.{m}shortents.all" # "숏텐츠"
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
