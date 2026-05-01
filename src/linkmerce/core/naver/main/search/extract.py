from __future__ import annotations
from linkmerce.common.extract import Extractor

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from bs4 import BeautifulSoup


###################################################################
############################## Search #############################
###################################################################

class Search(Extractor):
    """네이버 통합검색 결과를 스크래핑하여 HTML 소스코드를 추출하는 클래스.

    - **PC**: https://search.naver.com/search.naver
    - **Mobile**: https://m.search.naver.com/search.naver

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        검색어별 요청 간 대기 시간(초). 기본값은 `1.01`
    tqdm_options: dict | None
        반복 요청 작업 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    url = "https://{m}search.naver.com/search.naver"
    state = {"oquery": None, "tqi": None, "ackey": None}
    default_options = {"RequestEach": {"request_delay": 1.01}}

    @Extractor.with_session
    def extract(
            self,
            query: str | Iterable[str],
            mobile: bool = True,
            parse_html: bool = True,
        ) -> str | list[str] | BeautifulSoup | list[BeautifulSoup]:
        """네이버 통합검색 결과 페이지로부터 HTML 소스코드를 추출한다.

        Parameters
        ----------
        query: str | Iterable[str]
            검색어. 문자열 또는 문자열의 배열을 입력한다.
        mobile: bool
            - `True`: 모바일 검색 (기본값)
            - `False`: PC 검색
        parse_html: bool
            - `True`: HTML 소스코드를 `BeautifulSoup` 객체로 파싱한다. (기본값)
            - `False`: HTML 소스코드를 텍스트로 반환한다.

        Returns
        -------
        str | list[str] | BeautifulSoup | list[BeautifulSoup]
            네이버 통합검색 결과 페이지 소스코드.
            `query` 타입과 `parse_html` 값에 따라 반환 타입이 다르다. (JS 삼항 연산 표현)
                - `query`가 `str` 타입일 때 -> `parse_html ? BeautifulSoup : str`
                - `query`가 `Iterable[str]` 타입일 때 -> `parse_html ? list[BeautifulSoup] : list[str]`
        """
        return (self.request_each(self.search)
                .partial(mobile=mobile, parse_html=parse_html)
                .expand(query=query)
                .run())

    def search(self, mobile: bool = True, parse_html: bool = True, **kwargs) -> str | BeautifulSoup:
        """네이버 통합검색 요청을 실행하고 HTML 텍스트를 파싱한다."""
        kwargs["url"] = self.url.format(m=("m." if mobile else str()))
        response = self.request_text(mobile=mobile, **kwargs)
        self.save_search_query(response, kwargs.get("query"))
        if parse_html:
            from bs4 import BeautifulSoup
            return BeautifulSoup(response, "html.parser")
        else:
            return response

    def save_search_query(self, response: str, query: str):
        """검색 결과인 HTML 텍스트에서 `oquery`, `tqi`, `ackey` 상태를 추출한다."""
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
        """랜덤 `ackey` 값을 생성한다."""
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
    """네이버 탭별 검색 결과를 스크래핑하여 HTML 소스코드를 추출하는 클래스.

    - **PC**: https://search.naver.com/search.naver
    - **Mobile**: https://m.search.naver.com/search.naver

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        검색어별 요청 간 대기 시간(초). 기본값은 `1.01`
    tqdm_options: dict | None
        반복 요청 작업 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    url = "https://{m}search.naver.com/search.naver"
    default_options = {"RequestEach": {"request_delay": 1.01}}

    @Extractor.with_session
    def extract(
            self,
            query: str | Iterable[str],
            tab_type: Literal["image", "blog", "cafe", "kin", "influencer", "clip", "video", "news", "shortents"],
            mobile: bool = True,
            **kwargs
        ) -> BeautifulSoup | list[BeautifulSoup]:
        """네이버 탭별 검색 결과 페이지로부터 HTML 소스코드를 추출한다.

        Parameters
        ----------
        query: str | Iterable[str]
            검색어. 문자열 또는 문자열의 배열을 입력한다.
        tab_type: str
            탭 구분
                - `"image"`: 이미지
                - `"blog"`: 블로그
                - `"cafe"`: 카페
                - `"kin"`: 지식iN
                - `"influencer"`: 인플루언서
                - `"clip"`: 클립
                - `"video"`: 동영상
                - `"news"`: 뉴스
                - `"shortents"`: 숏텐츠
        mobile: bool
            - `True`: 모바일 검색 (기본값)
            - `False`: PC 검색

        Returns
        -------
        BeautifulSoup | list[BeautifulSoup]
            네이버 탭별 검색 결과 페이지 소스코드. `query` 타입에 따라 반환 타입이 다르다.
                - `query`가 `str` 타입일 때 -> `BeautifulSoup`
                - `query`가 `Iterable[str]` 타입일 때 -> `list[BeautifulSoup]`
        """
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
    def tab_type(self) -> dict[str, str]:
        """탭별 `ssc` 파라미터 매핑을 반환한다."""
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
    """네이버 카페 게시글을 조회하는 클래스.

    - **API**: https://article.cafe.naver.com/gw/v4/cafes/{cafe_url}/articles/{article_id}
    - **Referer**: https://cafe.naver.com/{cafe_url}/{article_id}

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        카페 URL별 요청 간 대기 시간(초). 기본값은 `1.01`
    tqdm_options: dict | None
        반복 요청 작업 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    url = "https://article.cafe.naver.com/gw/v4/cafes/{cafe_url}/articles/{article_id}"
    referer = "https://{m_}cafe.naver.com/{cafe_url}/{article_id}"
    default_options = {"RequestEach": {"request_delay": 1.01}}

    @Extractor.with_session
    def extract(
            self,
            url: str | Iterable[str],
            domain: Literal["article", "cafe", "m"] = "article",
            **kwargs
        ) -> dict | list[dict]:
        """카페 게시글의 정보를 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        url: str | Iterable[str]
            카페 URL. 문자열 또는 문자열의 배열을 입력한다. 다음 3가지 도메인에 대한 URL을 허용한다.
                - `"article"`: https://article.cafe.naver.com/gw/v4/cafes/{cafe_url}/articles/{article_id}
                - `"cafe"`: https://cafe.naver.com/{cafe_url}/{article_id}
                - `"m"`: https://m.cafe.naver.com/{cafe_url}/{article_id}
        domain: str
            카페 URL의 도메인. `url` 파라미터로 전달되는 URL 값은 전부 동일한 도메인이어야 한다.

        Returns
        -------
        dict | list[dict]
            카페 게시글 정보. `url` 타입에 따라 반환 타입이 다르다.
                - `url`이 `str` 타입일 때 -> `dict`
                - `url`이 `Iterable[str]` 타입일 때 -> `list[dict]`
        """
        return (self.request_each(self.request_json_safe)
                .partial(domain=domain)
                .expand(url=url)
                .run())

    def build_request_message(
            self,
            url: str | Iterable[str],
            domain: Literal["article", "cafe", "m"] = "article",
            **kwargs
        ) -> dict:
        if domain != "article":
            url = self.make_article_url(url)
        return super().build_request_message(url=url, **kwargs)

    def build_request_headers(
            self,
            url: str | Iterable[str],
            domain: Literal["article", "cafe", "m"] = "article",
            **kwargs
        ) -> dict[str, str]:
        referer = self.make_referral_url(url) if domain == "article" else url
        return dict(self.get_request_headers(), referer=referer)

    def set_request_headers(self, domain: Literal["cafe", "m"] = "m", **kwargs):
        origin = "https://cafe.naver.com" if domain == "cafe" else "https://m.cafe.naver.com"
        kwargs.update(authority=self.url, origin=origin, **{"x-cafe-product": "mweb"})
        super().set_request_headers(**kwargs)

    def get_ids_from_url(self, url: str) -> tuple[str, str]:
        """카페 게시글 URL에서 `cafe_url`과 `article_id`를 추출한다."""
        from linkmerce.utils.regex import regexp_groups
        return regexp_groups(r"/([^/]+)/(\d+)$", url.split('?')[0], indices=[0, 1])

    def make_article_url(self, url: str) -> str:
        """카페 게시글 URL을 API URL로 변환한다."""
        cafe_url, article_id = self.get_ids_from_url(url)
        if (cafe_url is not None) and (article_id is not None):
            params = self.make_article_params(url)
            return f"https://article.cafe.naver.com/gw/v4/cafes/{cafe_url}/articles/{article_id}?{params}"
        else:
            raise ValueError(f"URL is invalid: '{url}'")

    def make_article_params(self, url: str) -> str:
        """카페 게시글 URL에서 쿼리 파라미터를 추출하고 `useCafeId`, `buid` 파라미터를 추가한다."""
        from urllib.parse import urlencode
        from uuid import uuid4
        param_string = url.split('?')[1] if '?' in url else str()
        params = dict([kv.split('=', maxsplit=1) for kv in param_string.split('&')]) if param_string else dict()
        return urlencode(dict(params, **{"useCafeId": "false", "buid": uuid4()}))

    def make_referral_url(self, url: str) -> str:
        """카페 게시글 URL을 `referer` 헤더용 URL로 변환한다."""
        cafe_url, article_id = self.get_ids_from_url(url)
        if (cafe_url is not None) and (article_id is not None):
            m_ = "m." if "m.search" in url else str()
            params = ('?'+url.split('?')[1]) if '?' in url else str()
            return self.referer.format(m_=m_, cafe_url=cafe_url, article_id=article_id) + params
