from __future__ import annotations
from linkmerce.collect.naver.api import NaverOpenAPI

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Dict, Literal
    from linkmerce.types import JsonObject


class _SearchCollector(NaverOpenAPI):
    """
    Search various types of content using the Naver Open API.

    This collector sends a GET request to the Naver Open API endpoint for 
    the specified content type (blog, news, book, cafearticle, kin, image, shop, etc.) 
    and returns a list of search results as dictionaries.

    For detailed API documentation, see:
    - Blog: https://developers.naver.com/docs/serviceapi/search/blog/blog.md
    - News: https://developers.naver.com/docs/serviceapi/search/news/news.md
    - Book: https://developers.naver.com/docs/serviceapi/search/book/book.md
    - Cafearticle: https://developers.naver.com/docs/serviceapi/search/cafearticle/cafearticle.md
    - Kin: https://developers.naver.com/docs/serviceapi/search/kin/kin.md
    - Image: https://developers.naver.com/docs/serviceapi/search/image/image.md
    - Shop: https://developers.naver.com/docs/serviceapi/search/shopping/shopping.md
    """

    method = "GET"
    content_type: Literal["blog","news","book","adult","encyc","cafearticle","kin","local","errata","webkr","image","shop","doc"]
    response_type: Literal["json","xml"] = "json"

    @property
    def url(self) -> str:
        return f"{self.origin}/{self.version}/search/{self.content_type}.{self.response_type}"

    @NaverOpenAPI.with_session
    def collect(
            self,
            query: str,
            display: int = 100,
            start: int = 1,
            sort: Literal["sim","date"] = "sim",
        ) -> JsonObject:
        params = dict(query=query, display=display, start=start, sort=sort)
        return self._collect_backend(params=params)

    def _collect_backend(self, params: Dict = dict(), **kwargs) -> JsonObject:
        message = self.build_request(params=dict(params=params))
        response = self.request_json(**message)
        return self.parse(response, **params, **kwargs)

    @NaverOpenAPI.async_with_session
    async def collect_async(
            self,
            query: str,
            display: int = 100,
            start: int = 1,
            sort: Literal["sim","date"] = "sim",
        ) -> JsonObject:
        params = dict(query=query, display=display, start=start, sort=sort)
        return await self._collect_async_backend(params=params)

    async def _collect_async_backend(self, params: Dict = dict(), **kwargs) -> JsonObject:
        message = self.build_request(params=params)
        response = await self.request_async_json(**message)
        return self.parse(response, **params, **kwargs)

    def get_request_params(self, params: Dict = dict(), **kwargs) -> Dict:
        return params


class BlogSearch(_SearchCollector):
    content_type = "blog"


class NewsSearch(_SearchCollector):
    content_type = "news"


class BookSearch(_SearchCollector):
    content_type = "book"


class CafeSearch(_SearchCollector):
    content_type = "cafearticle"


class KiNSearch(_SearchCollector):
    content_type = "kin"

    @NaverOpenAPI.with_session
    def collect(
            self,
            query: str,
            display: int = 100,
            start: int = 1,
            sort: Literal["sim","date","point"] = "sim"
        ) -> JsonObject:
        params = dict(query=query, display=display, start=start, sort=sort)
        return self._collect_backend(params=params)

    @NaverOpenAPI.async_with_session
    async def collect_async(
            self,
            query: str,
            display: int = 100,
            start: int = 1,
            sort: Literal["sim","date","point"] = "sim"
        ) -> JsonObject:
        params = dict(query=query, display=display, start=start, sort=sort)
        return await self._collect_async_backend(params=params)


class ImageSearch(_SearchCollector):
    content_type = "image"

    @NaverOpenAPI.with_session
    def collect(
            self,
            query: str,
            display: int = 100,
            start: int = 1,
            sort: Literal["sim","date"] = "sim",
            filter: Literal["all","large","medium","small"] = "all",
        ) -> JsonObject:
        params = dict(query=query, display=display, start=start, sort=sort, filter=filter)
        return self._collect_backend(params=params)

    @NaverOpenAPI.async_with_session
    async def collect_async(
            self,
            query: str,
            display: int = 100,
            start: int = 1,
            sort: Literal["sim","date"] = "sim",
            filter: Literal["all","large","medium","small"] = "all",
        ) -> JsonObject:
        params = dict(query=query, display=display, start=start, sort=sort, filter=filter)
        return await self._collect_async_backend(params=params)


class ShoppingSearch(_SearchCollector):
    content_type = "shop"

    @NaverOpenAPI.with_session
    def collect(
            self,
            query: str,
            display: int = 100,
            start: int = 1,
            sort: Literal["sim","date","asc","dsc"] = "sim",
        ) -> JsonObject:
        params = dict(query=query, display=display, start=start, sort=sort)
        return self._collect_backend(params=params)

    @NaverOpenAPI.async_with_session
    async def collect_async(
            self,
            query: str,
            display: int = 100,
            start: int = 1,
            sort: Literal["sim","date","asc","dsc"] = "sim",
        ) -> JsonObject:
        params = dict(query=query, display=display, start=start, sort=sort)
        return await self._collect_async_backend(params=params)
