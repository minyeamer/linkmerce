from __future__ import annotations
from linkmerce.core.naver.openapi import NaverOpenAPI

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject


class _SearchExtractor(NaverOpenAPI):
    """
    Search various types of content using the Naver Open API.

    This extractor sends a GET request to the Naver Open API endpoint for 
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

    @property
    def default_options(self) -> dict:
        return dict(
            RequestLoop = dict(count=5),
            RequestEachLoop = dict(delay=0.3, limit=3))

    @NaverOpenAPI.with_session
    def extract(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim","date"] = "sim",
        ) -> JsonObject:
        return self._extract_backend(query, start, display=display, sort=sort)

    @NaverOpenAPI.async_with_session
    async def extract_async(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim","date"] = "sim",
        ) -> JsonObject:
        return await self._extract_async_backend(query, start, display=display, sort=sort)

    def _extract_backend(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            **kwargs
        ) -> JsonObject:
        return (self.request_each_loop(self.request_json_safe)
                .partial(**kwargs)
                .expand(query=query, start=start)
                .loop(self.is_valid_response)
                .run())

    async def _extract_async_backend(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            **kwargs
        ) -> JsonObject:
        return await (self.request_each_loop(self.request_async_json_safe)
                .partial(**kwargs)
                .expand(query=query, start=start)
                .loop(self.is_valid_response)
                .run_async())

    def build_request_params(self, **kwargs) -> dict:
        return kwargs

    def is_valid_response(self, response: JsonObject) -> bool:
        return not (isinstance(response, dict) and response.get("errorCode"))


class BlogSearch(_SearchExtractor):
    content_type = "blog"


class NewsSearch(_SearchExtractor):
    content_type = "news"


class BookSearch(_SearchExtractor):
    content_type = "book"


class CafeSearch(_SearchExtractor):
    content_type = "cafearticle"


class KiNSearch(_SearchExtractor):
    content_type = "kin"

    @NaverOpenAPI.with_session
    def extract(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim","date","point"] = "sim",
            **kwargs
        ) -> JsonObject:
        return self._extract_backend(query, start, display=display, sort=sort, **kwargs)

    @NaverOpenAPI.async_with_session
    async def extract_async(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim","date","point"] = "sim",
            **kwargs
        ) -> JsonObject:
        return await self._extract_async_backend(query, start, display=display, sort=sort, **kwargs)


class ImageSearch(_SearchExtractor):
    content_type = "image"

    @NaverOpenAPI.with_session
    def extract(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim","date"] = "sim",
            filter: Literal["all","large","medium","small"] = "all",
            **kwargs
        ) -> JsonObject:
        return self._extract_backend(query, start, display=display, sort=sort, filter=filter, **kwargs)

    @NaverOpenAPI.async_with_session
    async def extract_async(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim","date"] = "sim",
            filter: Literal["all","large","medium","small"] = "all",
            **kwargs
        ) -> JsonObject:
        return await self._extract_async_backend(query, start, display=display, sort=sort, filter=filter, **kwargs)


class ShoppingSearch(_SearchExtractor):
    content_type = "shop"

    @NaverOpenAPI.with_session
    def extract(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim","date","asc","dsc"] = "sim",
            **kwargs
        ) -> JsonObject:
        return self._extract_backend(query, start, display=display, sort=sort, **kwargs)

    @NaverOpenAPI.async_with_session
    async def extract_async(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim","date","asc","dsc"] = "sim",
            **kwargs
        ) -> JsonObject:
        return await self._extract_async_backend(query, start, display=display, sort=sort, **kwargs)


class ShoppingRank(ShoppingSearch):
    content_type = "shop"
