from __future__ import annotations
from linkmerce.collect.naver.api import NaverOpenAPI

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.types import JsonObject, TaskOptions


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
    args = ["query", "start", "display", "sort"]

    @property
    def url(self) -> str:
        return f"{self.origin}/{self.version}/search/{self.content_type}.{self.response_type}"

    def set_options(self, options: TaskOptions = dict()):
        if self.args[:2] != ["query", "start"]:
            ValueError("Arguments must start with 'query' followed by 'start'")
        super().set_options(options or dict(RequestLoop=dict(count=5), RequestEach=dict(delay=0.3, limit=3)))

    @NaverOpenAPI.with_session
    def collect(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim","date"] = "sim",
            **kwargs
        ) -> JsonObject:
        return self._collect_backend(query, start, display, sort, **kwargs)

    @NaverOpenAPI.async_with_session
    async def collect_async(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim","date"] = "sim",
            **kwargs
        ) -> JsonObject:
        return await self._collect_async_backend(query, start, display, sort, **kwargs)

    def _collect_backend(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            *args,
            **kwargs
        ) -> JsonObject:
        return (self.request_each(self._build_and_request)
                .partial(**dict(zip(self.args[2:], args))).expand(query=query, start=start)
                .parse(**self.update_parser(**kwargs))
                .loop(self._is_valid_response).concat("auto").run())

    def _build_and_request(self, **params) -> JsonObject:
        message = self.build_request(params=params)
        return self.request_json(**message)

    async def _collect_async_backend(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            *args,
            **kwargs
        ) -> JsonObject:
        return (await self.request_each(self._build_and_request_async)
                .partial(**dict(zip(self.args[2:], args))).expand(query=query, start=start)
                .parse(**self.update_parser(**kwargs))
                .loop(self._is_valid_response).concat("auto").run_async())

    async def _build_and_request_async(self, **params) -> JsonObject:
        message = self.build_request(params=params)
        return await self.request_async_json(**message)

    def _is_valid_response(self, response: JsonObject) -> bool:
        return not (isinstance(response, dict) and (response.get("errorCode") == "012"))


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
    args = ["query", "start", "display", "sort"]

    @NaverOpenAPI.with_session
    def collect(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim","date","point"] = "sim",
            **kwargs
        ) -> JsonObject:
        return self._collect_backend(query, start, display, sort, **kwargs)

    @NaverOpenAPI.async_with_session
    async def collect_async(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim","date","point"] = "sim",
            **kwargs
        ) -> JsonObject:
        return await self._collect_async_backend(query, start, display, sort, **kwargs)


class ImageSearch(_SearchCollector):
    content_type = "image"
    args = ["query", "start", "display", "sort", "filter"]

    @NaverOpenAPI.with_session
    def collect(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim","date"] = "sim",
            filter: Literal["all","large","medium","small"] = "all",
            **kwargs
        ) -> JsonObject:
        return self._collect_backend(query, start, display, sort, filter, **kwargs)

    @NaverOpenAPI.async_with_session
    async def collect_async(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim","date"] = "sim",
            filter: Literal["all","large","medium","small"] = "all",
            **kwargs
        ) -> JsonObject:
        return await self._collect_async_backend(query, start, display, sort, filter, **kwargs)


class ShoppingSearch(_SearchCollector):
    content_type = "shop"
    args = ["query", "start", "display", "sort"]

    @NaverOpenAPI.with_session
    def collect(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim","date","asc","dsc"] = "sim",
            **kwargs
        ) -> JsonObject:
        return self._collect_backend(query, start, display, sort, **kwargs)

    @NaverOpenAPI.async_with_session
    async def collect_async(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim","date","asc","dsc"] = "sim",
            **kwargs
        ) -> JsonObject:
        return await self._collect_async_backend(query, start, display, sort, **kwargs)


class ShoppingRank(ShoppingSearch):
    ...
