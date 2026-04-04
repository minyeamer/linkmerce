from __future__ import annotations
from linkmerce.core.naver.openapi import NaverOpenApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject


class _SearchExtractor(NaverOpenApi):
    """네이버 오픈 API로 다양한 콘텐츠를 검색하는 공통 클래스.

    `RequestEachLoop` Task를 사용하여 `query`와 `start` 파라미터의 조합으로 API 요청을 순차 또는 병렬 실행한다.
    - API 문서: https://developers.naver.com/docs/serviceapi/search/"""

    method = "GET"
    content_type: Literal["blog", "news", "book", "adult", "encyc", "cafearticle", "kin", "local", "errata", "webkr", "image", "shop", "doc"]
    response_type: Literal["json", "xml"] = "json"

    @property
    def url(self) -> str:
        """검색 API URL을 조합해 반환한다."""
        return f"{self.origin}/{self.version}/search/{self.content_type}.{self.response_type}"

    @property
    def default_options(self) -> dict:
        return {
            "RequestLoop": {"max_retries": 5},
            "RequestEachLoop": {"request_delay": 0.3, "max_concurrent": 3},
        }

    @NaverOpenApi.with_session
    def extract(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim", "date"] = "sim",
        ) -> JsonObject:
        """키워드(`query`)별 검색 결과를 동기 방식으로 순차 조회한다."""
        return self._extract_backend(query, start, display=display, sort=sort)

    @NaverOpenApi.async_with_session
    async def extract_async(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim", "date"] = "sim",
        ) -> JsonObject:
        """키워드(`query`)별 검색 결과를 비동기 방식으로 병렬 조회한다."""
        return await self._extract_async_backend(query, start, display=display, sort=sort)

    def _extract_backend(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            **kwargs
        ) -> JsonObject:
        """키워드(`query`)별 검색 결과를 동기 방식으로 순차 조회하는 공통 로직."""
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
        """키워드(`query`)별 검색 결과를 비동기 방식으로 병렬 조회하는 공통 로직."""
        return await (self.request_each_loop(self.request_async_json_safe)
                .partial(**kwargs)
                .expand(query=query, start=start)
                .loop(self.is_valid_response)
                .run_async())

    def build_request_params(self, **kwargs) -> dict:
        return kwargs

    def is_valid_response(self, response: JsonObject) -> bool:
        """HTTP 응답에 에러 코드가 없는지 검증하여 재시도할지 여부를 판단한다."""
        return not (isinstance(response, dict) and response.get("errorCode"))


class BlogSearch(_SearchExtractor):
    """네이버 블로그 검색 API를 요청하는 클래스."""

    content_type = "blog"


class NewsSearch(_SearchExtractor):
    """네이버 뉴스 검색 API를 요청하는 클래스."""

    content_type = "news"


class BookSearch(_SearchExtractor):
    """네이버 도서 검색 API를 요청하는 클래스."""

    content_type = "book"


class CafeSearch(_SearchExtractor):
    """네이버 카페 검색 API를 요청하는 클래스."""

    content_type = "cafearticle"


class KiNSearch(_SearchExtractor):
    """네이버 지식인 검색 API를 요청하는 클래스."""
    content_type = "kin"

    @NaverOpenApi.with_session
    def extract(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim", "date", "point"] = "sim",
            **kwargs
        ) -> JsonObject:
        """키워드(`query`)별 검색 결과를 동기 방식으로 순차 조회한다."""
        return self._extract_backend(query, start, display=display, sort=sort, **kwargs)

    @NaverOpenApi.async_with_session
    async def extract_async(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim", "date", "point"] = "sim",
            **kwargs
        ) -> JsonObject:
        """키워드(`query`)별 검색 결과를 비동기 방식으로 병렬 조회한다."""
        return await self._extract_async_backend(query, start, display=display, sort=sort, **kwargs)


class ImageSearch(_SearchExtractor):
    """네이버 이미지 검색 API를 요청하는 클래스."""

    content_type = "image"

    @NaverOpenApi.with_session
    def extract(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim", "date"] = "sim",
            filter: Literal["all", "large", "medium", "small"] = "all",
            **kwargs
        ) -> JsonObject:
        """키워드(`query`)별 검색 결과를 동기 방식으로 순차 조회한다."""
        return self._extract_backend(query, start, display=display, sort=sort, filter=filter, **kwargs)

    @NaverOpenApi.async_with_session
    async def extract_async(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim", "date"] = "sim",
            filter: Literal["all", "large", "medium", "small"] = "all",
            **kwargs
        ) -> JsonObject:
        """키워드(`query`)별 검색 결과를 비동기 방식으로 병렬 조회한다."""
        return await self._extract_async_backend(query, start, display=display, sort=sort, filter=filter, **kwargs)


class ShopSearch(_SearchExtractor):
    """네이버 쇼핑 검색 API를 요청하는 클래스."""

    content_type = "shop"

    @NaverOpenApi.with_session
    def extract(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim", "date", "asc", "dsc"] = "sim",
            **kwargs
        ) -> JsonObject:
        """키워드(`query`)별 검색 결과를 동기 방식으로 순차 조회한다."""
        return self._extract_backend(query, start, display=display, sort=sort, **kwargs)

    @NaverOpenApi.async_with_session
    async def extract_async(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim", "date", "asc", "dsc"] = "sim",
            **kwargs
        ) -> JsonObject:
        """키워드(`query`)별 검색 결과를 비동기 방식으로 병렬 조회한다."""
        return await self._extract_async_backend(query, start, display=display, sort=sort, **kwargs)
