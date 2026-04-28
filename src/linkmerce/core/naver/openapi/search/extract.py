from __future__ import annotations
from linkmerce.core.naver.openapi import NaverOpenApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject


class _SearchExtractor(NaverOpenApi):
    """네이버 검색 API 요청을 처리하는 공통 클래스.

    - **API**: https://openapi.naver.com/v1/search/{content_type}.{xml|json}

    해당 클래스를 상속받으면서 `content_type` 속성 값을 할당해야 한다.

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    client_id: str
        네이버 오픈 API 클라이언트 아이디
    client_secret: str
        네이버 오픈 API 클라이언트 시크릿

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestLoop` Task 옵션을 전달할 수 있다.

    max_retries: int | None
        최대 반복 실행 횟수. `None`이면 조건을 만족할 때까지 무한 반복한다. 기본값은 `1`
    request_delay: Literal["incremental"] | float | int | tuple[int, int]
        재시도 간 대기 시간. `"incremental"`이면 대기 시간이 1초씩 점진적으로 증가한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachLoop` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `0.3`
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    content_type: Literal["blog", "news", "book", "adult", "encyc", "cafearticle", "kin", "local", "errata", "webkr", "image", "shop", "doc"]
    response_type: Literal["json", "xml"] = "json"
    default_options = {
        "RequestLoop": {"max_retries": 5},
        "RequestEachLoop": {"request_delay": 0.3, "max_concurrent": 3},
    }

    @property
    def url(self) -> str:
        return f"{self.origin}/{self.version}/search/{self.content_type}.{self.response_type}"

    @NaverOpenApi.with_session
    def extract(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim", "date"] = "sim",
        ) -> dict | list[dict]:
        """검색 결과를 동기 방식으로 순차 조회한다.

        Parameters
        ----------
        query: str | Iterable[str]
            검색어. 문자열 또는 문자열의 배열을 입력한다.
        start: int | Iterable[int]
            검색 시작 위치. 정수 또는 정수의 배열을 입력한다. (기본값: 1, 최댓값: 1000)
        display: int
            한 번에 표시할 검색 결과 개수 (기본값: 10, 최댓값: 100)
        sort: str
            검색 결과 정렬 방법
                - `"sim"`: 정확도순으로 내림차순 정렬 (기본값)
                - `"date"`: 날짜순으로 내림차순 정렬

        Returns
        -------
        dict | list[dict]
            검색 결과
                - `query`와 `start`가 각각 `str`, `int` 타입일 때 -> `dict`
                - `query` 또는 `start`가 `Iterable` 타입일 때 -> `list[dict]`
        """
        return self._extract_backend(query, start, display=display, sort=sort)

    @NaverOpenApi.async_with_session
    async def extract_async(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim", "date"] = "sim",
        ) -> dict | list[dict]:
        """검색 결과를 비동기 방식으로 병렬 조회한다.

        Parameters
        ----------
        query: str | Iterable[str]
            검색어. 문자열 또는 문자열의 배열을 입력한다.
        start: int | Iterable[int]
            검색 시작 위치. 정수 또는 정수의 배열을 입력한다. (기본값: 1, 최댓값: 1000)
        display: int
            한 번에 표시할 검색 결과 개수 (기본값: 10, 최댓값: 100)
        sort: Literal["sim", "date"]
            검색 결과 정렬 방법
                - `"sim"`: 정확도순으로 내림차순 정렬 (기본값)
                - `"date"`: 날짜순으로 내림차순 정렬

        Returns
        -------
        dict | list[dict]
            검색 결과
                - `query`와 `start`가 각각 `str`, `int` 타입일 때 -> `dict`
                - `query` 또는 `start`가 `Iterable` 타입일 때 -> `list[dict]`
        """
        return await self._extract_async_backend(query, start, display=display, sort=sort)

    def _extract_backend(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            **kwargs
        ) -> dict | list[dict]:
        """검색어 및 시작 위치에 대한 검색 결과를 동기 방식으로 순차 조회하는 공통 메서드."""
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
        ) -> dict | list[dict]:
        """검색어 및 시작 위치에 대한 검색 결과를 비동기 방식으로 병렬 조회하는 공통 메서드."""
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
    """네이버 블로그 검색 API 요청을 처리하는 클래스.

    - **API**: https://openapi.naver.com/v1/search/blog.{xml|json}
    - **Docs**: https://developers.naver.com/docs/serviceapi/search/blog/blog.md#블로그

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    client_id: str
        네이버 오픈 API 클라이언트 아이디
    client_secret: str
        네이버 오픈 API 클라이언트 시크릿

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestLoop` Task 옵션을 전달할 수 있다.

    max_retries: int | None
        최대 반복 실행 횟수. `None`이면 조건을 만족할 때까지 무한 반복한다. 기본값은 `1`
    request_delay: Literal["incremental"] | float | int | tuple[int, int]
        재시도 간 대기 시간. `"incremental"`이면 대기 시간이 1초씩 점진적으로 증가한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachLoop` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `0.3`
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    content_type = "blog"


class NewsSearch(_SearchExtractor):
    """네이버 블로그 뉴스 API 요청을 처리하는 클래스.

    - **API**: https://openapi.naver.com/v1/search/news.{xml|json}
    - **Docs**: https://developers.naver.com/docs/serviceapi/search/news/news.md#뉴스

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    client_id: str
        네이버 오픈 API 클라이언트 아이디
    client_secret: str
        네이버 오픈 API 클라이언트 시크릿

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestLoop` Task 옵션을 전달할 수 있다.

    max_retries: int | None
        최대 반복 실행 횟수. `None`이면 조건을 만족할 때까지 무한 반복한다. 기본값은 `1`
    request_delay: Literal["incremental"] | float | int | tuple[int, int]
        재시도 간 대기 시간. `"incremental"`이면 대기 시간이 1초씩 점진적으로 증가한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachLoop` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `0.3`
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    content_type = "news"


class BookSearch(_SearchExtractor):
    """네이버 블로그 책 API 요청을 처리하는 클래스.

    - **API**: https://openapi.naver.com/v1/search/book.{xml|json}
    - **Docs**: https://developers.naver.com/docs/serviceapi/search/book/book.md#책

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    client_id: str
        네이버 오픈 API 클라이언트 아이디
    client_secret: str
        네이버 오픈 API 클라이언트 시크릿

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestLoop` Task 옵션을 전달할 수 있다.

    max_retries: int | None
        최대 반복 실행 횟수. `None`이면 조건을 만족할 때까지 무한 반복한다. 기본값은 `1`
    request_delay: Literal["incremental"] | float | int | tuple[int, int]
        재시도 간 대기 시간. `"incremental"`이면 대기 시간이 1초씩 점진적으로 증가한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachLoop` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `0.3`
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    content_type = "book"


class CafeSearch(_SearchExtractor):
    """네이버 카페글 API 요청을 처리하는 클래스.

    - **API**: https://openapi.naver.com/v1/search/cafearticle.{xml|json}
    - **Docs**: https://developers.naver.com/docs/serviceapi/search/cafearticle/cafearticle.md#카페글

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    client_id: str
        네이버 오픈 API 클라이언트 아이디
    client_secret: str
        네이버 오픈 API 클라이언트 시크릿

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestLoop` Task 옵션을 전달할 수 있다.

    max_retries: int | None
        최대 반복 실행 횟수. `None`이면 조건을 만족할 때까지 무한 반복한다. 기본값은 `1`
    request_delay: Literal["incremental"] | float | int | tuple[int, int]
        재시도 간 대기 시간. `"incremental"`이면 대기 시간이 1초씩 점진적으로 증가한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachLoop` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `0.3`
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    content_type = "cafearticle"


class KiNSearch(_SearchExtractor):
    """네이버 지식iN API 요청을 처리하는 클래스.

    - **API**: https://openapi.naver.com/v1/search/kin.{xml|json}
    - **Docs**: https://developers.naver.com/docs/serviceapi/search/kin/kin.md#지식iN

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    client_id: str
        네이버 오픈 API 클라이언트 아이디
    client_secret: str
        네이버 오픈 API 클라이언트 시크릿

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestLoop` Task 옵션을 전달할 수 있다.

    max_retries: int | None
        최대 반복 실행 횟수. `None`이면 조건을 만족할 때까지 무한 반복한다. 기본값은 `1`
    request_delay: Literal["incremental"] | float | int | tuple[int, int]
        재시도 간 대기 시간. `"incremental"`이면 대기 시간이 1초씩 점진적으로 증가한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachLoop` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `0.3`
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    content_type = "kin"

    @NaverOpenApi.with_session
    def extract(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim", "date", "point"] = "sim",
            **kwargs
        ) -> dict | list[dict]:
        """검색 결과를 동기 방식으로 순차 조회한다.

        Parameters
        ----------
        query: str | Iterable[str]
            검색어. 문자열 또는 문자열의 배열을 입력한다.
        start: int | Iterable[int]
            검색 시작 위치. 정수 또는 정수의 배열을 입력한다. (기본값: 1, 최댓값: 1000)
        display: int
            한 번에 표시할 검색 결과 개수 (기본값: 10, 최댓값: 100)
        sort: str
            검색 결과 정렬 방법
                - `"sim"`: 정확도순으로 내림차순 정렬 (기본값)
                - `"date"`: 날짜순으로 내림차순 정렬
                - `"point"`: 평점순으로 내림차순 정렬

        Returns
        -------
        dict | list[dict]
            검색 결과
                - `query`와 `start`가 각각 `str`, `int` 타입일 때 -> `dict`
                - `query` 또는 `start`가 `Iterable` 타입일 때 -> `list[dict]`
        """
        return self._extract_backend(query, start, display=display, sort=sort, **kwargs)

    @NaverOpenApi.async_with_session
    async def extract_async(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim", "date", "point"] = "sim",
            **kwargs
        ) -> dict | list[dict]:
        """검색 결과를 비동기 방식으로 병렬 조회한다.

        Parameters
        ----------
        query: str | Iterable[str]
            검색어. 문자열 또는 문자열의 배열을 입력한다.
        start: int | Iterable[int]
            검색 시작 위치. 정수 또는 정수의 배열을 입력한다. (기본값: 1, 최댓값: 1000)
        display: int
            한 번에 표시할 검색 결과 개수 (기본값: 10, 최댓값: 100)
        sort: str
            검색 결과 정렬 방법
                - `"sim"`: 정확도순으로 내림차순 정렬 (기본값)
                - `"date"`: 날짜순으로 내림차순 정렬
                - `"point"`: 평점순으로 내림차순 정렬

        Returns
        -------
        dict | list[dict]
            검색 결과
                - `query`와 `start`가 각각 `str`, `int` 타입일 때 -> `dict`
                - `query` 또는 `start`가 `Iterable` 타입일 때 -> `list[dict]`
        """
        return await self._extract_async_backend(query, start, display=display, sort=sort, **kwargs)


class ImageSearch(_SearchExtractor):
    """네이버 이미지 API 요청을 처리하는 클래스.

    - **API**: https://openapi.naver.com/v1/search/image.{xml|json}
    - **Docs**: https://developers.naver.com/docs/serviceapi/search/image/image.md#이미지

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    client_id: str
        네이버 오픈 API 클라이언트 아이디
    client_secret: str
        네이버 오픈 API 클라이언트 시크릿

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestLoop` Task 옵션을 전달할 수 있다.

    max_retries: int | None
        최대 반복 실행 횟수. `None`이면 조건을 만족할 때까지 무한 반복한다. 기본값은 `1`
    request_delay: Literal["incremental"] | float | int | tuple[int, int]
        재시도 간 대기 시간. `"incremental"`이면 대기 시간이 1초씩 점진적으로 증가한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachLoop` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `0.3`
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

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
        ) -> dict | list[dict]:
        """검색 결과를 동기 방식으로 순차 조회한다.

        Parameters
        ----------
        query: str | Iterable[str]
            검색어. 문자열 또는 문자열의 배열을 입력한다.
        start: int | Iterable[int]
            검색 시작 위치. 정수 또는 정수의 배열을 입력한다. (기본값: 1, 최댓값: 1000)
        display: int
            한 번에 표시할 검색 결과 개수 (기본값: 10, 최댓값: 100)
        sort: str
            검색 결과 정렬 방법
                - `"sim"`: 정확도순으로 내림차순 정렬 (기본값)
                - `"date"`: 날짜순으로 내림차순 정렬
        filter: str
            크기별 검색 결과 필터
                - `"all"`: 모든 이미지 (기본값)
                - `"large"`: 큰 이미지
                - `"medium"`: 중간 크기 이미지
                - `"small"`: 작은 크기 이미지

        Returns
        -------
        dict | list[dict]
            검색 결과
                - `query`와 `start`가 각각 `str`, `int` 타입일 때 -> `dict`
                - `query` 또는 `start`가 `Iterable` 타입일 때 -> `list[dict]`
        """
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
        ) -> dict | list[dict]:
        """검색 결과를 비동기 방식으로 병렬 조회한다.

        Parameters
        ----------
        query: str | Iterable[str]
            검색어. 문자열 또는 문자열의 배열을 입력한다.
        start: int | Iterable[int]
            검색 시작 위치. 정수 또는 정수의 배열을 입력한다. (기본값: 1, 최댓값: 1000)
        display: int
            한 번에 표시할 검색 결과 개수 (기본값: 10, 최댓값: 100)
        sort: str
            검색 결과 정렬 방법
                - `"sim"`: 정확도순으로 내림차순 정렬 (기본값)
                - `"date"`: 날짜순으로 내림차순 정렬
        filter: str
            크기별 검색 결과 필터
                - `"all"`: 모든 이미지 (기본값)
                - `"large"`: 큰 이미지
                - `"medium"`: 중간 크기 이미지
                - `"small"`: 작은 크기 이미지

        Returns
        -------
        dict | list[dict]
            검색 결과
                - `query`와 `start`가 각각 `str`, `int` 타입일 때 -> `dict`
                - `query` 또는 `start`가 `Iterable` 타입일 때 -> `list[dict]`
        """
        return await self._extract_async_backend(query, start, display=display, sort=sort, filter=filter, **kwargs)


class ShopSearch(_SearchExtractor):
    """네이버 쇼핑 API 요청을 처리하는 클래스.

    - **API**: https://openapi.naver.com/v1/search/shop.{xml|json}
    - **Docs**: https://developers.naver.com/docs/serviceapi/search/shopping/shopping.md#쇼핑

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    client_id: str
        네이버 오픈 API 클라이언트 아이디
    client_secret: str
        네이버 오픈 API 클라이언트 시크릿

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestLoop` Task 옵션을 전달할 수 있다.

    max_retries: int | None
        최대 반복 실행 횟수. `None`이면 조건을 만족할 때까지 무한 반복한다. 기본값은 `1`
    request_delay: Literal["incremental"] | float | int | tuple[int, int]
        재시도 간 대기 시간. `"incremental"`이면 대기 시간이 1초씩 점진적으로 증가한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachLoop` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `0.3`
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    content_type = "shop"

    @NaverOpenApi.with_session
    def extract(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim", "date", "asc", "dsc"] = "sim",
            **kwargs
        ) -> dict | list[dict]:
        """검색 결과를 동기 방식으로 순차 조회한다.

        Parameters
        ----------
        query: str | Iterable[str]
            검색어. 문자열 또는 문자열의 배열을 입력한다.
        start: int | Iterable[int]
            검색 시작 위치. 정수 또는 정수의 배열을 입력한다. (기본값: 1, 최댓값: 1000)
        display: int
            한 번에 표시할 검색 결과 개수 (기본값: 10, 최댓값: 100)
        sort: str
            검색 결과 정렬 방법
                - `"sim"`: 정확도순으로 내림차순 정렬 (기본값)
                - `"date"`: 날짜순으로 내림차순 정렬
                - `"asc"`: 가격순으로 오름차순 정렬
                - `"dsc"`: 가격순으로 내림차순 정렬

        Returns
        -------
        dict | list[dict]
            검색 결과
                - `query`와 `start`가 각각 `str`, `int` 타입일 때 -> `dict`
                - `query` 또는 `start`가 `Iterable` 타입일 때 -> `list[dict]`
        """
        return self._extract_backend(query, start, display=display, sort=sort, **kwargs)

    @NaverOpenApi.async_with_session
    async def extract_async(
            self,
            query: str | Iterable[str],
            start: int | Iterable[int] = 1,
            display: int = 100,
            sort: Literal["sim", "date", "asc", "dsc"] = "sim",
            **kwargs
        ) -> dict | list[dict]:
        """검색 결과를 비동기 방식으로 병렬 조회한다.

        Parameters
        ----------
        query: str | Iterable[str]
            검색어. 문자열 또는 문자열의 배열을 입력한다.
        start: int | Iterable[int]
            검색 시작 위치. 정수 또는 정수의 배열을 입력한다. (기본값: 1, 최댓값: 1000)
        display: int
            한 번에 표시할 검색 결과 개수 (기본값: 10, 최댓값: 100)
        sort: str
            검색 결과 정렬 방법
                - `"sim"`: 정확도순으로 내림차순 정렬 (기본값)
                - `"date"`: 날짜순으로 내림차순 정렬
                - `"asc"`: 가격순으로 오름차순 정렬
                - `"dsc"`: 가격순으로 내림차순 정렬

        Returns
        -------
        dict | list[dict]
            검색 결과
                - `query`와 `start`가 각각 `str`, `int` 타입일 때 -> `dict`
                - `query` 또는 `start`가 `Iterable` 타입일 때 -> `list[dict]`
        """
        return await self._extract_async_backend(query, start, display=display, sort=sort, **kwargs)
