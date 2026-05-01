from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.api.common import DuckDBResult
    from linkmerce.common.load import DuckDBConnection


def _search_config(
        client_id: str,
        client_secret: str,
        max_concurrent: int = 3,
        max_retries: int = 5,
        request_delay: float | int = 0.3,
        progress: bool = True,
    ) -> dict:
    """네이버 Open API 검색 Extractor의 공통 설정을 생성한다."""
    return dict(
        configs = {
            "client_id": client_id,
            "client_secret": client_secret
        },
        options = {
            "RequestLoop": {
                "max_retries": max_retries
            },
            "RequestEachLoop": {
                "request_delay": request_delay,
                "max_concurrent": max_concurrent,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )


@with_duckdb_connection(table="naver_blog")
def search_blog(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim", "date"] = "sim",
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        max_retries: int = 5,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """네이버 오픈 API 블로그 검색 결과를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: naver_blog`

    Parameters
    ----------
    client_id: str
        네이버 오픈 API 클라이언트 아이디
    client_secret: str
        네이버 오픈 API 클라이언트 시크릿
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
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    how_to_run: str
        `Extractor` 실행 방식
            - `"sync"`: 동기 실행. `extract` 메서드를 실행한다. (기본값)
            - `"async"`: 비동기 실행. `asyncio.run` 함수로 감싸서 `extract_async` 코루틴을 실행한다.
            - `"async_loop"`: 비동기 루프 내 실행. 주피터 노트북 등 이미 비동기 이벤트 루프 내에 있을 경우에 비동기 실행을 처리한다.
    max_concurrent: int
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    max_retries: int
        최대 반복 실행 횟수. 기본값은 `5`
    request_delay: float | int | tuple[int, int]
        검색어별 요청 간 대기 시간(초). 기본값은 `0.3`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    DuckDBResult | dict | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 또는 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.naver.openapi.search.extract import BlogSearch
    from linkmerce.core.naver.openapi.search.transform import BlogSearch as T
    return BlogSearch(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        **_search_config(client_id, client_secret, max_concurrent, max_retries, request_delay, progress),
    )).run(query, start, display, sort, how_to_run=how_to_run)


@with_duckdb_connection(table="naver_news")
def search_news(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim", "date"] = "sim",
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        max_retries: int = 5,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """네이버 오픈 API 뉴스 검색 결과를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: naver_news`

    Parameters
    ----------
    client_id: str
        네이버 오픈 API 클라이언트 아이디
    client_secret: str
        네이버 오픈 API 클라이언트 시크릿
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
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    how_to_run: str
        `Extractor` 실행 방식
            - `"sync"`: 동기 실행. `extract` 메서드를 실행한다. (기본값)
            - `"async"`: 비동기 실행. `asyncio.run` 함수로 감싸서 `extract_async` 코루틴을 실행한다.
            - `"async_loop"`: 비동기 루프 내 실행. 주피터 노트북 등 이미 비동기 이벤트 루프 내에 있을 경우에 비동기 실행을 처리한다.
    max_concurrent: int
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    max_retries: int
        최대 반복 실행 횟수. 기본값은 `5`
    request_delay: float | int | tuple[int, int]
        검색어별 요청 간 대기 시간(초). 기본값은 `0.3`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    DuckDBResult | dict | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 또는 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.naver.openapi.search.extract import NewsSearch
    from linkmerce.core.naver.openapi.search.transform import NewsSearch as T
    return NewsSearch(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        **_search_config(client_id, client_secret, max_concurrent, max_retries, request_delay, progress),
    )).run(query, start, display, sort, how_to_run=how_to_run)


@with_duckdb_connection(table="naver_book")
def search_book(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim", "date"] = "sim",
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        max_retries: int = 5,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """네이버 오픈 API 책 검색 결과를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: naver_book`

    Parameters
    ----------
    client_id: str
        네이버 오픈 API 클라이언트 아이디
    client_secret: str
        네이버 오픈 API 클라이언트 시크릿
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
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    how_to_run: str
        `Extractor` 실행 방식
            - `"sync"`: 동기 실행. `extract` 메서드를 실행한다. (기본값)
            - `"async"`: 비동기 실행. `asyncio.run` 함수로 감싸서 `extract_async` 코루틴을 실행한다.
            - `"async_loop"`: 비동기 루프 내 실행. 주피터 노트북 등 이미 비동기 이벤트 루프 내에 있을 경우에 비동기 실행을 처리한다.
    max_concurrent: int
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    max_retries: int
        최대 반복 실행 횟수. 기본값은 `5`
    request_delay: float | int | tuple[int, int]
        검색어별 요청 간 대기 시간(초). 기본값은 `0.3`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    DuckDBResult | dict | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 또는 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.naver.openapi.search.extract import BookSearch
    from linkmerce.core.naver.openapi.search.transform import BookSearch as T
    return BookSearch(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        **_search_config(client_id, client_secret, max_concurrent, max_retries, request_delay, progress),
    )).run(query, start, display, sort, how_to_run=how_to_run)


@with_duckdb_connection(table="naver_cafe")
def search_cafe(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim", "date"] = "sim",
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        max_retries: int = 5,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """네이버 오픈 API 카페글 검색 결과를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: naver_cafe`

    Parameters
    ----------
    client_id: str
        네이버 오픈 API 클라이언트 아이디
    client_secret: str
        네이버 오픈 API 클라이언트 시크릿
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
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    how_to_run: str
        `Extractor` 실행 방식
            - `"sync"`: 동기 실행. `extract` 메서드를 실행한다. (기본값)
            - `"async"`: 비동기 실행. `asyncio.run` 함수로 감싸서 `extract_async` 코루틴을 실행한다.
            - `"async_loop"`: 비동기 루프 내 실행. 주피터 노트북 등 이미 비동기 이벤트 루프 내에 있을 경우에 비동기 실행을 처리한다.
    max_concurrent: int
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    max_retries: int
        최대 반복 실행 횟수. 기본값은 `5`
    request_delay: float | int | tuple[int, int]
        검색어별 요청 간 대기 시간(초). 기본값은 `0.3`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    DuckDBResult | dict | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 또는 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.naver.openapi.search.extract import CafeSearch
    from linkmerce.core.naver.openapi.search.transform import CafeSearch as T
    return CafeSearch(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        **_search_config(client_id, client_secret, max_concurrent, max_retries, request_delay, progress),
    )).run(query, start, display, sort, how_to_run=how_to_run)


@with_duckdb_connection(table="naver_kin")
def search_kin(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim", "date", "point"] = "sim",
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        max_retries: int = 5,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """네이버 오픈 API 지식iN 검색 결과를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: naver_kin`

    Parameters
    ----------
    client_id: str
        네이버 오픈 API 클라이언트 아이디
    client_secret: str
        네이버 오픈 API 클라이언트 시크릿
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
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    how_to_run: str
        `Extractor` 실행 방식
            - `"sync"`: 동기 실행. `extract` 메서드를 실행한다. (기본값)
            - `"async"`: 비동기 실행. `asyncio.run` 함수로 감싸서 `extract_async` 코루틴을 실행한다.
            - `"async_loop"`: 비동기 루프 내 실행. 주피터 노트북 등 이미 비동기 이벤트 루프 내에 있을 경우에 비동기 실행을 처리한다.
    max_concurrent: int
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    max_retries: int
        최대 반복 실행 횟수. 기본값은 `5`
    request_delay: float | int | tuple[int, int]
        검색어별 요청 간 대기 시간(초). 기본값은 `0.3`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    DuckDBResult | dict | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 또는 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.naver.openapi.search.extract import KiNSearch
    from linkmerce.core.naver.openapi.search.transform import KiNSearch as T
    return KiNSearch(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        **_search_config(client_id, client_secret, max_concurrent, max_retries, request_delay, progress),
    )).run(query, start, display, sort, how_to_run=how_to_run)


@with_duckdb_connection(table="naver_image")
def search_image(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim", "date"] = "sim",
        filter: Literal["all", "large", "medium", "small"] = "all",
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        max_retries: int = 5,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """네이버 오픈 API 이미지 검색 결과를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: naver_image`

    Parameters
    ----------
    client_id: str
        네이버 오픈 API 클라이언트 아이디
    client_secret: str
        네이버 오픈 API 클라이언트 시크릿
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
        이미지 크기 필터
            - `"all"`: 전체 (기본값)
            - `"large"`: 대형
            - `"medium"`: 중형
            - `"small"`: 소형
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    how_to_run: str
        `Extractor` 실행 방식
            - `"sync"`: 동기 실행. `extract` 메서드를 실행한다. (기본값)
            - `"async"`: 비동기 실행. `asyncio.run` 함수로 감싸서 `extract_async` 코루틴을 실행한다.
            - `"async_loop"`: 비동기 루프 내 실행. 주피터 노트북 등 이미 비동기 이벤트 루프 내에 있을 경우에 비동기 실행을 처리한다.
    max_concurrent: int
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    max_retries: int
        최대 반복 실행 횟수. 기본값은 `5`
    request_delay: float | int | tuple[int, int]
        검색어별 요청 간 대기 시간(초). 기본값은 `0.3`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    DuckDBResult | dict | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 또는 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.naver.openapi.search.extract import ImageSearch
    from linkmerce.core.naver.openapi.search.transform import ImageSearch as T
    return ImageSearch(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        **_search_config(client_id, client_secret, max_concurrent, max_retries, request_delay, progress),
    )).run(query, start, display, sort, filter, how_to_run=how_to_run)


@with_duckdb_connection(table="naver_shop")
def search_shop(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim", "date", "asc", "dsc"] = "sim",
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        max_retries: int = 5,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """네이버 오픈 API 쇼핑 검색 결과를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: naver_shop`

    Parameters
    ----------
    client_id: str
        네이버 오픈 API 클라이언트 아이디
    client_secret: str
        네이버 오픈 API 클라이언트 시크릿
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
            - `"asc"`: 가격 오름차순 정렬
            - `"dsc"`: 가격 내림차순 정렬
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    how_to_run: str
        `Extractor` 실행 방식
            - `"sync"`: 동기 실행. `extract` 메서드를 실행한다. (기본값)
            - `"async"`: 비동기 실행. `asyncio.run` 함수로 감싸서 `extract_async` 코루틴을 실행한다.
            - `"async_loop"`: 비동기 루프 내 실행. 주피터 노트북 등 이미 비동기 이벤트 루프 내에 있을 경우에 비동기 실행을 처리한다.
    max_concurrent: int
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    max_retries: int
        최대 반복 실행 횟수. 기본값은 `5`
    request_delay: float | int | tuple[int, int]
        검색어별 요청 간 대기 시간(초). 기본값은 `0.3`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    DuckDBResult | dict | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 또는 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.naver.openapi.search.extract import ShopSearch
    from linkmerce.core.naver.openapi.search.transform import ShopSearch as T
    return ShopSearch(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        **_search_config(client_id, client_secret, max_concurrent, max_retries, request_delay, progress),
    )).run(query, start, display, sort, how_to_run=how_to_run)


@with_duckdb_connection(tables={"rank": "naver_shop_rank", "product": "naver_shop_product"})
def shop_rank(
        client_id: str,
        client_secret: str,
        query: str | Iterable[str],
        start: int | Iterable[int] = 1,
        display: int = 100,
        sort: Literal["sim", "date", "asc", "dsc"] = "sim",
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        max_retries: int = 5,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str, DuckDBResult] | dict | list[dict] | None:
    """네이버 오픈 API 쇼핑 검색 결과를 수집하고 순위와 상품 데이터로 분리해 각각의 DuckDB 테이블에 적재한다.

    **Tables** ( *table_key: table_name (description)* ):
        1. `rank: naver_shop_rank` (상품 순위)
        2. `product: naver_shop_product` (상품 목록)

    Parameters
    ----------
    client_id: str
        네이버 오픈 API 클라이언트 아이디
    client_secret: str
        네이버 오픈 API 클라이언트 시크릿
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
            - `"asc"`: 가격 오름차순 정렬
            - `"dsc"`: 가격 내림차순 정렬
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    how_to_run: str
        `Extractor` 실행 방식
            - `"sync"`: 동기 실행. `extract` 메서드를 실행한다. (기본값)
            - `"async"`: 비동기 실행. `asyncio.run` 함수로 감싸서 `extract_async` 코루틴을 실행한다.
            - `"async_loop"`: 비동기 루프 내 실행. 주피터 노트북 등 이미 비동기 이벤트 루프 내에 있을 경우에 비동기 실행을 처리한다.
    max_concurrent: int
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    max_retries: int
        최대 반복 실행 횟수. 기본값은 `5`
    request_delay: float | int | tuple[int, int]
        검색어별 요청 간 대기 시간(초). 기본값은 `0.3`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    dict[str, DuckDBResult] | dict | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 모든 테이블 조회 결과를 `{table_key: list[tuple]}` 구조로 반환한다.
            - `"json"`: 모든 테이블 조회 결과를 `{table_key: list[dict]}` 구조로 반환한다. (기본값)
            - `"parquet"`: 모든 테이블 조회 결과를 `{table_key: Parquet 바이너리}` 구조로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 또는 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.naver.openapi.search.extract import ShopSearch
    from linkmerce.core.naver.openapi.search.transform import ShopRank as T
    return ShopSearch(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        **_search_config(client_id, client_secret, max_concurrent, max_retries, request_delay, progress),
    )).run(query, start, display, sort, how_to_run=how_to_run)
