from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Iterable, Literal
    from linkmerce.api.common import DuckDBResult
    from linkmerce.common.load import DuckDBConnection
    from bs4 import BeautifulSoup


@with_duckdb_connection(tables={"sections": "naver_search_sections", "summary": "naver_search_summary"})
def search(
        query: str | Iterable[str],
        mobile: bool = True,
        parse_html: bool = False,
        cookies: str | None = None,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1.01,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str, DuckDBResult] | str | list[str] | BeautifulSoup | list[BeautifulSoup] | None:
    """네이버 통합검색 결과 결과를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Tables** ( *table_key: table_name (description)* ):
        1. `sections: naver_search_sections` (검색 결과 섹션 목록)
        2. `summary: naver_search_summary` (검색 결과 요약)

    Parameters
    ----------
    query: str | Iterable[str]
        검색어. 문자열 또는 문자열의 배열을 입력한다.
    mobile: bool
        - `True`: 모바일 검색 (기본값)
        - `False`: PC 검색
    parse_html: bool
        - `True`: 원본 응답인 HTML 소스코드를 `BeautifulSoup` 객체로 파싱한다.
        - `False`: 원본 응답인 HTML 소스코드를 텍스트로 처리한다. (기본값)
    cookies: str | None
        네이버 로그인 쿠키 문자열을 입력하면 검색 중 세션 쿠키로 사용된다.
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        검색어별 요청 간 대기 시간(초). 기본값은 `1.01`
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
    dict[str, DuckDBResult] | str | list[str] | BeautifulSoup | list[BeautifulSoup] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 모든 테이블 조회 결과를 `{table_key: list[tuple]}` 구조로 반환한다.
            - `"json"`: 모든 테이블 조회 결과를 `{table_key: list[dict]}` 구조로 반환한다. (기본값)
            - `"parquet"`: 모든 테이블 조회 결과를 `{table_key: Parquet 바이너리}` 구조로 반환한다.
            - `"raw"`: 데이터 수집 후 HTML 텍스트 또는 `BeautifulSoup` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.naver.main.search.extract import Search
    from linkmerce.core.naver.main.search.transform import Search as T
    return Search(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(query, mobile, (parse_html and (return_type == "raw")))


@with_duckdb_connection(table="naver_cafe_search")
def search_cafe(
        query: str | Iterable[str],
        mobile: bool = True,
        cookies: str | None = None,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1.01,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | BeautifulSoup | list[BeautifulSoup] | None:
    """네이버 카페 탭 검색 결과를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: naver_cafe_search`

    Parameters
    ----------
    query: str | Iterable[str]
        검색어. 문자열 또는 문자열의 배열을 입력한다.
    mobile: bool
        - `True`: 모바일 검색 (기본값)
        - `False`: PC 검색
    cookies: str | None
        네이버 로그인 쿠키 문자열을 입력하면 검색 중 세션 쿠키로 사용된다.
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        검색어별 요청 간 대기 시간(초). 기본값은 `1.01`
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
    DuckDBResult | BeautifulSoup | list[BeautifulSoup] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `BeautifulSoup` 또는 `list[BeautifulSoup]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.naver.main.search.extract import SearchTab
    from linkmerce.core.naver.main.search.transform import CafeTab as T
    return SearchTab(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(query, "cafe", mobile)


@with_duckdb_connection(table="naver_cafe_article")
def cafe_article(
        url: str | Iterable[str],
        domain: Literal["article", "cafe", "m"] = "article",
        cookies: str | None = None,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1.01,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """네이버 카페 게시글 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: naver_cafe_article`

    Parameters
    ----------
    url: str | Iterable[str]
        카페 URL. 문자열 또는 문자열의 배열을 입력한다. 다음 3가지 도메인에 대한 URL을 허용한다.
            - `"article"`: https://article.cafe.naver.com/gw/v4/cafes/{cafe_url}/articles/{article_id}
            - `"cafe"`: https://cafe.naver.com/{cafe_url}/{article_id}
            - `"m"`: https://m.cafe.naver.com/{cafe_url}/{article_id}
    domain: str
        카페 URL의 도메인. `url` 파라미터로 전달되는 URL 값은 전부 동일한 도메인이어야 한다.
    cookies: str | None
        네이버 로그인 쿠키 문자열을 입력하면 검색 중 세션 쿠키로 사용된다.
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        카페 URL별 요청 간 대기 시간(초). 기본값은 `1.01`
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
    from linkmerce.core.naver.main.search.extract import CafeArticle
    from linkmerce.core.naver.main.search.transform import CafeArticle as T
    return CafeArticle(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(url, domain)


@with_duckdb_connection(tables={
    "search": "naver_cafe_search",
    "article": "naver_cafe_article",
    "merged": "naver_cafe_result"
})
def search_cafe_plus(
        query: str | Iterable[str],
        mobile: bool = True,
        max_rank: int | None = None,
        cookies: str | None = None,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1.01,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: tuple[dict | None, dict | None] = (None, None),
        transform_options: tuple[dict | None, dict | None] = (None, None),
        merged_table: str | None = None,
    ) -> dict[str, DuckDBResult] | dict[str, Any] | None:
    """네이버 카페 검색 후 결과 게시글의 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Tables** ( *table_key: table_name (description)* ):
        1. `search: naver_cafe_search` (카페 탭 검색 결과)
        2. `article: naver_cafe_article` (카페 게시글 데이터)
        3. `merged: naver_cafe_result` (검색 결과와 게시글 병합 결과)

    Parameters
    ----------
    query: str | Iterable[str]
        검색어. 문자열 또는 문자열의 배열을 입력한다.
    mobile: bool
        - `True`: 모바일 검색 (기본값)
        - `False`: PC 검색
    max_rank: int | None
        최대 순위. 값이 입력되면 검색 결과에서 순위를 넘어서는 게시글을 제외한다.
    cookies: str | None
        네이버 로그인 쿠키 문자열을 입력하면 검색 중 세션 쿠키로 사용된다.
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        검색어 및 카페 URL별 요청 간 대기 시간(초). 기본값은 `1.01`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: tuple[dict | None, dict | None]
        `Extractor` 초기화 옵션. `(SearchTab, CafeArticle)` 순서로 튜플을 구성한다.
    transform_options: tuple[dict | None, dict | None]
        `Transformer` 초기화 옵션. `(CafeTab, CafeArticle)` 순서로 튜플을 구성한다.   
        실행 결과의 `table_key`에 해당하는 테이블은 아래와 같이 각 `Transformer.table_key`와 매칭된다.
            - `search` - `CafeTab.table`
            - `article` - `CafeArticle.table`
    merged_table: str | None
        검색 결과와 게시글 병합 결과를 적재할 테이블 명칭. 생략하면 `"naver_cafe_result"` 테이블을 생성한다.

    Returns
    -------
    dict[str, DuckDBResult] | dict[str, Any] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 모든 테이블 조회 결과를 `{table_key: list[tuple]}` 구조로 반환한다.
            - `"json"`: 모든 테이블 조회 결과를 `{table_key: list[dict]}` 구조로 반환한다. (기본값)
            - `"parquet"`: 모든 테이블 조회 결과를 `{table_key: Parquet 바이너리}` 구조로 반환한다.
            - `"raw"`: 데이터 수집 후 `{"search": SearchTab, "article": CafeTab}` 구조의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.api.common import get_table
    SEARCH, ARTICLE = 0, 1
    search_table = get_table(transform_options[SEARCH], default="naver_cafe_search")
    article_table = get_table(transform_options[ARTICLE], default="naver_cafe_article")

    results = dict()
    return_type = return_type if return_type == "raw" else "none"

    results["search"] = search_cafe(
        query, mobile, cookies,
        connection=connection, request_delay=request_delay, progress=progress, return_type=return_type,
        extract_options=extract_options[SEARCH], transform_options=transform_options[SEARCH])

    if return_type == "raw":
        from linkmerce.core.naver.main.search.transform import CafeParser
        parser = CafeParser()

        if isinstance(results["search"], list):
            urls = [article["article_url"] for result in results["search"] for article in parser.transform(result)[:max_rank]]
        else:
            urls = [article["article_url"] for article in parser.transform(results["search"])[:max_rank]]

        article_url = list(dict.fromkeys(urls))

    else:
        if isinstance(max_rank, int):
            connection.execute(f"DELETE FROM {search_table} WHERE rank > {max_rank}")

        q = f"SELECT DISTINCT article_url FROM {search_table} WHERE article_url IS NOT NULL"
        article_url = [row[0] for row in connection.execute(q)[0].fetchall()]

    results["article"] = cafe_article(
        article_url, "article", cookies,
        connection=connection, request_delay=request_delay, progress=progress, return_type=return_type,
        extract_options=extract_options[ARTICLE], transform_options=transform_options[ARTICLE])

    if return_type == "raw":
        return results

    table = merged_table or "naver_cafe_result"
    keyword = f"INSERT INTO {table}" if connection.table_exists(table) else f"CREATE TABLE {table} AS"

    from textwrap import dedent
    results["merged"] = connection.execute(
        dedent(f"""{keyword}
        SELECT
            L.query,
            L.rank,
            R.cafe_id,
            L.cafe_url,
            L.article_id,
            L.ad_id,
            L.cafe_name,
            COALESCE(R.title, L.title) AS title,
            R.menu_name,
            R.tags,
            R.nick_name,
            L.url,
            L.image_url,
            R.title_length,
            R.content_length,
            R.image_count,
            R.read_count,
            R.comment_count,
            R.commenter_count,
            COALESCE(STRFTIME(R.write_dt, '%Y-%m-%d %H:%M:%S'), L.write_date) AS write_date
        FROM (SELECT *, (ROW_NUMBER() OVER ()) AS seq FROM {search_table}) AS L
        LEFT JOIN {article_table} AS R
            ON (L.cafe_url = R.cafe_url) AND (L.article_id = R.article_id)
        ORDER BY L.seq, L.rank
        """))

    return results
