from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection


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
    ) -> JsonObject:
    """네이버 통합검색 결과의 섹션별 하위 블럭을 조회하고, 직렬화 또는 요약하여 각각의 테이블에 적재한다.

    테이블 키 | 테이블명 | 설명
    - `sections` | `naver_search_sections` | 네이버 통합검색 결과의 각 섹션 목록
    - `summary` | `naver_search_summary` | 네이버 통합검색 결과 요약"""
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
    ) -> JsonObject:
    """네이버 모바일 카페 탭 검색 결과를 조회하고 `naver_cafe_search` 테이블에 적재한다."""
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
    ) -> JsonObject:
    """네이버 카페 게시글 데이터를 조회하고 `naver_cafe_article` 테이블에 적재한다."""
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
    ) -> JsonObject:
    """네이버 모바일 카페 탭 검색 결과와 각 카페 게시글 데이터를 조합하여 통합된 결과를 생성한다.

    테이블 키 | 테이블명 | 설명
    - `search` | `naver_cafe_search` | 네이버 모바일 카페 탭 검색 결과
    - `article` | `naver_cafe_article` | 카페 게시글 데이터
    - `merged` | `naver_cafe_result` | 카페 탭 검색 결과와 게시글 데이터를 병합"""
    SEARCH, ARTICLE = 0, 1
    results = dict()
    return_type = return_type if return_type == "raw" else "none"

    results["search"] = search_cafe(
        query, mobile, cookies,
        connection=connection, request_delay=request_delay, progress=progress, return_type=return_type,
        extract_options=extract_options[SEARCH], transform_options=transform_options[SEARCH])
    if isinstance(max_rank, int):
        connection.execute(f"DELETE FROM naver_cafe_search WHERE rank > {max_rank}")

    select_query = f"SELECT DISTINCT article_url FROM naver_cafe_search WHERE article_url IS NOT NULL"
    article_url = [row[0] for row in connection.execute(select_query)[0].fetchall()]
    results["article"] = cafe_article(
        article_url, "article", cookies,
        connection=connection, request_delay=request_delay, progress=progress, return_type=return_type,
        extract_options=extract_options[ARTICLE], transform_options=transform_options[ARTICLE])

    if return_type == "raw":
        return results

    columns = [
        "L.query"
        , "L.rank"
        , "R.cafe_id"
        , "L.cafe_url"
        , "L.article_id"
        , "L.ad_id"
        , "L.cafe_name"
        , "COALESCE(R.title, L.title) AS title"
        , "R.menu_name"
        , "R.tags"
        , "R.nick_name"
        , "L.url"
        , "L.image_url"
        , "R.title_length"
        , "R.content_length"
        , "R.image_count"
        , "R.read_count"
        , "R.comment_count"
        , "R.commenter_count"
        , "COALESCE(STRFTIME(R.write_dt, '%Y-%m-%d %H:%M:%S'), L.write_date) AS write_date"
    ]

    if connection.table_exists("naver_cafe_result"):
        keyword = f"INSERT INTO naver_cafe_result "
    else:
        keyword = f"CREATE TABLE naver_cafe_result AS "
    results["merged"] = connection.execute(
        keyword
        + f"SELECT {', '.join(columns)} "
        + f"FROM (SELECT *, (ROW_NUMBER() OVER ()) AS seq FROM naver_cafe_search) AS L "
        + f"LEFT JOIN naver_cafe_article AS R "
            + "ON (L.cafe_url = R.cafe_url) AND (L.article_id = R.article_id) "
        + "ORDER BY L.seq, L.rank")
    return results
