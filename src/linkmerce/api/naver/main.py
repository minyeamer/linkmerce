from __future__ import annotations

from linkmerce.common.api import run_with_duckdb, update_options

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection


def get_module(name: str) -> str:
    return (".naver.main" + name) if name.startswith('.') else name


def get_options(
        request_delay: float | int = 1.01,
        progress: bool = True,
    ) -> dict:
    return dict(
        RequestEach = dict(request_delay=request_delay, tqdm_options=dict(disable=(not progress))),
    )


def _search_tab(
        query: str | Iterable[str],
        tab_type: Literal["image","blog","cafe","kin","influencer","clip","video","news","surf","shortents"],
        cookies: str | None = None,
        transformer: str | None = None,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 1.01,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.naver.main.search.extract import MobileTabSearch
    return run_with_duckdb(
        module = get_module(".search"),
        extractor = "MobileTabSearch",
        transformer = transformer,
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (query, tab_type),
        extract_options = update_options(
            extract_options,
            **(dict(headers=dict(cookies=cookies)) if cookies else dict()),
            options = get_options(request_delay, progress),
        ),
        transform_options = transform_options,
    )


def search_cafe(
        query: str | Iterable[str],
        cookies: str | None = None,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 1.01,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.naver.main.search.extract import MobileTabSearch
    # from linkmerce.core.naver.main.search.transform import CafeTab
    return _search_tab(
        query, "cafe", cookies, "CafeTab", connection, tables,
        request_delay, progress, return_type, extract_options, transform_options)


def cafe_article(
        url: str | Iterable[str],
        domain: Literal["article","cafe","m"] = "article",
        cookies: str | None = None,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 1.01,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.naver.main.search.extract import CafeArticle
    # from linkmerce.core.naver.main.search.transform import CafeArticle
    return run_with_duckdb(
        module = get_module(".search"),
        extractor = "CafeArticle",
        transformer = "CafeArticle",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (url, domain),
        extract_options = update_options(
            extract_options,
            **(dict(headers=dict(cookies=cookies)) if cookies else dict()),
            options = get_options(request_delay, progress),
        ),
        transform_options = transform_options,
    )


def search_cafe_plus(
        connection: DuckDBConnection,
        query: str | Iterable[str],
        cookies: str | None = None,
        max_rank: int | None = None,
        tables: dict | None = None,
        request_delay: float | int = 1.01,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
        if_merged_table_exists: Literal["insert","replace"] = "replace",
    ) -> JsonObject:
    """`tables = {'search': 'naver_cafe_search', 'article': 'naver_cafe_article', 'merged': 'data'}`"""
    # from linkmerce.core.naver.main.search.extract import MobileTabSearch, CafeArticle
    # from linkmerce.core.naver.main.search.transform import CafeTab, CafeArticle
    from copy import deepcopy
    results = dict()
    common = (request_delay, progress, return_type)
    search_table = (tables or dict()).get("search", "naver_cafe_search")
    article_table = (tables or dict()).get("article", "naver_cafe_article")
    merged_table = (tables or dict()).get("merged", "data")

    options = (deepcopy(extract_options), deepcopy(transform_options))
    results["search"] = search_cafe(query, cookies, connection, dict(default=search_table), *common, *options)
    if isinstance(max_rank, int):
        connection.execute(f"DELETE FROM {search_table} WHERE rank > {max_rank}")

    select_query = f"SELECT DISTINCT next_url FROM {search_table} WHERE next_url IS NOT NULL;"
    next_url = [row[0] for row in connection.execute(select_query).fetchall()]
    options = (deepcopy(extract_options), deepcopy(transform_options))
    results["article"] = cafe_article(next_url, "article", cookies, connection, dict(default=article_table), *common, *options)

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
        # , "L.description"
        # , "L.replies AS comments"
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
    if if_merged_table_exists == "replace":
        keyword = f"CREATE OR REPLACE TABLE {merged_table} AS "
    elif connection.table_exists(merged_table):
        keyword = f"INSERT INTO {merged_table} "
    else:
        keyword = f"CREATE TABLE {merged_table} AS "
    connection.execute(
        keyword
        + f"SELECT {', '.join(columns)} "
        + f"FROM (SELECT *, (ROW_NUMBER() OVER ()) AS seq FROM {search_table}) AS L "
        + f"LEFT JOIN {article_table} AS R "
            + "ON (L.cafe_url = R.cafe_url) AND (L.article_id = R.article_id) "
        + "ORDER BY L.seq;")

    if return_type == "none":
        results["merged"] = None
    else:
        results["merged"] = connection.fetch_all(return_type, f"SELECT * FROM {merged_table}")

    return results


# (deprecated)
# def shopping_page(
#         query: str | Iterable[str],
#         connection: DuckDBConnection | None = None,
#         tables: dict | None = None,
#         how: Literal["sync","async","async_loop"] = "sync",
#         max_concurrent: int = 3,
#         max_retries: int = 5,
#         request_delay: float | int = 1.01,
#         progress: bool = True,
#         return_type: Literal["csv","json","parquet","raw","none"] = "json",
#         extract_options: dict = dict(),
#         transform_options: dict = dict(),
#     ) -> JsonObject:
#     """`tables = {'default': 'data'}`"""
#     # from linkmerce.core.naver.main.search.extract import ShoppingProduct
#     # from linkmerce.core.naver.main.search.transform import ShoppingProduct
#     return run_with_duckdb(
#         module = get_module(".search"),
#         extractor = "ShoppingPage",
#         transformer = "ShoppingPage",
#         connection = connection,
#         tables = tables,
#         how = how,
#         return_type = return_type,
#         args = (query,),
#         extract_options = update_options(
#             extract_options,
#             options = get_options(max_concurrent, max_retries, request_delay, progress),
#         ),
#         transform_options = transform_options,
#     )
