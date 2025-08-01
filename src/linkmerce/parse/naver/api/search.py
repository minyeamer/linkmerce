from __future__ import annotations

from linkmerce.parse import QueryParser
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.types import JsonObject


class _SearchParser(QueryParser):
    content_type: Literal["blog","news","book","adult","encyc","cafearticle","kin","local","errata","webkr","image","shop","doc"]
    rank_start: int = 1

    def check_errors(func):
        @functools.wraps(func)
        def wrapper(self: _SearchParser, response: JsonObject, *args, **kwargs):
            if isinstance(response, dict):
                if "errorMessage" not in response:
                    return func(self, response, *args, **kwargs)
                else:
                    self.raise_request_error(response)
            else:
                self.raise_parse_error("Could not parse the HTTP response.")
        return wrapper

    def raise_request_error(self, response: dict):
        from linkmerce.exceptions import RequestError
        raise RequestError(response.get("errorMessage") or str())

    @check_errors
    def parse(self, response: JsonObject, query: str, start: int = 1, **kwargs) -> list[dict]:
        data = response["items"]
        start = (start-1) + (self.rank_start-1)
        return self.select(data, self.make_query(query, start)) if data else list()


class BlogSearch(_SearchParser):
    content_type = "blog"

    def make_query(self, keyword: str, start: int, **kwargs) -> str:
        query = """
        SELECT
            '{{ keyword }}' AS keyword,
            (ROW_NUMBER() OVER () + {{ start }}) AS rank,
            title,
            link AS url,
            description,
            bloggername AS address,
            bloggerlink AS bloggerUrl,
            TRY_CAST(TRY_STRPTIME(postdate, '%Y%m%d') AS DATE) AS postDate
        FROM {{ table }}
        """
        return self.render_query(query, keyword=keyword, start=start)


class NewsSearch(_SearchParser):
    content_type = "news"

    def make_query(self, keyword: str, start: int, **kwargs) -> str:
        query = """
        SELECT
            '{{ keyword }}' AS keyword,
            (ROW_NUMBER() OVER () + {{ start }}) AS rank,
            title,
            originallink AS url,
            description,
            TRY_CAST(TRY_STRPTIME(pubDate, '%a, %d %b %Y %H:%M:%S %z') AS TIMESTAMP) AS postDate
        FROM {{ table }}
        """
        return self.render_query(query, keyword=keyword, start=start)


class BookSearch(_SearchParser):
    content_type = "book"

    def make_query(self, keyword: str, start: int, **kwargs) -> str:
        query = """
        SELECT
            '{{ keyword }}' AS keyword,
            (ROW_NUMBER() OVER () + {{ start }}) AS rank,
            title,
            link AS url,
            description,
            image AS imageUrl,
            author AS author,
            TRY_CAST(discount AS INT64) AS salesPrice,
            publisher AS publisher,
            TRY_CAST(isbn AS INT64) AS isbn,
            TRY_CAST(TRY_STRPTIME(pubdate, '%Y%m%d') AS DATE) AS publishDate
        FROM {{ table }}
        """
        return self.render_query(query, keyword=keyword, start=start)


class CafeSearch(_SearchParser):
    content_type = "cafe"

    def make_query(self, keyword: str, start: int, **kwargs) -> str:
        query = """
        SELECT
            '{{ keyword }}' AS keyword,
            (ROW_NUMBER() OVER () + {{ start }}) AS rank,
            title,
            link AS url,
            description,
            cafename AS address,
            cafeurl AS cafeUrl
        FROM {{ table }}
        """
        return self.render_query(query, keyword=keyword, start=start)


class KiNSearch(_SearchParser):
    content_type = "kin"

    def make_query(self, keyword: str, start: int, **kwargs) -> str:
        query = """
        SELECT
            '{{ keyword }}' AS keyword,
            (ROW_NUMBER() OVER () + {{ start }}) AS rank,
            title,
            link AS url,
            description
        FROM {{ table }}
        """
        return self.render_query(query, keyword=keyword, start=start)


class ImageSearch(_SearchParser):
    content_type = "image"

    def make_query(self, keyword: str, start: int, **kwargs) -> str:
        query = """
        SELECT
            '{{ keyword }}' AS keyword,
            (ROW_NUMBER() OVER () + {{ start }}) AS rank,
            title,
            link AS url,
            thumbnail,
            TRY_CAST(sizeheight AS INT64) AS sizeheight,
            TRY_CAST(sizewidth AS INT64) AS sizewidth,
        FROM {{ table }}
        """
        return self.render_query(query, keyword=keyword, start=start)


class ShoppingSearch(_SearchParser):
    content_type = "shop"

    def make_query(self, keyword: str, start: int, **kwargs) -> str:
        query = """
        SELECT
            '{{ keyword }}' AS keyword,
            (ROW_NUMBER() OVER () + {{ start }}) AS displayRank,
            productId AS nvMid,
            TRY_CAST(REGEXP_EXTRACT(link, '/products/(\\d+)$', 1) AS INT64) AS mallPid,
            title AS productName,
            ((TRY_CAST(productType AS INT1) + 2) % 3) AS productType,
            NULLIF(mallName, '네이버') AS mallName,
            IF(link LIKE '%/catalog/%', link, NULL) AS nvMurl,
            IF(link LIKE '%/catalog/%', NULL, link) AS mallPurl,
            brand AS brandName,
            maker AS makerName,
            category1 AS categoryName1,
            category2 AS categoryName2,
            category3 AS categoryName3,
            category4 AS categoryName4,
            image AS imageUrl,
            TRY_CAST(lprice AS INT64) AS salesPrice,
            {{ created_at }} AS createdAt
        FROM {{ table }}
        """
        created_at = self.curret_datetime()
        return self.render_query(query, keyword=keyword, start=start, created_at=created_at)


class ShoppingRank(_SearchParser):
    content_type = "shop"

    def make_query(self, keyword: str, start: int, **kwargs) -> str:
        query = """
        SELECT
            productId AS nvMid,
            TRY_CAST(REGEXP_EXTRACT(link, '/products/(\\d+)$', 1) AS INT64) AS mallPid,
            '{{ keyword }}' AS keyword,
            ((TRY_CAST(productType AS INT1) + 2) % 3) AS productType,
            (ROW_NUMBER() OVER () + {{ start }}) AS displayRank,
            {{ created_at }} AS createdAt
        FROM {{ table }}
        WHERE productId IS NOT NULL
        """
        created_at = self.curret_datetime()
        return self.render_query(query, keyword=keyword, start=start, created_at=created_at)
