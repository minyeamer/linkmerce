from __future__ import annotations

from linkmerce.parse import QueryParser
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.types import JsonObject


class ExposureDiagnosis(QueryParser):
    def check_errors(func):
        @functools.wraps(func)
        def wrapper(self: ExposureDiagnosis, response: JsonObject, *args, **kwargs):
            if isinstance(response, dict):
                if not response.get("code"):
                    return func(self, response, *args, **kwargs)
                else:
                    self.raise_request_error(response)
            else:
                self.raise_parse_error("Could not parse the HTTP response.")
        return wrapper

    def raise_request_error(self, response: dict):
        from linkmerce.exceptions import RequestError, UnauthorizedError
        msg = response.get("title") or response.get("message") or str()
        if (msg == "Forbidden") or ("권한이 없습니다." in msg) or ("인증이 만료됐습니다." in msg):
            raise UnauthorizedError(msg)
        else:
            raise RequestError(msg)

    @check_errors
    def parse(self, response: JsonObject, **kwargs) -> list[dict]:
        data = response["adList"]
        return self.select(data, self.make_query(**kwargs)) if data else list()

    def make_query(self, keyword: str, mobile: bool = True, is_own: bool | None = None, **kwargs) -> str:
        query = """
        SELECT
            '{{ keyword }}' AS keyword,
            rank AS displayRank,
            TRY_CAST(REGEXP_EXTRACT(imageUrl, '^https://shopping-phinf.pstatic.net/main_\\d+/(\\d+)', 1) AS INT64) AS nvMid,
            productTitle AS productName,
            isOwn,
            categoryNames AS wholeCategoryName,
            NULLIF(fmpBrand, '') AS mallName,
            NULLIF(fmpMaker, '') AS makerName,
            imageUrl,
            CAST(COALESCE(lowPrice, mobileLowPrice, NULL) AS INT64) AS salesPrice,
            {{ created_at }} AS createdAt
        FROM {{ table }}
        {{ where }}
        """
        created_at = self.curret_datetime()
        where = "WHERE isOwn = {}".format(str(is_own).upper()) if isinstance(is_own, bool) else str()
        return self.render_query(query, keyword=keyword, mobile=mobile, created_at=created_at, where=where)


class ExposureRank(ExposureDiagnosis):
    def make_query(self, keyword: str, mobile: bool = True, is_own: bool | None = None, **kwargs) -> str:
        query = """
        SELECT * EXCLUDE (isOwn)
        FROM (
            SELECT
                TRY_CAST(REGEXP_EXTRACT(imageUrl, '^https://shopping-phinf.pstatic.net/main_\\d+/(\\d+)', 1) AS INT64) AS nvMid,
                '{{ keyword }}' AS keyword,
                productTitle AS productName,
                ROW_NUMBER() OVER () AS displayRank,
                {{ created_at }} AS createdAt,
                isOwn
            FROM {{ table }}
        ) WHERE nvMid IS NOT NULL {{ is_own }}
        """
        created_at = self.curret_datetime()
        is_own = "AND isOwn = {}".format(str(is_own).upper()) if isinstance(is_own, bool) else str()
        return self.render_query(query, keyword=keyword, mobile=mobile, created_at=created_at, is_own=is_own)
