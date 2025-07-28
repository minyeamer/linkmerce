from __future__ import annotations

from linkmerce.parse import QueryParser
import functools

from typing import Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import List
    from linkmerce.types import JsonObject


class ExposureDiagnosis(QueryParser):
    def check_errors(func):
        @functools.wraps(func)
        def wrapper(self: ExposureDiagnosis, response: JsonObject, *args, **kwargs):
            if isinstance(response, Dict):
                if not response.get("code"):
                    return func(self, response, *args, **kwargs)
                else:
                    self.raise_request_error(response)
            else:
                self.raise_parse_error("Could not parse the HTTP response.")
        return wrapper

    def raise_request_error(self, response: Dict):
        from linkmerce.exceptions import RequestError, UnauthorizedError
        msg = response.get("title") or response.get("message") or str()
        if (msg == "Forbidden") or ("권한이 없습니다." in msg) or ("인증이 만료됐습니다." in msg):
            raise UnauthorizedError(msg)
        else:
            raise RequestError(msg)

    @check_errors
    def parse(self, response: JsonObject, **kwargs) -> List[Dict]:
        data = response["adList"]
        return self.select(data, self.make_query(**kwargs)) if data else list()

    def make_query(self, keyword: str, mobile: bool = True, is_own: bool | None = None, **kwargs) -> str:
        query = """
        SELECT
            '{{ keyword }}' AS keyword,
            ROW_NUMBER() OVER () AS rank,
            productTitle AS productName,
            isOwn AS isOwn,
            categoryNames AS wholeCategoryName,
            NULLIF(fmpBrand, '') AS mallName,
            NULLIF(fmpMaker, '') AS makerName,
            CAST(COALESCE(lowPrice, mobileLowPrice, NULL) AS INT64) AS salesPrice
        FROM {{ table }}
        {{ where }}
        """
        where = "WHERE isOwn = {}".format(str(is_own).upper()) if isinstance(is_own, bool) else str()
        return self.render_query(query, keyword=keyword, mobile=mobile, where=where)
