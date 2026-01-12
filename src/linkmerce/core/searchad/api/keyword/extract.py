from __future__ import annotations
from linkmerce.core.searchad.api import NaverSearchAdAPI

from typing import Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import JsonObject


class Keyword(NaverSearchAdAPI):
    method = "GET"
    uri = "/keywordstool"

    @property
    def default_options(self) -> dict:
        return dict(RequestEach = dict(request_delay=1))

    @NaverSearchAdAPI.with_session
    def extract(
            self,
            keywords: str | Iterable[str],
            max_rank: int | None = None,
            show_detail: bool = True,
            **kwargs
        ) -> JsonObject:
        return (self.request_each(self.request_json_safe)
                .partial(max_rank=max_rank, show_detail=show_detail)
                .expand(keywords=self.chunk_keywords(keywords))
                .run())

    def chunk_keywords(self, keywords: str | Iterable[str], n: int = 5) -> list[list[str]] | str:
        if isinstance(keywords, str):
            return keywords
        elif isinstance(keywords, Iterable):
            return [keywords[i:i+n] for i in range(0, len(keywords), n)]
        else:
            return list()

    def build_request_params(self, keywords: str | Iterable[str], show_detail: bool = True, **kwargs) -> dict:
        keywords = keywords if isinstance(keywords, str) else ','.join(keywords)
        return dict(hintKeywords=keywords, showDetail=int(show_detail))
