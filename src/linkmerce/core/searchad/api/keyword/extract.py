from __future__ import annotations
from linkmerce.core.searchad.api import NaverSearchAdApi

from typing import Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import JsonObject


class Keyword(NaverSearchAdApi):
    """검색광고 API로 키워드 도구의 연관키워드 조회 결과를 수집하는 클래스.

    - **API URL**: `GET` https://api.searchad.naver.com/keywordstool
    - **API Docs**: https://naver.github.io/searchad-apidoc/#/tags/Tool

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """
    method = "GET"
    uri = "/keywordstool"

    @property
    def default_options(self) -> dict:
        return {"RequestEach": {"request_delay": 1}}

    @NaverSearchAdApi.with_session
    def extract(
            self,
            keywords: str | Iterable[str],
            max_rank: int | None = None,
            show_detail: bool = True,
            **kwargs
        ) -> JsonObject:
        """키워드 도구의 연관키워드 조회 결과를 수집해 JSON 형식으로 반환한다."""
        return (self.request_each(self.request_json_safe)
                .partial(max_rank=max_rank, show_detail=show_detail)
                .expand(keywords=self.chunk_keywords(keywords))
                .run())

    def chunk_keywords(self, keywords: str | Iterable[str], n: int = 5) -> list[list[str]] | str:
        """키워드 목록을 n개씩 청크 단위로 분할한다."""
        if isinstance(keywords, str):
            return keywords
        elif isinstance(keywords, Iterable):
            return [keywords[i:i+n] for i in range(0, len(keywords), n)]
        else:
            return list()

    def build_request_params(self, keywords: str | Iterable[str], show_detail: bool = True, **kwargs) -> dict:
        keywords = keywords if isinstance(keywords, str) else ','.join(keywords)
        return {"hintKeywords": keywords, "showDetail": int(show_detail)}
