from __future__ import annotations
from linkmerce.core.searchad.api import NaverSearchAdApi

from typing import Iterable


class Keyword(NaverSearchAdApi):
    """검색광고 API로 키워드 도구의 연관키워드 조회 결과를 수집하는 클래스.

    - **Menu**: 도구 > 키워드 도구 > 연관키워드 조회 결과
    - **API**: https://api.searchad.naver.com/keywordstool
    - **Docs**: https://naver.github.io/searchad-apidoc/#/tags/RelKwdStat
    - **Referer**: https://ads.naver.com/manage/ad-accounts/{account_no}/sa/tool/keyword-planner

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간(초). 기본값은 `1`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    uri = "/keywordstool"
    default_options = {"RequestEach": {"request_delay": 1}}

    @NaverSearchAdApi.with_session
    def extract(
            self,
            keywords: str | Iterable[str],
            max_rank: int | None = None,
            show_detail: bool = True,
            **kwargs
        ) -> dict | list[dict]:
        """키워드 도구의 연관키워드 조회 결과를 수집해 JSON 형식으로 반환한다.

        Parameters
        ----------
        keywords: str | Iterable[str]
            키워드. 문자열 또는 문자열의 배열을 입력한다. 키워드는 5개씩 묶어서 조회한다.
        max_rank: int | None
            최대 순위. 조회 시점에는 사용되지 않고 파서 함수에 전달된다.
        show_detail: bool
            - `True`: 연관키워드의 상세 통계 정보를 조회한다. (기본값)
            - `False`: 연관키워드의 월간검색수만 조회한다.

        Returns
        -------
        dict | list[dict]
            연관키워드 조회 결과. `keywords` 타입에 따라 반환 타입이 다르다.
                - `keywords`가 `str` 타입일 때 -> `dict`
                - `keywords`가 `Iterable[str]` 타입일 때 -> `list[dict]`
        """
        return (self.request_each(self.request_json_safe)
                .partial(max_rank=max_rank, show_detail=show_detail)
                .expand(keywords=self.chunk_keywords(keywords))
                .run())

    def chunk_keywords(self, keywords: str | Iterable[str], n: int = 5) -> str | list[list[str]]:
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
