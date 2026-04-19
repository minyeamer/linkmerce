from __future__ import annotations
from linkmerce.core.searchad.center import SearchAdCenter

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject


class ExposureDiagnosis(SearchAdCenter):
    """네이버 검색광고 키워드 노출 진단 데이터를 추출하는 클래스.

    - **Menu**: 도구 > 광고 노출 진단
    - **API URL**: `GET` https://ads.naver.com/apis/sa/api/ncc/sam/exposure-status-shopping

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    path = "/ncc/sam/exposure-status-shopping"

    @property
    def default_options(self) -> dict:
        return {"RequestEach": {"request_delay": 1}}

    @SearchAdCenter.with_session
    def extract(
            self,
            keyword: str | Iterable[str],
            domain: Literal["search", "shopping"] = "search",
            mobile: bool = True,
            is_own: bool | None = None,
            **kwargs
        ) -> JsonObject:
        """키워드(`keyword`)별 노출 진단 데이터를 조회해 JSON 형식으로 반환한다."""
        return (self.request_each(self.request_json_safe)
                .partial(domain=domain, mobile=mobile, is_own=is_own)
                .expand(keyword=keyword)
                .run())

    def build_request_params(
            self,
            keyword: str,
            domain: Literal["search", "shopping"] = "search",
            mobile: bool = True,
            ageTarget: int = 11,
            genderTarget: str = 'U',
            regionalCode: int = 99,
            **kwargs
        ) -> dict:
        return {
            "keyword": str(keyword).upper(),
            "media": int(str(["search", "shopping"].index(domain))+str(int(mobile)),2),
            "ageTarget": int(ageTarget),
            "genderTarget": genderTarget,
            "regionalCode": int(regionalCode),
        }

    def set_request_headers(self, **kwargs: str):
        referer = f"{self.origin}/manage/ad-accounts/{self.account_no}/sa/tool/exposure-status"
        super().set_request_headers(referer=referer, **kwargs)
