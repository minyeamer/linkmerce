from __future__ import annotations
from linkmerce.core.searchad.center import SearchAdCenter

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject


class ExposureDiagnosis(SearchAdCenter):
    """네이버 검색광고 `광고 노출 진단` 화면의 키워드별 노출 진단 결과를 조회하는 클래스.

    - **Menu**: 검색 광고 > 도구 > 광고 노출 진단 > 쇼핑검색 (노출현황보기)
    - **API**: https://ads.naver.com/apis/sa/api/ncc/sam/exposure-status-shopping
    - **Referer**: https://ads.naver.com/manage/ad-accounts/{account_no}/sa/tool/exposure-status

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    account_no: int | str
        검색광고 계정 번호
    customer_id: int | str
        검색광고 고객 ID

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `1`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    path = "/ncc/sam/exposure-status-shopping"
    default_options = {"RequestEach": {"request_delay": 1}}

    @SearchAdCenter.with_session
    def extract(
            self,
            keyword: str | Iterable[str],
            domain: Literal["search", "shopping"] = "search",
            mobile: bool = True,
            is_own: bool | None = None,
            **kwargs
        ) -> JsonObject:
        """키워드별 광고 노출 진단 결과를 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        keyword: str | Iterable[str]
            키워드 또는 키워드 목록
        domain: Literal["search", "shopping"]
            매체 구분. `mobile` 값과 조합해 "네이버 통합검색 모바일/PC" 또는 "네이버 쇼핑검색 모바일/PC" 탭을 선택한다.
                - `"search"`: 네이버 통합검색
                - `"shopping"`: 네이버 쇼핑검색
        mobile: bool
            기기 구분.
                - `True`: 모바일
                - `False`: PC
        is_own: bool | None
            소유 여부 필터. 조회 시점에는 사용되지 않고 파서 함수에 전달된다.
                - `True`: 내 광고만 반환
                - `False`: 내 광고를 제외하고 반환
                - `None`: 전체 광고 반환

        Returns
        -------
        dict | list[dict]
            키워드별 상위 100위 이내 광고 상품
        """
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
