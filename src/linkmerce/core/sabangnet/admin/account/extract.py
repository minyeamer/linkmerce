from __future__ import annotations
from linkmerce.core.sabangnet.admin import SabangnetAdmin

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal


class Account(SabangnetAdmin):
    """사방넷 쇼핑몰로그인 메뉴의 쇼핑몰 계정 목록을 페이지 단위로 조회하는 클래스.

    - **Menu**: 계정관리 > 쇼핑몰로그인
    - **API**: https://sbadmin{domain}.sabangnet.co.kr/prod-api/customer/account/mall-login/auto
    - **Referer**: https://sbadmin{domain}.sabangnet.co.kr/#/account/mall-login

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    userid: str
        사방넷 아이디
    passwd: str
        사방넷 비밀번호
    domain: int
        사방넷 시스템 도메인 번호

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        페이지 요청 간 대기 시간(초). 기본값은 `0.3`
    tqdm_options: dict | None
        페이지 순회 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    path = "/prod-api/customer/account/mall-login/{}"
    max_page_size = 500
    page_start = 1
    default_options = {"PaginateAll": {"request_delay": 0.3}}

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(self, region_type: Literal["auto", "global"] = "auto", **kwargs) -> list[dict]:
        """쇼핑몰로그인 화면에서 검색 유형에 대한 쇼핑몰 계정 목록을 페이지 단위로 조회한다.

        Parameters
        ----------
        region_type: str
            쇼핑몰 검색 유형
                - `"auto"`: 국내 쇼핑몰 (기본값)
                - `"global"`: 해외 쇼핑몰

        Returns
        -------
        list[dict]
            쇼핑몰 계정 목록
        """
        url = self.concat_path(self.origin, self.path.format(region_type))
        return (self.paginate_all(
                    self.request_json,
                    counter = self.count_total,
                    max_page_size = self.max_page_size,
                    page_start = self.page_start
                ).run(url = url))

    def count_total(self, response: dict, **kwargs) -> int:
        """HTTP 응답에서 전체 계정 개수를 추출한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(response, "data.0.total") or 0

    def build_request_json(self, page: int = 1, size: int = 500, **kwargs) -> dict:
        return {
            "currentPage": page,
            "pageSize": size,
            "sortOption": "001",
            "sort": "ASC",
            "searchCondition": None,
            "searchKeyword": None,
        }


class ShopNormal(SabangnetAdmin):
    """사방넷 쇼핑몰관리(일반) 메뉴의 일반 쇼핑몰 목록을 페이지 단위로 조회하는 클래스.

    - **Menu**: 계정관리 > 쇼핑몰관리(일반)
    - **API**: https://sbadmin{domain}.sabangnet.co.kr/prod-api/customer/account/mall-normal
    - **Referer**: https://sbadmin{domain}.sabangnet.co.kr/#/account/mall-normal

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    userid: str
        사방넷 아이디
    passwd: str
        사방넷 비밀번호
    domain: int
        사방넷 시스템 도메인 번호

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        페이지 요청 간 대기 시간(초). 기본값은 `0.3`
    tqdm_options: dict | None
        페이지 순회 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    path = "/prod-api/customer/account/mall-normal"
    max_page_size = 500
    page_start = 1
    default_options = {"PaginateAll": {"request_delay": 0.3}}

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(self, **kwargs) -> list[dict]:
        """쇼핑몰관리(일반) 화면에서 일반 쇼핑몰 목록을 페이지 단위로 조회한다.

        Returns
        -------
        list[dict]
            일반 쇼핑몰 목록
        """
        return (self.paginate_all(
                    self.request_json,
                    counter = self.count_total,
                    max_page_size = self.max_page_size,
                    page_start = self.page_start
                ).run())

    def count_total(self, response: dict, **kwargs) -> int:
        """HTTP 응답에서 전체 계정 개수를 추출한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(response, "data.0.total") or 0

    def build_request_json(self, page: int = 1, size: int = 500, **kwargs) -> dict:
        return {
            "currentPage": page,
            "pageSize": size,
            "sortOption": "001",
            "sort": "DESC",
            "searchCondition": None,
            "searchKeyword": "",
            # "useYn": "Y",
            "sortSeq": "shmaId",
            "sortMethod": "ASC",
        }


class AccountNormal(SabangnetAdmin):
    """사방넷 쇼핑몰 정보 보기 팝업의 일반 쇼핑몰 정보를 페이지 단위로 조회하는 클래스.

    - **Menu**: 계정관리 > 쇼핑몰관리(일반) > 쇼핑몰 정보 보기
    - **API**: https://sbadmin{domain}.sabangnet.co.kr/prod-api/customer/account/mall-normal/mall-normal-info-popup/id-setting
    - **Referer**: https://sbadmin{domain}.sabangnet.co.kr/#/popup/views/pages/account/mall-normal-popup/mall-normal-info-popup.vue

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    userid: str
        사방넷 아이디
    passwd: str
        사방넷 비밀번호
    domain: int
        사방넷 시스템 도메인 번호

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        쇼핑몰코드별 요청 간 대기 시간(초). 기본값은 `0.3`
    tqdm_options: dict | None
        반복 요청 작업 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    path = "/prod-api/customer/account/mall-normal/mall-normal-info-popup/id-setting"
    default_options = {"RequestEach": {"request_delay": 0.3}}

    @SabangnetAdmin.with_session
    @SabangnetAdmin.with_token
    def extract(self, shop_id: str | Iterable[str], **kwargs) -> list[dict]:
        """쇼핑몰 정보 보기 팝업에서 일반 쇼핑몰 정보를 조회한다.

        Parameters
        ----------
        shop_id: str | Iterable[str]
            쇼핑몰코드. 문자열 또는 문자열의 배열을 입력한다.

        Returns
        -------
        list[dict]
            일반 쇼핑몰 목록
        """
        return (self.request_each(self.request_json)
                .expand(shop_id=shop_id)
                .run())

    def count_total(self, response: dict, **kwargs) -> int:
        """HTTP 응답에서 전체 계정 개수를 추출한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(response, "data.0.total") or 0

    def build_request_json(self, shop_id: str, **kwargs) -> dict:
        return {"shmaId": shop_id}
