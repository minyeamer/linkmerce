from __future__ import annotations

from linkmerce.core.ebay.esmplus import EsmPlus

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Sequence


class Item(EsmPlus):
    """ESM PLUS 상품목록을 조회하는 클래스.

    - **Menu**: 상품 관리 > 상품 조회/수정 > 상품목록
    - **API**: https://item.esmplus.com/api/ea/goods/search
    - **Referer**: https://www.esmplus.com/Home/v2/goods-manage

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        페이지 요청 간 대기 시간(초). 기본값은 `1`
    tqdm_options: dict | None
        페이지 순회 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    subdomain = "item"
    path = "/ea/goods/search"
    max_page_size = 500
    page_start = 1
    default_options = {
        "PaginateAll": {"request_delay": 1},
        "RequestEachPages": {"request_delay": 1},
    }

    @EsmPlus.with_session
    def extract(
            self,
            product_id: str | Sequence[str] = str(),
            keyword: str = str(),
            sell_status: Sequence[str] = list(),
            **kwargs,
        ) -> list[dict]:
        """상품목록을 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        product_id: str | Iterable[str]
            검색할 상품번호/마스터상품번호/판매자관리코드/SKU번호/그룹 번호. 문자열 또는 배열을 입력할 수 있다.
        keyword: str
            검색할 상품명, 브랜드명, 제조사명을 입력할 수 있다.
        sell_status: list[str]
            판매상태 목록
                - `"11"`: 판매가능
                - `"21"`: 판매불가
                - `"22"`: 판매중지
                - `"31"`: SKU품절
                - `"01"`: 등록대기

        Returns
        -------
        list[dict]
            상품목록 조회 결과
        """
        return (self.paginate_all(
                    self.request_json,
                    counter = self.count_total,
                    max_page_size = self.max_page_size,
                    page_start = self.page_start
                ).run(
                    product_id = (product_id if isinstance(product_id, str) else ','.join(product_id)),
                    keyword = keyword,
                    sell_status = list(sell_status),
                    **kwargs
                ))

    def count_total(self, response: dict, **kwargs) -> int:
        """HTTP 응답에서 전체 행 수를 추출한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(response, "data.totalCount")

    def build_request_json(
            self,
            product_id: str = str(),
            keyword: str = str(),
            sell_status: list[str] = list(),
            page: int = 1,
            page_size: int = 500,
            **kwargs,
        ) -> dict:
        return {
            "query": {
                "goodsIds": product_id,
                "keyword": keyword,
                "sellStatus": sell_status,
                "category": {},
                "registrationDate": {},
                "shipping": {},
                "additionalService": [],
            },
            "pageIndex": page,
            "pageSize": page_size,
            "sortField": 0,
            "sortOrder": 1,
        }

    def build_request_headers(self, **kwargs) -> dict[str, str]:
        return self.get_request_headers() | {
            "referer": (self.origin + "/goods/list"),
        }

    @property
    def sell_status(self) -> dict[str, str]:
        """판매상태 코드와 한글명 매핑을 반환한다."""
        return {
            "11": "판매가능", "21": "판매불가", "22": "판매중지", "31": "SKU품절", "01": "등록대기"
        }
