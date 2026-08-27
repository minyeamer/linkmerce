from __future__ import annotations
from linkmerce.core.cj.eflexs import CjEflexs

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable


class Stock(CjEflexs):
    """CJ대한통운 eFLEXs 상세재고조회 메뉴의 재고 내역을 조회하는 클래스.

    - **Menu**: 재고관리 > 재고조회 > 상세재고조회 (`IMSI0002M`)
    - **API**: https://b2c-api.cjlogistics.com/api/v1/inventory/detail
    - **Referer**: https://eflexs-x.cjlogistics.com/

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    userid: str
        CJ eFLEXs 로그인을 위한 User ID
    passwd: str
        CJ eFLEXs 로그인을 위한 Password
    mail_info: dict[str, str]
        2단계 인증을 위한 이메일 정보. 다음 키값을 포함해야 한다.
            - `origin`: 메일 서비스 도메인.
            - `email`: 메일 계정 아이디.
            - `passwd`: 메일 계정 비밀번호.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        고객별 요청 간 대기 시간(초). 기본값은 `1`
    tqdm_options: dict | None
        반복 요청 작업 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    path = "/inventory/detail"
    date_format = "%Y%m%d"
    default_options = {"RequestEach": {"request_delay": 1}}

    @CjEflexs.with_session
    @CjEflexs.with_token
    def extract(self, customer_id: int | str | Iterable[int | str], **kwargs) -> dict | list[dict]:
        """상세재고조회 화면에서 고객별 재고 내역을 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        customer_id: int | str | Iterable[int | str]
            조회할 고객 ID. 단일 값 또는 배열을 입력한다.

        Returns
        -------
        dict | list[dict]
            고객별 재고 내역. `customer_id` 타입에 따라 반환 타입이 다르다.
                - `customer_id`가 `int | str` 타입일 때 -> `dict`
                - `customer_id`가 `Iterable[int | str]` 타입일 때 -> `list[dict]`
        """
        return (self.request_each(self.request_json)
                .expand(customer_id=customer_id)
                .run())

    def build_request_params(
            self,
            customer_id: int | str,
            page_start: int = 0,
            page_end: int = 100001,
            **kwargs
        ) -> dict:
        return {"pageS": page_start, "pageE": page_end, "strrId": customer_id}
