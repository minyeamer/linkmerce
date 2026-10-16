from __future__ import annotations
from linkmerce.core.cj.eflexs import CjEflexs

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable


class Invoice(CjEflexs):
    """CJ대한통운 eFLEXs 고객매출확인 메뉴의 운송장 상세 내역을 조회하는 클래스.

    - **Menu**: 정산관리 > 현황 > 고객매출확인 > B2C출고(운송장)상세 (`BLST0002M`)
    - **API**: https://b2c-api.cjlogistics.com/api/v1/billing/status/customer-sales/tabs/03
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
            - `origin`: 메일 서비스 도메인
            - `email`: 메일 계정 아이디
            - `passwd`: 메일 계정 비밀번호

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        고객 및 정산년월별 요청 간 대기 시간(초). 기본값은 `1`
    tqdm_options: dict | None
        반복 요청 작업 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    path = "/billing/status/customer-sales/tabs/03"
    default_options = {"RequestEach": {"request_delay": 1}}

    @CjEflexs.with_session
    @CjEflexs.with_token
    def extract(
            self,
            customer_id: int | str | Iterable[int | str],
            bill_ym: int | str | Iterable[int | str],
        ) -> dict | list[dict]:
        """고객 및 정산년월별 운송장 상세 내역을 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        customer_id: int | str | Iterable[int | str]
            조회할 고객 ID. 단일 값 또는 배열을 입력한다.
        bill_ym: int | str | Iterable[int | str]
            정산년월. `YYYYMM` 또는 `YYYY-MM` 형식의 단일 값 또는 배열을 입력한다.

        Returns
        -------
        dict | list[dict]
            고객 및 정산년월별 운송장 상세 내역
                - 두 인자가 모두 `int | str` 타입이면 `dict`를 반환한다.
                - 하나 이상의 인자가 배열이면 모든 고객 및 정산년월 조합의 `list[dict]`를 반환한다.
        """
        return (self.request_each(self.request_json)
                .expand(customer_id=customer_id, bill_ym=bill_ym)
                .run())

    def build_request_params(
            self,
            customer_id: int | str,
            bill_ym: int | str,
            **kwargs
        ) -> dict:
        return {
            "strrId": str(customer_id),
            "billYm": str(bill_ym).replace('-', ''),
        }
