from __future__ import annotations
from linkmerce.core.ecount.api import EcountApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    import datetime as dt


class Inventory(EcountApi):
    """이카운트 재고현황을 조회하는 클래스.

    - **Menu**: 재고 I > 출력물 > 재고현황
    - **API**: https://oapi{ZONE}.ecount.com/OAPI/V2/InventoryBalance/GetListInventoryBalanceStatus
    - **Docs**: https://sboapi.ecount.com/ECERP/OAPI/OAPIView?lan_type=ko-KR
    - **Referer**: https://loginad.ecount.com/.../view/erp?ec_req_sid=...&

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    com_code: int | str
        이카운트 회사코드
    userid: str
        이카운트 아이디
    api_key: str
        오픈 API 인증키
    """

    method = "POST"
    path = "/InventoryBalance/GetListInventoryBalanceStatus"
    date_format = "%Y%m%d"

    @EcountApi.with_session
    @EcountApi.with_oapi
    def extract(
            self,
            base_date: dt.date | str | Literal[":today:"] = ":today:",
            warehouse_code: str | None = None,
            product_code: str | None = None,
            zero_yn: bool = False,
            balanced_yn: bool = False,
            deleted_yn: bool = False,
            safe_yn: bool = False,
            **kwargs
        ) -> dict:
        """재고현황을 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        base_date: dt.date | str
            조회 기준일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":today:"`: 오늘 날짜 (기본값)
        warehouse_code: str | None
            조회할 창고 코드. 생략 시 전체 창고를 조회한다.
        product_code: str | None
            조회할 품목 코드. 생략 시 모든 품목을 조회한다.
        zero_yn: bool
            재고 수량이 0인 품목 포함 여부. 기본값은 `False`
        balanced_yn: bool
            수량 관리 제외 품목 포함 여부. 기본값은 `False`
        deleted_yn: bool
            사용 중단 품목 포함 여부. 기본값은 `False`
        safe_yn: bool
            안전 재고 설정 미만 표시 여부. 기본값은 `False`

        Returns
        -------
        dict
            재고현황 조회 결과
        """
        if base_date == ":today:":
            import datetime as dt
            base_date = dt.date.today()
        message = self.build_request_message(
            base_date=base_date, warehouse_code=warehouse_code, product_code=product_code,
            zero_yn=zero_yn, balanced_yn=balanced_yn, deleted_yn=deleted_yn, safe_yn=safe_yn)
        with self.request(**message) as response:
            return self.parse(response.json(), **kwargs)

    def build_request_json(
            self,
            base_date: dt.date | str,
            warehouse_code: str | None = None,
            product_code: str | None = None,
            zero_yn: bool = True,
            balanced_yn: bool = False,
            deleted_yn: bool = False,
            safe_yn: bool = False,
            **kwargs
        ) -> dict[str, str]:
        return {
            "SESSION_ID": self.session_id,
            "BASE_DATE": str(base_date).replace('-', ''),
            **({"WH_CD": warehouse_code} if warehouse_code else dict()),
            **({"PROD_CD": product_code} if product_code else dict()),
            "ZERO_FLAG": ('Y' if zero_yn else 'N'),
            "BAL_FLAG": ('Y' if balanced_yn else 'N'),
            "DEL_GUBUN": ('Y' if deleted_yn else 'N'),
            "SAFE_FLAG": ('Y' if safe_yn else 'N'),
        }
