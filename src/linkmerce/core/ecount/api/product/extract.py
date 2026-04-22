from __future__ import annotations
from linkmerce.core.ecount.api import EcountApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import JsonObject


class Product(EcountApi):
    """이카운트 품목등록 리스트를 조회하는 클래스.

    - **Menu**: 재고 I > 기초등록 > 품목등록
    - **API**: https://oapi{ZONE}.ecount.com/OAPI/V2/InventoryBasic/GetBasicProductsList
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
    path = "/InventoryBasic/GetBasicProductsList"

    @EcountApi.with_session
    @EcountApi.with_oapi
    def extract(self, product_code: str | None = None, comma_yn: bool = True, **kwargs) -> JsonObject:
        """품목등록 리스트를 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        product_code: str | None
            검색할 품목코드. 생략 시 모든 품목을 조회한다. 기본값은 `None`
        comma_yn: bool
            쉼표 사용 여부. 기본값은 `True`

        Returns
        -------
        dict
            품목등록 리스트
        """
        message = self.build_request_message(product_code=product_code, comma_yn=comma_yn)
        with self.request(**message) as response:
            return self.parse(response.json(), **kwargs)

    def build_request_json(self, product_code: str | None = None, comma_yn: bool = True, **kwargs) -> dict:
        return {
            "SESSION_ID": self.session_id,
            **({"PROD_CD": product_code} if product_code else dict()),
            "COMMA_FLAG": ('Y' if comma_yn else 'N'),
        }
