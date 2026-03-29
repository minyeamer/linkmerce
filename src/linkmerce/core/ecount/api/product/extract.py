from __future__ import annotations
from linkmerce.core.ecount.api import EcountApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import JsonObject


class Product(EcountApi):
    """이카운트 품목 리스트를 조회하는 클래스."""

    method = "POST"
    path = "/InventoryBasic/GetBasicProductsList"

    @EcountApi.with_session
    @EcountApi.with_oapi
    def extract(self, product_code: str | None = None, comma_yn: bool = False, **kwargs) -> JsonObject:
        """품목 리스트를 조회해 JSON 형식으로 반환한다."""
        message = self.build_request_message(product_code=product_code, comma_yn=comma_yn)
        with self.request(**message) as response:
            return self.parse(response.json(), **kwargs)

    def build_request_json(self, product_code: str | None = None, comma_yn: bool = True, **kwargs) -> dict:
        return {
            "SESSION_ID": self.session_id,
            **({"PROD_CD": product_code} if product_code else dict()),
            "COMMA_FLAG": ('Y' if comma_yn else 'N'),
        }
