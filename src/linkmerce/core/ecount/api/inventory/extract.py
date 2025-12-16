from __future__ import annotations
from linkmerce.core.ecount.api import EcountAPI

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class Inventory(EcountAPI):
    method = "POST"
    path = "/InventoryBalance/GetListInventoryBalanceStatus"
    date_format = "%Y%m%d"

    @EcountAPI.with_session
    @EcountAPI.with_oapi
    def extract(self,
            base_date: dt.date | str | Literal[":today:"] = ":today:",
            warehouse_code: str | None = None,
            product_code: str | None = None,
            zero_yn: bool = False,
            balanced_yn: bool = False,
            deleted_yn: bool = False,
            safe_yn: bool = False,
            **kwargs
        ) -> JsonObject:
        if base_date == ":today:":
            import datetime as dt
            base_date = dt.date.today()
        message = self.build_request_message(
            base_date=base_date, warehouse_code=warehouse_code, product_code=product_code,
            zero_yn=zero_yn, balanced_yn=balanced_yn, deleted_yn=deleted_yn, safe_yn=safe_yn)
        with self.request(**message) as response:
            return self.parse(response.json(), **kwargs)

    def build_request_json(self,
            base_date: dt.date | str,
            warehouse_code: str | None = None,
            product_code: str | None = None,
            zero_yn: bool = True,
            balanced_yn: bool = False,
            deleted_yn: bool = False,
            safe_yn: bool = False,
            **kwargs
        ) -> dict[str,str]:
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
