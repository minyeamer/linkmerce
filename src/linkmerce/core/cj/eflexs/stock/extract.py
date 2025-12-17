from __future__ import annotations
from linkmerce.core.cj.eflexs import CJeFLEXs

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class Stock(CJeFLEXs):
    menu = "IMSI0002M"
    path = "/selectDtlStckSearch.do"
    date_format = "%Y%m%d"

    @property
    def default_options(self) -> dict:
        return dict(RequestEach = dict(request_delay=1))

    @CJeFLEXs.with_session
    @CJeFLEXs.with_auth_info
    def extract(
            self,
            customer_id: int | str | Iterable,
            start_date: dt.date | str | Literal[":last_week:"] = ":last_week:",
            end_date: dt.date | str | Literal[":start_date:",":today:"] = ":today:",
            **kwargs
        ) -> JsonObject:
        return (self.request_each(self.request_json)
                .partial(**self.set_date(start_date, end_date))
                .expand(customer_id=customer_id)
                .run())

    def set_date(self, start_date: dt.date | str, end_date: dt.date | str) -> dict[str,dt.date]:
        import datetime as dt

        if start_date == ":last_week:":
            start_date = dt.date.today() - dt.timedelta(days=7)

        if end_date == ":start_date:":
            end_date = start_date
        elif end_date == ":today:":
            end_date = dt.date.today()

        return dict(start_date=start_date, end_date=end_date)

    def build_request_data(
            self,
            customer_id: int | str,
            start_date: dt.date | str,
            end_date: dt.date | str,
            page: int = 0,
            page_size: int = 100000,
            **kwargs
        ) -> dict:
        return {
            "pgmId": self.menu,
            "requestDataIds": "dmMainParam",
            "@d1#strrId": str(customer_id),
            "@d1#oWhCd": None,
            "@d1#srchZoneCd": None,
            "@d1#srchZoneNm": None,
            "@d1#srchItemNm": None,
            "@d1#srchItemCd": None,
            "@d1#srchWcellNm": None,
            "@d1#srchWcellTcd": None,
            "@d1#srchLotNo": None,
            "@d1#srchItemRarcode": None,
            "@d1#srchHldScd": None,
            "@d1#fromCloseDate": str(start_date).replace('-', ''),
            "@d1#toCloseDate": str(end_date).replace('-', ''),
            "@d1#srchMallId": None,
            "@d1#page": page,
            "@d1#pageRow": page_size,
            "@d1#srchLotNo7": None,
            "@d1#srchLotNo10": None,
            "@d1#itemGcd": None,
            "@d#": "@d1#",
            "@d1#": "dmMainParam",
            "@d1#tp": "dm",
        }
