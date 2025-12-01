from __future__ import annotations
from linkmerce.core.smartstore.api import SmartstoreAPI

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class MarketingChannel(SmartstoreAPI):
    method = "GET"
    version = "v1"
    path = "/bizdata-stats/channels/:channelNo/marketing/custom/detail"
    date_format = "%Y-%m-%d"

    @property
    def default_options(self) -> dict:
        return dict(RequestEach = dict(request_delay=1))

    @SmartstoreAPI.with_session
    @SmartstoreAPI.with_token
    def extract(
            self,
            channel_seq: int | str,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            max_retries: int = 5,
            **kwargs
        ) -> JsonObject:
        url = self.url.replace(":channelNo", str(channel_seq))
        return (self.request_each(self.request_json_until_success)
                .partial(url=url, channel_seq=channel_seq, max_retries=max_retries)
                .expand(date=self.generate_date_range(start_date, end_date, freq='D'))
                .run())

    def build_request_params(self, date: dt.date, **kwargs) -> dict:
        return {"startDate": str(date), "endDate": str(date)}
