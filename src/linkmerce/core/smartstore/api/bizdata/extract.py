from __future__ import annotations
from linkmerce.core.smartstore.api import SmartstoreApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class MarketingChannel(SmartstoreApi):
    """네이버 커머스 API로 상품/마케팅 채널 데이터를 요청하는 클래스.

    `RequestEach` Task를 사용하여 일별 채널 데이터를 조회한다."""

    method = "GET"
    version = "v1"
    path = "/bizdata-stats/channels/:channelNo/marketing/custom/detail"
    date_format = "%Y-%m-%d"

    @property
    def default_options(self) -> dict:
        return {"RequestEach": {"request_delay": 1}}

    @SmartstoreApi.with_session
    @SmartstoreApi.with_token
    def extract(
            self,
            channel_seq: int | str,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            max_retries: int = 5,
            **kwargs
        ) -> JsonObject:
        """일별 상품/마케팅 채널 데이터를 조회해 JSON 형식으로 반환한다."""
        url = self.url.replace(":channelNo", str(channel_seq))
        return (self.request_each(self.request_json_until_success)
                .partial(url=url, channel_seq=channel_seq, max_retries=max_retries)
                .expand(date=self.generate_date_range(start_date, end_date, freq='D'))
                .run())

    def build_request_params(self, date: dt.date, **kwargs) -> dict:
        return {"startDate": str(date), "endDate": str(date)}
