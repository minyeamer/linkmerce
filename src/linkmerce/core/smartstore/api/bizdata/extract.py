from __future__ import annotations
from linkmerce.core.smartstore.api import SmartstoreApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class MarketingChannel(SmartstoreApi):
    """네이버 커머스 API로 상품/마케팅 채널 데이터를 요청하는 클래스.

    - **API URL**: `GET` https://api.commerce.naver.com/external/v1/bizdata-stats/channels/:channelNo/marketing/custom/detail
    - **API Docs**: https://apicenter.commerce.naver.com/ko/basic/commerce-api

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

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
