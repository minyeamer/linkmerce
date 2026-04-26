from __future__ import annotations
from linkmerce.core.smartstore.api import SmartstoreApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class MarketingChannel(SmartstoreApi):
    """네이버 커머스 API로 사용자 정의 채널 상세 데이터를 조회하는 클래스.

    - **Menu**: 통계 > 마케팅분석 > 사용자정의채널
    - **API**: https://api.commerce.naver.com/external/v1/bizdata-stats/channels/:channelNo/marketing/custom/detail
    - **Docs**: https://apicenter.commerce.naver.com/docs/commerce-api/current/custom-channel-report-using-get-bizdata-stats
    - **Referer**: https://sell.smartstore.naver.com/#/bizadvisor/marketing/custom

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    client_id: str
        커머스 API 애플리케이션 ID
    client_secret: str
        커머스 API 애플리케이션 시크릿

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값 `1`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    version = "v1"
    path = "/bizdata-stats/channels/:channelNo/marketing/custom/detail"
    date_format = "%Y-%m-%d"
    default_options = {"RequestEach": {"request_delay": 1}}

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
        """일별 상품/마케팅 채널 데이터를 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        query: str | Iterable[str]
            검색어 또는 검색어 목록
        start: int | Iterable[int]
            검색 시작 위치 (기본값: 1, 최댓값: 1000)
        display: int
            한 번에 표시할 검색 결과 개수 (기본값: 10, 최댓값: 100)
        sort: Literal["sim", "date"]
            검색 결과 정렬 방법
                - `"sim"`: 정확도순으로 내림차순 정렬 (기본값)
                - `"date"`: 날짜순으로 내림차순 정렬

        Returns
        -------
        dict | list[dict]
            검색 결과 또는 목록
        """
        url = self.url.replace(":channelNo", str(channel_seq))
        return (self.request_each(self.request_json_until_success)
                .partial(url=url, channel_seq=channel_seq, max_retries=max_retries)
                .expand(date=self.generate_date_range(start_date, end_date, freq='D'))
                .run())

    def build_request_params(self, date: dt.date, **kwargs) -> dict:
        return {"startDate": str(date), "endDate": str(date)}
