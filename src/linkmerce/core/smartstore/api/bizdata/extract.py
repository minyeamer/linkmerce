from __future__ import annotations
from linkmerce.core.smartstore.api import SmartstoreApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    import datetime as dt


class MarketingChannel(SmartstoreApi):
    """네이버 커머스 API로 사용자 정의 채널 상세 데이터를 조회하는 클래스.

    - **Menu**: 통계 > 마케팅분석 > 사용자정의채널 (사용자 정의 채널 상세 API)
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
        요청 간 대기 시간. 기본값은 `1`
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
        ) -> dict | list[dict]:
        """사용자가 별도로 정의한 마케팅 채널별 유입 및 결제 성과를 일별로 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        channel_seq: int | str
            조회 채널 번호
        start_date: dt.date | str
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
        end_date: dt.date | str
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
        max_retries: int = 5
            동시 요청 제한이 발생할 경우 최대 재시도 횟수

        Returns
        -------
        dict | list[dict]
            사용자 정의 채널 상세 데이터. 조회 기간에 따라 반환 타입이 다르다.
                - `start_date`와 `end_date`가 동일할 때 -> `dict`
                - `start_date`와 `end_date`가 다를 때 -> `list[dict]`
        """
        url = self.url.replace(":channelNo", str(channel_seq))
        return (self.request_each(self.request_json_until_success)
                .partial(url=url, channel_seq=channel_seq, max_retries=max_retries)
                .expand(date=self.generate_date_range(start_date, end_date, freq='D'))
                .run())

    def build_request_params(self, date: dt.date, **kwargs) -> dict:
        return {"startDate": str(date), "endDate": str(date)}
