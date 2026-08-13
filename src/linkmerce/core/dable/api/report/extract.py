from __future__ import annotations
from linkmerce.core.dable.api import DableApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    import datetime as dt


class DailyReport(DableApi):
    """데이블 광고 보고서 API 요청을 처리하는 클래스.

    > 안내) API를 통해 최대 90일치의 데이터를 한 번에 수신할 수 있으며, 지난 365일간의 데이터에 접근할 수 있다.

    - **API**: https://marketing.dable.io/api/client/:client_name/daily_report
    - **Docs**: https://dableglobal.notion.site/Dable-API-For-Advertiser-Agency-f5a12bd0b2bf4b80a5087693c3dca510

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    api_key: str
        데이블 설정에서 발급 가능한 API KEY
    client_name: str
        데이블 URL 내 클라이언트 명칭
    """

    method = "GET"
    path = "/daily_report"
    date_format = "%Y%m%d"
    days_limit = 90

    @DableApi.with_session
    def extract(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            group_by_campaign: bool = True,
            **kwargs
        ) -> dict:
        """광고 보고서를 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        start_date: dt.date | str
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
        end_date: dt.date | str
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
        group_by_campaign: bool
            캠페인 단위로 조회할지 여부
                - `True`: 캠페인 단위로 조회 (기본값)
                - `False`: 일별로 조회

        Returns
        -------
        dict
            데이블 광고 보고서 조회 결과
        """
        response = self.request_json(
            start_date = start_date,
            end_date = (start_date if end_date == ":start_date:" else end_date),
            group_by_campaign = group_by_campaign,
        )
        return self.parse(response, group_by_campaign=group_by_campaign)

    def build_request_params(
            self, 
            start_date: dt.date | str,
            end_date: dt.date | str,
            group_by_campaign: bool = True,
            **kwargs
        ):
        return {
            "api_key": self.api_key,
            "start_date": str(start_date).replace('-', ''),
            "end_date": str(end_date).replace('-', ''),
            **({"group_by_campaign": 1} if group_by_campaign else dict()),
        }

    def build_request_headers(self, **kwargs) -> dict[str, str]:
        return dict()
