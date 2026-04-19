from __future__ import annotations
from linkmerce.core.searchad.center import SearchAdCenter

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class AdvancedReport(SearchAdCenter):
    """네이버 검색광고 시스템에서 다차원 보고서를 다운로드하는 클래스.

    - **Menu**: 보고서 > 다차원 보고서
    - **API URL**: `POST` https://ads.naver.com/apis/sa/api/advanced-report/downloads
    """
    method = "POST"
    path = "/advanced-report/downloads"
    date_format = "%Y-%m-%d"
    days_limit = 731

    @SearchAdCenter.with_session
    def extract(
            self,
            report_id: str,
            report_name: str,
            userid: str,
            attributes: Iterable[str],
            fields: Iterable[str],
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            **kwargs
        ) -> JsonObject | str:
        """다차원 보고서를 CSV 문자열 형식으로 다운로드한다."""
        kwargs.update({
            "report_id": report_id, "report_name": report_name,
            "userid": userid, "attributes": attributes, "fields": fields,
            "start_date": start_date, "end_date": (start_date if end_date == ":start_date:" else end_date),
        })
        response = self.request_text(**kwargs)
        return self.parse(response, **kwargs)

    def build_request_data(self, **kwargs) -> str:
        return "{}"

    def build_request_params(
            self,
            report_name: str,
            userid: str,
            attributes: Iterable[str],
            fields: Iterable[str],
            start_date: dt.date | str,
            end_date: dt.date | str,
            **kwargs
        ) -> dict:
        return {
            "attributes": ','.join(attributes),
            "clientLoginId": userid,
            "reportName": report_name,
            "since": str(start_date),
            "until": str(end_date),
            "values": '{"type":"metric","fields":"'+','.join(fields)+'"}',
        }

    def build_request_headers(self, report_id: str, **kwargs: str) -> dict[str, str]:
        referer = f"{self.origin}/manage/ad-accounts/{self.account_no}/sa/reports/{report_id}"
        return dict(self.get_request_headers(), referer=referer)

    def set_request_headers(self, **kwargs: str):
        super().set_request_headers(contents="json", origin=self.origin, **kwargs)


class DailyReport(AdvancedReport):
    """네이버 검색광고 시스템에서 다차원 보고서를 일별로 다운로드하는 클래스.

    - **Menu**: 보고서 > 다차원 보고서
    - **API URL**: `POST` https://ads.naver.com/apis/sa/api/advanced-report/downloads
    """

    @SearchAdCenter.with_session
    def extract(
            self,
            report_id: str,
            report_name: str,
            userid: str,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            **kwargs
        ) -> JsonObject | str:
        """일별 다차원 보고서를 CSV 문자열 형식으로 다운로드한다."""
        kwargs.update({
            "customer_id": self.customer_id, "report_id": report_id, "report_name": report_name,
            "userid": userid, "attributes": self.attributes, "fields": self.fields,
            "start_date": start_date, "end_date": (start_date if end_date == ":start_date:" else end_date),
        })
        response = self.request_text(**kwargs)
        return self.parse(response, **kwargs)

    @property
    def attributes(self) -> list[str]:
        """보고서 차원(소재, 매체이름, PC/모바일, 검색/콘텐츠, 일별) 목록을 반환한다."""
        # ["소재", "매체이름", "PC/모바일 매체", "검색/콘텐츠 매체", "일별"]
        return ["nccAdId", "mediaNm", "pcMblTp", "ntwkTp", "ymd"]

    @property
    def fields(self) -> list[str]:
        """보고서 측정값(노출수, 클릭수, 총비용, 전환 등) 목록을 반환한다."""
        # ["노출수", "클릭수", "총비용", "총 전환수", "직접전환수", "총 전환매출액(원)", "직접전환매출액(원)", ...]
        return ["impCnt", "clkCnt", "salesAmt", "ccnt", "drtCcnt", "convAmt", "drtConvAmt", "avgRnk", "pv", "stayTm"]
