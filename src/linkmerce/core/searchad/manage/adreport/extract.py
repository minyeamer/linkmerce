from __future__ import annotations
from linkmerce.core.searchad.manage import SearchAdManager

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal, Sequence
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class AdvancedReport(SearchAdManager):
    method = "POST"
    path = "/advanced-report/downloads"
    date_format = "%Y-%m-%d"

    @SearchAdManager.with_session
    @SearchAdManager.with_token
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
        ) -> Sequence:
        kwargs = dict(kwargs,
            report_id=report_id, report_name=report_name, userid=userid, attributes=attributes, fields=fields,
            start_date=start_date, end_date=(start_date if end_date == ":start_date:" else end_date))

        response = self.request_text(**kwargs)
        if self.is_valid_response(response):
            return self.parse(response, **kwargs)

    def is_valid_response(self, response: str) -> bool:
        if response.startswith('{') and response.endswith('}') and ("Forbidden" in response):
            from linkmerce.common.exceptions import UnauthorizedError
            raise UnauthorizedError("Forbidden")
        return True

    def build_request_data(self, **kwargs) -> str:
        return f"Authorization={self.get_authorization().replace(' ','+')}"

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
            "language": "ko-KR",
            "reportName": report_name,
            "since": str(start_date),
            "until": str(end_date),
            "values": '{"type":"metric","fields":"'+','.join(fields)+'"}',
        }

    def build_request_headers(self, report_id: str, **kwargs: str) -> dict[str,str]:
        referer = "{}/customers/{}/reports/{}".format(self.main_url, self.customer_id, report_id)
        return dict(self.get_request_headers(), authorization=self.get_authorization(), referer=referer)

    @SearchAdManager.cookies_required
    def set_request_headers(self, **kwargs: str):
        super().set_request_headers(contents="json", origin=self.main_url, metadata="cors", **kwargs)


class DailyReport(AdvancedReport):

    @SearchAdManager.with_session
    @SearchAdManager.with_token
    def extract(
            self,
            report_id: str,
            report_name: str,
            userid: str,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            **kwargs
        ) -> JsonObject:
        kwargs = dict(kwargs,
            report_id=report_id, report_name=report_name, userid=userid, attributes=self.attributes, fields=self.fields,
            start_date=start_date, end_date=(start_date if end_date == ":start_date:" else end_date), customer_id=self.customer_id)

        response = self.request_text(**kwargs)
        if self.is_valid_response(response):
            return self.parse(response, **kwargs)

    @property
    def attributes(self) -> list[str]:
        return ["nccAdId", "mediaNm", "pcMblTp", "ntwkTp", "ymd"]

    @property
    def fields(self) -> list[str]:
        return ["impCnt", "clkCnt", "salesAmt", "ccnt", "drtCcnt", "convAmt", "drtConvAmt", "avgRnk", "pv", "stayTm"]
