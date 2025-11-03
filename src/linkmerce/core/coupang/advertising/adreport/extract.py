from __future__ import annotations
from linkmerce.core.coupang.advertising import CoupangAds

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    import datetime as dt


class MarketingReport(CoupangAds):
    method = "POST"
    path = "/marketing-reporting/v2/graphql"
    date_format = "%Y%m%d"

    @CoupangAds.with_session
    @CoupangAds.authorize
    def extract(
            self,
            start_date: dt.date | str, 
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            date_type: Literal["total","daily"] = "daily",
            report_type: Literal["campaign","adGroup","vendorItem","keyword"] = "vendorItem",
            campaign_ids: Sequence[int | str] = list(),
            vendor_id: str | None = None,
            wait_seconds: int = 60,
            wait_interval: int = 1,
            **kwargs
        ) -> dict[str,bytes]:
        start_date = self.to_date(start_date)
        end_date = self.to_date(start_date if end_date == ":start_date:" else end_date)

        if not campaign_ids:
            campaign_ids = self.fetch_campaign_ids(start_date, end_date)
        campaign_ids = list(map(str, campaign_ids))

        if not campaign_ids:
            print(f"No campaigns or data found for the period: '{start_date}' - '{end_date}'")
            return dict()

        report = self.request_report(start_date, end_date, date_type, report_type, campaign_ids=campaign_ids)
        report_id = report["data"]["requestReport"]["id"]

        self.wait_report(report_id, wait_seconds, wait_interval)
        file_name = f"{vendor_id or str()}_pa_{date_type}_{report_type}_{start_date}_{end_date}.xlsx"
        return {file_name: self.download_excel(report_id, vendor_id)}

    def fetch_dashboard(self):
        super().fetch_dashboard()
        url = self.origin + "/marketing-reporting/billboard"
        headers = self.build_request_headers()
        headers["referer"] = url + "/reports"
        self.request("GET", url, headers=headers)

    def fetch_campaign_ids(self, start_date: int, end_date: int) -> list[str]:
        body = self.build_campaign_body(start_date, end_date)
        with self.request(self.method, self.url, json=body, headers=self.build_request_headers()) as response:
            return [row["id"] for row in response.json()[0]["data"]["getCampaignList"]]

    def request_report(self, start_date: int, end_date: int, date_type: str, report_type: str, campaign_ids: list[str]) -> dict:
        body = self.build_mutation_body(start_date, end_date, date_type, report_type, campaign_ids)
        with self.request(self.method, self.url, json=body, headers=self.build_request_headers()) as response:
            return reports[0] if (reports := response.json()) else dict()

    def wait_report(self, report_id: str, wait_seconds: int = 60, wait_interval: int = 1) -> bool:
        import time
        for _ in range(0, max(wait_seconds, 1), max(wait_interval, 1)):
            time.sleep(wait_interval)
            for report in self.list_report()[0]["data"]["reportList"]["reports"]:
                if isinstance(report, dict) and (report["id"] == report_id):
                    if report["status"] == "completed":
                        return True
        raise ValueError("Failed to create the marketing report.")

    def list_report(self, page: int = 1, page_size: int = 10, duration: int = 90) -> list[dict]:
        body = self.build_query_body(page=page, paege_size=page_size, duration=duration)
        with self.request(self.method, self.url, json=body, headers=self.build_request_headers()) as response:
            return response.json()

    def download_excel(self, report_id: str, vendor_id: str | None = None) -> bytes:
        url = self.origin + f"/marketing-reporting/v2/api/excel-report?id={report_id}"
        with self.request("GET", url, headers=self.build_request_headers()) as response:
            return self.parse(response.content, vendor_id=vendor_id)

    def build_mutation_body(
            self,
            start_date: int,
            end_date: int,
            date_type: Literal["total","daily"] = "daily",
            report_type: Literal["campaign","adGroup","vendorItem","keyword"] = "vendorItem",
            campaign_ids: list[str] = list(),
        ) -> list[dict]:
        from linkmerce.utils.graphql import GraphQLOperation, GraphQLSelection, GraphQLFragment

        variables = {
            "startDate": start_date,
            "endDate": end_date,
            "campaignIds": campaign_ids,
            "reportType": "pa",
            "dateGroup": date_type,
            "granularity": report_type,
            "excludeIfNoClickCount": False
        }

        types = {
            "startDate": "Int!",
            "endDate": "Int!",
            "campaignIds": "[ID]",
            "reportType": "ReportType!",
            "dateGroup": "DateGroup!",
            "granularity": "Granularity",
            "excludeIfNoClickCount": "Boolean",
        }

        return [GraphQLOperation(
            operation = str(),
            variables = variables,
            types = types,
            selection = GraphQLSelection(
                name = "requestReport",
                variables = dict(data=list(variables.keys())),
                fields = GraphQLFragment("ReportRequest", "ReportRequest", fields=self.report_fields),
            ),
        ).generate_body(query_options = dict(
            command = "mutation",
            selection = dict(variables=dict(linebreak=True), fields=dict(linebreak=True)),
            suffix = '\n'))]

    def build_query_body(self, page: int = 1, paege_size: int = 10, duration: int = 90) -> list[dict]:
        from linkmerce.utils.graphql import GraphQLOperation, GraphQLSelection, GraphQLFragment

        variables = {"reportType": "pa", "page": page, "pageSize": paege_size, "duration": duration, "onlyScheduledReport": False}
        types = {"reportType": "ReportType!", "page": "Int!", "pageSize": "Int!", "duration": "Int!", "onlyScheduledReport": "Boolean"}

        return [GraphQLOperation(
            operation = str(),
            variables = variables,
            types = types,
            selection = GraphQLSelection(
                name = "reportList",
                variables = dict(data=list(variables.keys())),
                fields = GraphQLFragment("ReportList", "ReportList", fields=self.report_list_fields),
            ),
        ).generate_body(query_options = dict(
            command = "query",
            selection = dict(variables=dict(linebreak=True), fields=dict(linebreak=True)),
            suffix = '\n'))]

    def build_campaign_body(self, start_date: int, end_date: int) -> list[dict]:
        from linkmerce.utils.graphql import GraphQLOperation, GraphQLSelection

        variables = {"startDate": start_date, "endDate": end_date, "reportType": "pa"}
        types = {"startDate": "Int!", "endDate": "Int!", "reportType": "ReportType!"}

        return [GraphQLOperation(
            operation = "GetCampaignListInBillboard",
            variables = variables,
            types = types,
            selection = GraphQLSelection(
                name = "getCampaignList",
                variables = list(variables.keys()),
                fields = ["id", "name"],
            )
        ).generate_body(query_options = dict(
            selection = dict(variables=dict(linebreak=True), fields=dict(linebreak=True)),
            suffix = '\n'))]

    @CoupangAds.cookies_required
    def set_request_headers(self, **kwargs):
        super().set_request_headers(
            authority = self.origin,
            contents = "json",
            origin = self.origin,
            referer = self.origin + "/marketing-reporting/billboard/reports/pa",
            **kwargs
        )

    def build_request_headers(self, **kwargs):
        headers = super().build_request_headers()
        headers["cookies"] = self.get_cookies()
        return headers

    def to_date(self, date: dt.date | str) -> int:
        return int(str(date).replace('-', ''))

    @property
    def date_type(self) -> dict[str,str]:
        return {"total": "합계", "daily": "일별"}

    @property
    def report_type(self) -> dict[str,str]:
        return {
            "campaign": "캠페인",
            "adGroup": "캠페인 > 광고그룹",
            "vendorItem": "캠페인 > 광고그룹 > 상품",
            "keyword": "캠페인 > 광고그룹 > 상품 > 키워드",
        }

    @property
    def report_fields(self) -> list[str]:
        return [
            "id",
            "requestDate",
            "startDate",
            "endDate",
            "reportType",
            "dateGroup",
            "granularity",
            "excludeIfNoClickCount",
            "campaignName",
            "campaignCount",
            "status",
            "isLargeReport",
            {"schedule": ["scheduleType", "title"]},
        ]

    @property
    def report_list_fields(self) -> str:
        schedule = ["title", "scheduleType", "createDay", "requestDate", "expireAt"]
        reports = self.report_fields[:-1] + [{"schedule": schedule}]
        return ["page", "pageSize", "total", "duration", "onlyScheduledReport", {"reports": reports}]
