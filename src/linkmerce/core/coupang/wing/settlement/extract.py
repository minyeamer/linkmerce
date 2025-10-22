from __future__ import annotations
from linkmerce.core.coupang.wing import CoupangWing

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import JsonObject
    import datetime as dt


def isoformat(date: dt.date | str) -> str:
    from linkmerce.utils.date import strptime
    return strptime(str(date), tzinfo="Asia/Seoul", astimezone="UTC").strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]+'Z'


class Summary(CoupangWing):
    method = "POST"
    path = "/tenants/rfm/v2/settlements/profit-status/search"
    token_required = True
    datetime_format = "%Y-%m-%dT%H:%M:%S.%fZ" # 2000-01-01T00:00:00.000Z

    @CoupangWing.with_session
    def extract(self, start_from: str, end_to: str, **kwargs) -> JsonObject:
        response = self.request_json(start_from=start_from, end_to=end_to)
        return self.parse(response)

    def build_request_json(self, start_from: str, end_to: str, **kwargs) -> dict:
        return {"recognitionDateFrom": start_from, "recognitionDateTo": end_to}


class RocketSettlement(CoupangWing):
    method = "POST"
    path = "/tenants/rfm/v2/settlements/status/api"
    token_required = True
    date_format = "%Y-%m-%d"

    @CoupangWing.with_session
    def extract(
            self,
            start_date: dt.date | str, 
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            date_type: Literal["PAYMENT","SALES"] = "SALES",
            vendor_id: str | None = None,
            **kwargs
        ) -> JsonObject:
        end_date = (start_date if end_date == ":start_date:" else end_date)
        response = self.request_json(start_date=start_date, end_date=end_date, date_type=date_type)
        return self.parse(response, vendor_id=vendor_id)

    def build_request_json(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str,
            date_type: str,
            **kwargs
        ) -> dict:
        from linkmerce.utils.date import strptime
        format = "%Y-%m-%dT%H:%M:%S.%f"
        return {
            "startDate": strptime(str(start_date), tzinfo="Asia/Seoul", astimezone="UTC").strftime(format)[:-3]+'Z',
            "endDate": strptime(str(end_date), tzinfo="Asia/Seoul", astimezone="UTC").strftime(format)[:-3]+'Z',
            "searchDateType": date_type
        }

    def build_request_headers(self, **kwargs: str) -> dict[str,str]:
        from linkmerce.utils.headers import add_headers
        return add_headers(
            self.get_request_headers(),
            authority = self.origin,
            contents = "json",
            origin = self.origin,
            referer = (self.origin + "/tenants/rfm/settlements/status-new")
        )

    @property
    def date_type(self) -> dict[str,str]:
        return {"PAYMENT": "정산일", "SALES": "매출 인식일"}


class RocketSettlementDownload(RocketSettlement):
    method = "POST"
    locale = "ko"

    @CoupangWing.with_session
    def extract(
            self,
            start_date: dt.date | str, 
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            date_type: Literal["PAYMENT","SALES"] = "SALES",
            vendor_id: str | None = None,
            wait_seconds: int = 60,
            wait_interval: int = 1,
            progress: bool = True,
            **kwargs
        ) -> dict[str,bytes]:
        from linkmerce.utils.progress import import_tqdm
        tqdm = import_tqdm()

        end_date = (start_date if end_date == ":start_date:" else end_date)
        response = self.request_json(start_date=start_date, end_date=end_date, date_type=date_type)

        downloaded, requested = list(), set()
        for report in tqdm(response["settlementStatusReports"], desc=f"Downloading reports", disable=(not progress)):
            group_key = report["settlementGroupKey"]
            if group_key not in requested:
                for report_type in ["CATEGORY_TR", "WAREHOUSING_SHIPPING"]:
                    downloaded.append(self.download(report_type, group_key, vendor_id, wait_seconds, wait_interval))
                    requested.add(group_key)
        return dict(downloaded)

    def download(
            self,
            report_type: str,
            group_key: str,
            vendor_id: str | None = None,
            wait_seconds: int = 60,
            wait_interval: int = 1,
        ) -> tuple[str,bytes]:
        request_time = self.current_time()
        request_info = self.request_download(report_type, group_key, request_time)
        file_name = "{}-{}-{}-{}.xlsx".format(request_info["vendorId"], report_type, self.locale, request_info["requestId"])
        self.wait_download(request_info["requestId"], request_time, wait_seconds, wait_interval)
        download_url = self.get_download_url(request_time)
        content = self.download_excel(download_url)
        return file_name, self.parse(content, report_type=report_type, vendor_id=vendor_id)

    def current_time(self) -> int:
        from pytz import timezone
        import datetime as dt
        return int(dt.datetime.now(timezone("UTC")).timestamp() * 1000)

    def request_download(self, report_type: str, group_key: str, request_time: int) -> dict:
        url = self.origin + "/tenants/rfm/v2/settlements/request-download/api"
        body = {
            "sellerReportType": report_type,
            "requestTime": str(request_time),
            "settlementGroupKeys": [group_key],
            "locale": self.locale
        }
        with self.request("POST", url, json=body, headers=self.build_request_headers()) as response:
            return response.json()

    def wait_download(self, request_id: str, request_time: int, wait_seconds: int = 60, wait_interval: int = 1) -> bool:
        import time
        url = self.origin + "/tenants/rfm/v2/settlements/download-list/api"
        body = {"requestTimeFrom": str(request_time - 3600000), "requestTimeTo": str(request_time + 3600000)}
        for _ in range(0, max(wait_seconds, 1), max(wait_interval, 1)):
            time.sleep(wait_interval)
            with self.request("POST", url, json=body, headers=self.build_request_headers()) as response:
                for request in response.json():
                    if isinstance(request, dict) and (request.get("requestId") == request_id):
                        if request.get("downloadStatus") == "COMPLETED":
                            return True
        raise ValueError("Failed to create the settlement report.")

    def get_download_url(self, request_time: int) -> str:
        url = self.origin + "/tenants/rfm/v2/settlements/download/api/v2"
        body = {"requestTime": str(request_time), "locale": self.locale}
        headers = self.build_request_headers()
        with self.request("POST", url, json=body, headers=headers) as response:
            return response.json()["url"]

    def download_excel(self, download_url: str) -> bytes:
        from linkmerce.utils.headers import build_headers
        headers = build_headers(host=download_url, referer=self.origin, metadata="navigate", https=True)
        return self.request("GET", download_url, headers=headers).content
