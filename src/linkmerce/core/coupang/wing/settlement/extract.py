from __future__ import annotations
from linkmerce.core.coupang.wing import CoupangWing

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import JsonObject
    import datetime as dt


def isoformat(date: dt.date | str) -> str:
    """날짜를 ISO 8601 형식 문자열로 변환한다."""
    from linkmerce.utils.date import strptime
    return strptime(str(date), tzinfo="Asia/Seoul", astimezone="UTC").strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]+'Z'


class Summary(CoupangWing):
    """쿠팡 로켓 손익 현황 요약 데이터를 조회하는 클래스."""

    method = "POST"
    path = "/tenants/rfm/v2/settlements/profit-status/search"
    token_required = True
    datetime_format = "%Y-%m-%dT%H:%M:%S.%fZ" # 2000-01-01T00:00:00.000Z

    @CoupangWing.with_session
    def extract(self, start_from: str, end_to: str, **kwargs) -> JsonObject:
        """로켓 손익 현황 요약 데이터를 조회해 JSON 형식으로 반환한다."""
        response = self.request_json(start_from=start_from, end_to=end_to)
        return self.parse(response)

    def build_request_json(self, start_from: str, end_to: str, **kwargs) -> dict:
        return {"recognitionDateFrom": start_from, "recognitionDateTo": end_to}


class RocketSettlement(CoupangWing):
    """쿠팡 로켓 정산 현황을 조회하는 클래스."""

    method = "POST"
    path = "/tenants/rfm/v2/settlements/status/api"
    token_required = True
    date_format = "%Y-%m-%d"

    @CoupangWing.with_session
    def extract(
            self,
            start_date: dt.date | str, 
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            date_type: Literal["PAYMENT", "SALES"] = "SALES",
            vendor_id: str | None = None,
            **kwargs
        ) -> JsonObject:
        """로켓 정산 현황을 매출 인식일(`SALES`) 또는 정산일(`PAYMENT`) 기준으로 조회하고 JSON 형식으로 반환한다."""
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

    def build_request_headers(self, **kwargs: str) -> dict[str, str]:
        from linkmerce.utils.headers import add_headers
        return add_headers(
            self.get_request_headers(),
            authority = self.origin,
            contents = "json",
            origin = self.origin,
            referer = (self.origin + "/tenants/rfm/settlements/status-new")
        )

    @property
    def date_type(self) -> dict[str, str]:
        return {"PAYMENT": "정산일", "SALES": "매출 인식일"}


class RocketSettlementDownload(RocketSettlement):
    """쿠팡 로켓 정산 보고서를 엑셀로 다운로드하는 클래스."""

    method = "POST"
    locale = "ko"

    @CoupangWing.with_session
    def extract(
            self,
            start_date: dt.date | str, 
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            date_type: Literal["PAYMENT", "SALES"] = "SALES",
            vendor_id: str | None = None,
            wait_seconds: int = 60,
            wait_interval: int = 1,
            progress: bool = True,
            **kwargs
        ) -> dict[str, bytes]:
        """두 가지 유형의 로켓 정산 보고서를 다운로드하여 `{시트명: 엑셀_바이너리}` 형식으로 반환한다.
        - `CATEGORY_TR`: 판매 수수료 리포트
        - `WAREHOUSING_SHIPPING`: 입출고비/배송비 리포트"""
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
        ) -> tuple[str, bytes]:
        """보고서 유형별 엑셀 파일을 생성하고 다운로드한다."""
        request_time = self.current_time()
        request_info = self.request_download(report_type, group_key, request_time)
        file_name = "{}-{}-{}-{}.xlsx".format(request_info["vendorId"], report_type, self.locale, request_info["requestId"])
        self.wait_download(request_info["requestId"], request_time, wait_seconds, wait_interval)
        download_url = self.get_download_url(request_time)
        content = self.download_excel(download_url)
        return file_name, self.parse(content, report_type=report_type, vendor_id=vendor_id)

    def current_time(self) -> int:
        """현재 시각을 밀리초 타임스탬프로 반환한다."""
        from pytz import timezone
        import datetime as dt
        return int(dt.datetime.now(timezone("UTC")).timestamp() * 1000)

    def request_download(self, report_type: str, group_key: str, request_time: int) -> dict:
        """엑셀 다운로드 보고서 생성을 요청한다."""
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
        """보고서 생성 요청 후 완료 여부를 주기적으로 확인하면서 대기한다."""
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
        """엑셀 다운로드 URL을 조회한다."""
        url = self.origin + "/tenants/rfm/v2/settlements/download/api/v2"
        body = {"requestTime": str(request_time), "locale": self.locale}
        headers = self.build_request_headers()
        with self.request("POST", url, json=body, headers=headers) as response:
            return response.json()["url"]

    def download_excel(self, download_url: str) -> bytes:
        """엑셀 파일을 다운로드하여 바이너리로 반환한다."""
        from linkmerce.utils.headers import build_headers
        import requests
        headers = build_headers(host=download_url, referer=self.origin, metadata="navigate", https=True)
        return requests.request("GET", download_url, headers=headers).content
