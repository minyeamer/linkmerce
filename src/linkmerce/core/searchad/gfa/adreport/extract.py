from __future__ import annotations
from linkmerce.core.searchad.gfa import SearchAdGfa

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class _MasterReport(SearchAdGfa):
    """네이버 성과형 디스플레이 광고 리포트를 조회하는 공통 클래스.

    `RequestEachPages` Task를 사용하여 캠페인, 광고그룹, 소재 목록을 조회한다."""

    report_type: Literal["Campaign", "AdSet", "Creative"]
    method = "GET"
    max_page_size = 100
    page_start = 0

    @property
    def default_options(self) -> dict:
        return {
            "PaginateAll": {"request_delay": 0.3},
            "RequestEachPages": {"request_delay": 0.3},
        }

    def count_total(self, response: JsonObject, **kwargs) -> int:
        """HTTP 응답에서 전체 항목 수를 추출한다."""
        return response.get("totalElements") if isinstance(response, dict) else None


class Campaign(_MasterReport):
    """네이버 성과형 디스플레이 광고 캠페인 목록을 상태(`status`)별로 조회하는 클래스."""

    report_type = "Campaign"
    version = "v1.2"
    path = "/campaigns"

    @_MasterReport.with_session
    def extract(
            self,
            status: Sequence[Literal["RUNNABLE", "DELETED"]] = ["RUNNABLE", "DELETED"],
            **kwargs
        ) -> JsonObject:
        """캠페인 목록을 상태(`status`)별로 조회해 JSON 형식으로 반환한다."""
        return (self.request_each_pages(self.request_json_safe)
                .expand(status=status)
                .all_pages(self.count_total, self.max_page_size, self.page_start)
                .run())

    def build_request_params(
            self,
            status: Literal["RUNNABLE", "DELETED"],
            page: int = 0,
            page_size: int = 100,
            **kwargs
        ) -> list[tuple]:
        return [
            ("page", int(page)),
            ("size", int(page_size)),
            ("sort", "no,desc"),
            ("statusList", status),
            *[("objectiveList", code) for code in self.campaign_objective.keys()],
        ]

    @property
    def campaign_objective(self) -> dict[str, str]:
        """캠페인 목표 유형 매핑을 반환한다."""
        return {
            "CONVERSION": "웹사이트 전환", "WEB_SITE_TRAFFIC": "인지도 및 트래픽", "INSTALL_APP": "앱 전환",
            "WATCH_VIDEO": "동영상 조회", "CATALOG": "카탈로그 판매", "SHOPPING": "쇼핑 프로모션",
            "LEAD": "참여 유도", "PMAX": "ADVoost 쇼핑"
        }


class AdSet(_MasterReport):
    """네이버 성과형 디스플레이 광고그룹 목록을 상태(`status`)별로 조회하는 클래스."""

    report_type = "AdSet"
    version = "v1.2"
    path = "/adSets"

    @_MasterReport.with_session
    def extract(
            self,
            status: Sequence[Literal["ALL", "RUNNABLE", "BEFORE_STARTING", "TERMINATED", "DELETED"]] = ["ALL", "DELETED"],
            **kwargs
        ) -> JsonObject:
        """광고그룹 목록을 상태(`status`)별로 조회해 JSON 형식으로 반환한다."""
        return (self.request_each_pages(self.request_json_safe)
                .partial(account_no=self.account_no)
                .expand(status=status)
                .all_pages(self.count_total, self.max_page_size, self.page_start)
                .run())

    def build_request_params(
            self,
            status: Literal["ALL", "RUNNABLE", "BEFORE_STARTING", "TERMINATED", "DELETED"],
            page: int = 0,
            page_size: int = 100,
            **kwargs
        ) -> list[tuple]:
        return [
            ("page", int(page)),
            ("size", int(page_size)),
            *([("statusList", code) for code in self.status.keys()] if status == "ALL" else [("statusList", status)]),
            ("adSetNameOnly", "true"),
            *[("budgetTypeList", code) for code in self.budget_type.keys()],
            *[("bidTypeList", code) for code in self.bid_type.keys()],
            *[("placementGroupCodeList", code) for code in self.placement_group.keys()],
        ]

    @property
    def status(self) -> dict[str, str]:
        """광고그룹 상태 매핑을 반환한다."""
        return {"RUNNABLE": "운영가능", "BEFORE_STARTING": "광고집행전", "TERMINATED": "광고집행종료"}

    @property
    def budget_type(self) -> dict[str, str]:
        """예산 유형 목록을 반환한다."""
        return {"DAILY": "일예산", "TOTAL": "총예산"}

    @property
    def bid_type(self) -> dict[str, str]:
        """입찰 유형 목록을 반환한다."""
        return {
            "COST_CAP": "비용 한도", "BID_CAP": "입찰가 한도", "NO_CAP": "입찰가 한도 없음",
            "CPC": "수동 CPC", "CPM": "수동 CPM", "CPV": "수동 CPV"
        }

    @property
    def placement_group(self) -> dict[str, str]:
        """노출 위치 목록을 반환한다."""
        return {
            "M_SMARTCHANNEL": "네이버+ > 스마트채널", "M_FEED": "네이버+ > 피드", "M_MAIN": "네이버+ > 네이버 메인",
            "M_BANNER": "네이버+ > 서비스 통합", "N_SHOPPING": "네이버+ > 쇼핑", "N_COMMUNICATION": "네이버+ > 커뮤니케이션",
            "N_INSTREAM": "네이버+ > 인스트림", "NW_SMARTCHANNEL": "네이버 퍼포먼스 네트워크 > 스마트채널",
            "NW_FEED": "네이버 퍼포먼스 네트워크 > 피드", "NW_BANNER": "네이버 퍼포먼스 네트워크 > 서비스 통합"
        }


class Creative(_MasterReport):
    """네이버 성과형 디스플레이 광고 소재 목록을 상태(`status`)별로 조회하는 클래스."""

    report_type = "Creative"
    version = "v1"
    path = "/creatives/draft/searchByKeyword"

    @_MasterReport.with_session
    def extract(
            self,
            status: Sequence[Literal["ALL", "PENDING", "REJECT", "ACCEPT", "PENDING_IN_OPERATION", "REJECT_IN_OPERATION", "DELETED"]] = ["ALL", "DELETED"],
            **kwargs
        ) -> JsonObject:
        """소재 목록을 상태(`status`)별로 조회해 JSON 형식으로 반환한다."""
        return (self.request_each_pages(self.request_json_safe)
                .partial(account_no=self.account_no)
                .expand(status=status)
                .all_pages(self.count_total, self.max_page_size, self.page_start)
                .run())

    def build_request_params(
            self,
            status: Literal["ALL", "PENDING", "REJECT", "ACCEPT", "PENDING_IN_OPERATION", "REJECT_IN_OPERATION", "DELETED"],
            page: int = 0,
            page_size: int = 100,
            **kwargs
        ) -> list[tuple]:
        return [
            ("page", int(page)),
            ("size", int(page_size)),
            *[("onOffs", str(i)) for i in [1, 0]],
            *([("statuses", code) for code in self.status.keys()] if status == "ALL" else [("statuses", status)]),
            *[("creativeTypes", code) for code in self.creative_type.keys()],
        ]

    @property
    def status(self) -> dict[str, str]:
        """소재 검수 상태 매핑을 반환한다."""
        return {
            "PENDING": "검수중", "REJECT": "반려", "ACCEPT": "승인",
            "PENDING_IN_OPERATION": "승인 (수정사항 검수중)", "REJECT_IN_OPERATION": "승인 (수정사항 반려)"
        }

    @property
    def creative_type(self) -> dict[str, str]:
        """소재 유형 매핑을 반환한다."""
        return {
            "SINGLE_IMAGE": "네이티브 이미지", "MULTIPLE_IMAGE": "컬렉션", "SINGLE_VIDEO": "동영상",
            "IMAGE_BANNER": "이미지 배너", "CATALOG": "카탈로그", "COMPOSITION": "ADVoost 소재"
        }


class PerformanceReport(SearchAdGfa):
    """네이버 성과형 디스플레이 광고 성과 리포트를 다운로드하는 클래스.

    날짜 범위를 최대 62일 단위로 분할하여 엑셀 리포트를 요청하고 다운로드한다."""

    date_format = "%Y-%m-%d"
    version = "v1"
    path = "/report/downloads"

    @SearchAdGfa.with_session
    def extract(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            date_type: Literal["TOTAL", "DAY", "WEEK", "MONTH", "HOUR"] = "DAY",
            columns: list[str] | Literal[":default:"] = ":default:",
            ad_unit: Literal["AD_ACCOUNT", "CAMPAIGN", "AD_SET", "ASSET_GROUP", "CREATIVE"] = "CREATIVE",
            wait_seconds: int = 60,
            wait_interval: int = 1,
            progress: bool = True,
            **kwargs
        ) -> dict[str, bytes]:
        """성과 리포트를 생성하고 엑셀 파일로 다운로드한다."""
        columns = self.db_columns if columns == ":default:" else columns
        dates = self.generate_date_range(start_date, end_date=end_date)
        return self.download(columns, dates, date_type, ad_unit, wait_seconds, wait_interval, progress)

    def download(
            self,
            columns: list[str],
            dates: list[tuple[dt.date, dt.date]],
            date_type: Literal["TOTAL", "DAY", "WEEK", "MONTH", "HOUR"] = "DAY",
            ad_unit: Literal["AD_ACCOUNT", "CAMPAIGN", "AD_SET", "ASSET_GROUP", "CREATIVE"] = "CREATIVE",
            wait_seconds: int = 60,
            wait_interval: int = 1,
            progress: bool = True,
        ) -> dict[str, bytes]:
        """리포트 생성부터 삭제까지의 워크플로우를 실행한다."""
        from linkmerce.utils.progress import import_tqdm
        import time
        tqdm = import_tqdm()

        status = [False] * len(dates)
        for index, (start_date, end_date) in enumerate(tqdm(dates, desc="Requesting performance reports", disable=(not progress))):
            kwargs = {"start_date": start_date, "end_date": end_date, "date_type": date_type, "ad_unit": ad_unit, "columns": columns}
            status[index] = self.request_report(**kwargs)
            time.sleep(wait_interval)

        indices = [i for i, status in enumerate(status[::-1]) if status]
        downloads = self.wait_reports(indices, wait_seconds, wait_interval, **kwargs)

        results = dict()
        for report in tqdm(downloads, desc="Downloading performance reports", disable=(not progress)):
            try:
                file_name = self.query_to_filename(report["reportQuery"])
                content = self.download_excel(report["no"], **kwargs)
                if isinstance(report["fileSize"], int) and (report["fileSize"] > 0):
                    results[file_name] = self.parse(content, account_no=self.account_no)
                    time.sleep(wait_interval)
                else:
                    results[file_name] = None
            finally:
                self.delete_report(report["no"], **kwargs)
        return results

    def generate_date_range(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        ) -> list[tuple[dt.date, dt.date]]:
        """분석 기간 단위가 '일'인 경우, 요청 당 조회 기간은 최대 62일로 제한한다."""
        from linkmerce.utils.date import date_split
        end_date = start_date if end_date == ":start_date:" else end_date
        return date_split(start_date, end_date, delta={"days": 60}, format=self.date_format)

    def request_report(self, **kwargs) -> bool:
        """성과 리포트 생성을 요청한다."""
        body = self.build_download_json(**kwargs)
        headers = self.build_request_headers(**kwargs)
        headers["content-type"] = "application/json"
        with self.request("POST", self.url, json=body, headers=headers) as response:
            return response.json()["success"]

    def wait_reports(self, indices: list[int], wait_seconds: int = 60, wait_interval: int = 1, **kwargs) -> list[dict]:
        """리포트 생성 완료를 대기한다."""
        from linkmerce.common.exceptions import RequestError
        import time
        params = {"reportType": "PERFORMANCE"}
        headers = self.build_request_headers(**kwargs)
        for _ in range(0, max(wait_seconds, 1), max(wait_interval, 1)):
            time.sleep(wait_interval)
            with self.request("GET", self.url, params=params, headers=headers) as response:
                downloads = response.json()
                for index in indices:
                    if downloads[index]["status"] != "COMPLETED":
                        continue
                return [downloads[index] for index in indices]
        raise RequestError("Download was not completed within the waiting seconds.")

    def download_excel(self, download_no: int, **kwargs) -> bytes:
        """리포트 엑셀 파일을 다운로드한다."""
        url = self.url + f"/{download_no}/download"
        headers = self.build_request_headers(**kwargs)
        with self.request("GET", url, headers=headers) as response:
            return response.content

    def delete_report(self, download_no: int, **kwargs) -> bool:
        """생성된 리포트를 삭제한다."""
        params = {"reportDownloadNos": download_no, "reportType": "PERFORMANCE"}
        headers = self.build_request_headers(**kwargs)
        with self.request("DELETE", self.url, params=params, headers=headers) as response:
            return response.json()["success"]

    def query_to_filename(self, report_query: dict) -> str:
        """리포트 쿼리 값으로 엑셀 파일명을 생성한다."""
        start_date = str(report_query["startDate"]).replace('-', '')
        end_date = str(report_query["endDate"]).replace('-', '')
        return f"ReportDownload_aa_{self.account_no}_PERFORMANCE_{start_date}_{end_date}.csv"

    def build_download_json(
            self,
            columns: list[str],
            start_date: dt.date,
            end_date: dt.date,
            date_type: Literal["TOTAL", "DAY", "WEEK", "MONTH", "HOUR"] = "DAY",
            ad_unit: Literal["AD_ACCOUNT", "CAMPAIGN", "AD_SET", "ASSET_GROUP", "CREATIVE"] = "CREATIVE",
            **kwargs
        ) -> dict:
        return {
            "needToNoti": False,
            "reportQuery": {
                "startDate": str(start_date),
                "endDate": str(end_date),
                "reportDateUnit": date_type,
                "placeUnit": "TOTAL",
                "reportAdUnit": ad_unit,
                "reportDimension": "TOTAL",
                "colList": columns,
                "adAccountNo": self.account_no,
                "reportFilterList": list()
            },
            "reportType": "PERFORMANCE"
        }

    def build_request_headers(self, **kwargs: str) -> dict[str, str]:
        return dict(self.get_request_headers(), origin=self.origin, referer=self.referer(**kwargs))

    def referer(self, start_date: dt.date, end_date: dt.date, columns: list[str] = list(), **kwargs) -> str:
        params = '&'.join([f"{key}={value}" for key, value in {
            "startDate": str(start_date),
            "endDate": str(end_date),
            "adUnit": kwargs.get("ad_unit") or "CREATIVE",
            "dateUnit": kwargs.get("date_type") or "DAY",
            "placeUnit": "TOTAL",
            "dimension": "TOTAL",
            "currentPage": 1,
            "pageSize": 100,
            "filterList": "%5B%5D",
            "showColList": ("%5B%22" + "%22,%22".join(columns if columns else self.db_columns) + "%22%5D"),
            "accessAdAccountNo": self.account_no,
        }.items()])
        return f"{self.origin}/manage/ad-accounts/{self.account_no}/da/report/performance?{params}"

    @property
    def db_columns(self) -> list[str]:
        """성과 리포트 측정값 칼럼 목록을 반환한다."""
        return list()

    @property
    def ad_unit(self) -> dict[str, str]:
        """광고 단위 목록을 반환한다."""
        return {
            "광고 계정": "AD_ACCOUNT",
            "캠페인": "CAMPAIGN",
            "광고 그룹": "AD_SET",
            "애셋 그룹": "ASSET_GROUP",
            "광고 소재": "CREATIVE",
        }


class CampaignReport(PerformanceReport):
    """네이버 성과형 디스플레이 광고 캠페인 성과 리포트를 다운로드하는 클래스."""

    def extract(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            date_type: Literal["TOTAL", "DAY", "WEEK", "MONTH", "HOUR"] = "DAY",
            columns: list[str] | Literal[":default:"] = ":default:",
            wait_seconds: int = 60,
            wait_interval: int = 1,
            progress: bool = True,
            **kwargs
        ) -> dict[str, bytes]:
        """캠페인 성과 리포트를 다운로드한다."""
        return super().extract(
            start_date, end_date, date_type, columns, "CAMPAIGN", wait_seconds, wait_interval, progress, **kwargs)

    @property
    def db_columns(self) -> list[str]:
        """캠페인 성과 리포트 측정값 칼럼 목록을 반환한다."""
        return ["sales", "impCount", "clickCount", "convCount", "convSales"]


class CreativeReport(PerformanceReport):
    """네이버 성과형 디스플레이 광고 소재 성과 리포트를 다운로드하는 클래스."""

    def extract(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            date_type: Literal["TOTAL", "DAY", "WEEK", "MONTH", "HOUR"] = "DAY",
            columns: list[str] | Literal[":default:"] = ":default:",
            wait_seconds: int = 60,
            wait_interval: int = 1,
            progress: bool = True,
            **kwargs
        ) -> dict[str, bytes]:
        """소재 성과 리포트를 다운로드한다."""
        return super().extract(
            start_date, end_date, date_type, columns, "CREATIVE", wait_seconds, wait_interval, progress, **kwargs)

    @property
    def db_columns(self) -> list[str]:
        """소재 성과 리포트 측정값 칼럼 목록을 반환한다."""
        return ["sales", "impCount", "clickCount", "convCount", "convSales"]
