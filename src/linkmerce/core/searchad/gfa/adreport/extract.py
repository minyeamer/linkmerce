from __future__ import annotations
from linkmerce.core.searchad.gfa import SearchAdGFA

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class _MasterReport(SearchAdGFA):
    report_type: Literal["Campaign", "AdSet", "Creative"]
    method = "GET"
    max_page_size = 100
    page_start = 0

    @property
    def default_options(self) -> dict:
        return dict(
            PaginateAll = dict(request_delay=0.3),
            RequestEachPages = dict(request_delay=0.3)
        )

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.path.format(self.account_no))

    def count_total(self, response: JsonObject, **kwargs) -> int:
        return response.get("totalElements") if isinstance(response, dict) else None

    def build_request_headers(self, **kwargs: str) -> dict[str,str]:
        args = (self.account_no, self.report_type, self.account_no)
        referer = self.origin + "/adAccount/accounts/{}/ad/search?page=1&tabId=tab{}&accessAdAccountNo={}".format(*args)
        return dict(self.get_request_headers(with_token=True), referer=referer)


class Campaign(_MasterReport):
    report_type = "Campaign"
    path = "/apis/gfa/v1.1/adAccounts/{}/campaigns"

    @_MasterReport.with_session
    @_MasterReport.with_token
    def extract(
            self,
            status: Sequence[Literal["RUNNABLE","DELETED"]] = ["RUNNABLE","DELETED"],
            **kwargs
        ) -> JsonObject:
        return (self.request_each_pages(self.request_json_safe)
                .expand(status=status)
                .all_pages(self.count_total, self.max_page_size, self.page_start)
                .run())

    def build_request_params(
            self,
            status: Literal["RUNNABLE","DELETED"],
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
    def campaign_objective(self) -> dict[str,str]:
        return {
            "CONVERSION": "웹사이트 전환", "WEB_SITE_TRAFFIC": "인지도 및 트래픽", "INSTALL_APP": "앱 전환",
            "WATCH_VIDEO": "동영상 조회", "CATALOG": "카탈로그 판매", "SHOPPING": "쇼핑 프로모션",
            "LEAD": "참여 유도", "PMAX": "ADVoost 쇼핑"
        }


class AdSet(_MasterReport):
    report_type = "AdSet"
    path = "/apis/gfa/v1.2/adAccounts/{}/adSets"

    @_MasterReport.with_session
    @_MasterReport.with_token
    def extract(
            self,
            status: Sequence[Literal["ALL","RUNNABLE","BEFORE_STARTING","TERMINATED","DELETED"]] = ["ALL","DELETED"],
            **kwargs
        ) -> JsonObject:
        return (self.request_each_pages(self.request_json_safe)
                .partial(account_no=self.account_no)
                .expand(status=status)
                .all_pages(self.count_total, self.max_page_size, self.page_start)
                .run())

    def build_request_params(
            self,
            status: Literal["ALL","RUNNABLE","BEFORE_STARTING","TERMINATED","DELETED"],
            page: int = 0,
            page_size: int = 100,
            **kwargs
        ) -> list[tuple]:
        return [
            ("page", int(page)),
            ("size", int(page_size)),
            *([("statusList", code) for code in self.status] if status == "ALL" else [("statusList", status)]),
            ("adSetNameOnly", "true"),
            *[("budgetTypeList", code) for code in self.budget_type.keys()],
            *[("bidTypeList", code) for code in self.bid_type.keys()],
            *[("placementGroupCodeList", code) for code in self.placement_group.keys()],
        ]

    @property
    def status(self) -> dict[str,str]:
        return {"RUNNABLE": "운영가능", "BEFORE_STARTING": "광고집행전", "TERMINATED": "광고집행종료"}

    @property
    def budget_type(self) -> dict[str,str]:
        return {"DAILY": "일예산", "TOTAL": "총예산"}

    @property
    def bid_type(self) -> dict[str,str]:
        return {
            "COST_CAP": "비용 한도", "BID_CAP": "입찰가 한도", "NO_CAP": "입찰가 한도 없음",
            "CPC": "수동 CPC", "CPM": "수동 CPM", "CPV": "수동 CPV"
        }

    @property
    def placement_group(self) -> dict[str,str]:
        return {
            "M_SMARTCHANNEL": "네이버+ > 스마트채널", "M_FEED": "네이버+ > 피드", "M_MAIN": "네이버+ > 네이버 메인",
            "M_BANNER": "네이버+ > 서비스 통합", "N_SHOPPING": "네이버+ > 쇼핑", "N_COMMUNICATION": "네이버+ > 커뮤니케이션",
            "N_INSTREAM": "네이버+ > 인스트림", "NW_SMARTCHANNEL": "네이버 퍼포먼스 네트워크 > 스마트채널",
            "NW_FEED": "네이버 퍼포먼스 네트워크 > 피드", "NW_BANNER": "네이버 퍼포먼스 네트워크 > 서비스 통합"
        }


class Creative(_MasterReport):
    report_type = "Creative"
    path = "/apis/gfa/v1/adAccounts/{}/creatives/draft/searchByKeyword"

    @_MasterReport.with_session
    @_MasterReport.with_token
    def extract(
            self,
            status: Sequence[Literal["ALL","PENDING","REJECT","ACCEPT","PENDING_IN_OPERATION","REJECT_IN_OPERATION","DELETED"]] = ["ALL","DELETED"],
            **kwargs
        ) -> JsonObject:
        return (self.request_each_pages(self.request_json_safe)
                .partial(account_no=self.account_no)
                .expand(status=status)
                .all_pages(self.count_total, self.max_page_size, self.page_start)
                .run())

    def build_request_params(
            self,
            status: Literal["ALL","PENDING","REJECT","ACCEPT","PENDING_IN_OPERATION","REJECT_IN_OPERATION","DELETED"],
            page: int = 0,
            page_size: int = 100,
            **kwargs
        ) -> list[tuple]:
        return [
            ("page", int(page)),
            ("size", int(page_size)),
            *[("onOffs", str(i)) for i in [1,0]],
            *([("statuses", code) for code in self.status] if status == "ALL" else [("statuses", status)]),
            *[("creativeTypes", code) for code in self.creative_type.keys()],
        ]

    @property
    def status(self) -> dict[str,str]:
        return {
            "PENDING": "검수중", "REJECT": "반려", "ACCEPT": "승인",
            "PENDING_IN_OPERATION": "승인 (수정사항 검수중)", "REJECT_IN_OPERATION": "승인 (수정사항 반려)"
        }

    @property
    def creative_type(self) -> dict[str,str]:
        return {
            "SINGLE_IMAGE": "네이티브 이미지", "MULTIPLE_IMAGE": "컬렉션", "SINGLE_VIDEO": "동영상",
            "IMAGE_BANNER": "이미지 배너", "CATALOG": "카탈로그", "COMPOSITION": "ADVoost 소재"
        }


class PerformanceReport(SearchAdGFA):
    method = "GET"
    path = "/apis/stats/v2/adAccounts/{}/stats/reportPerformanceDetail"
    date_format = "%Y-%m-%d"

    @property
    def default_options(self) -> dict:
        return dict(
            Request = dict(request_delay=1.01),
        )

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.path.format(self.account_no))

    @SearchAdGFA.with_session
    @SearchAdGFA.with_token
    def extract(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            date_type: Literal["TOTAL","DAY","WEEK","MONTH","HOUR"] = "DAY",
            columns: list[str] | Literal[":default:"] = ":default:",
            wait_seconds: int = 60,
            wait_interval: int = 1,
            progress: bool = True,
            **kwargs
        ) -> dict[str,bytes]:
        columns = self.db_columns if columns == ":default:" else columns
        dates = self.generate_date_range(start_date, end_date=(start_date if end_date == ":start_date:" else end_date))
        return self.download(columns, dates, date_type, wait_seconds, wait_interval, progress)

    def download(
            self,
            columns: list[str],
            dates: list[tuple[dt.date,dt.date]],
            date_type: Literal["TOTAL","DAY","WEEK","MONTH","HOUR"] = "DAY",
            wait_seconds: int = 60,
            wait_interval: int = 1,
            progress: bool = True,
        ) -> dict[str,bytes]:
        from linkmerce.utils.progress import import_tqdm
        import time
        tqdm = import_tqdm()

        status = [False] * len(dates)
        for index, (start_date, end_date) in enumerate(tqdm(dates, desc="Requesting performance reports", disable=(not progress))):
            kwargs = dict(start_date=start_date, end_date=end_date, date_type=date_type, columns=columns)
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

    def generate_date_range(self, start_date: dt.date | str, end_date: dt.date | str) -> list[tuple[dt.date,dt.date]]:
        """분석 기간 단위가 '일'인 경우, 조회 기간은 최대 62일을 선택할 수 있습니다."""
        from linkmerce.utils.date import date_split
        return date_split(start_date, end_date, delta=dict(days=60), format=self.date_format)

    def request_report(self, **kwargs) -> bool:
        url = self.origin + f"/apis/gfa/v1/adAccounts/{self.account_no}/report/downloads"
        body = self.build_download_json(**kwargs)
        headers = self.build_request_headers(**kwargs)
        headers["content-type"] = "application/json"
        with self.request("POST", url, json=body, headers=headers) as response:
            return response.json()["success"]

    def wait_reports(self, indices: list[int], wait_seconds: int = 60, wait_interval: int = 1, **kwargs) -> list[dict]:
        from linkmerce.common.exceptions import RequestError
        import time
        url = self.origin + f"/apis/gfa/v1/adAccounts/{self.account_no}/report/downloads"
        params = {"reportType": "PERFORMANCE"}
        headers = self.build_request_headers(**kwargs)
        for _ in range(0, max(wait_seconds, 1), max(wait_interval, 1)):
            time.sleep(wait_interval)
            with self.request("GET", url, params=params, headers=headers) as response:
                downloads = response.json()
                for index in indices:
                    if downloads[index]["status"] != "COMPLETED":
                        continue
                return [downloads[index] for index in indices]
        raise RequestError("Download was not completed within the waiting seconds.")

    def download_excel(self, download_no: int, **kwargs) -> bytes:
        url = self.origin + f"/apis/gfa/v1/adAccounts/{self.account_no}/report/downloads/{download_no}/download"
        headers = self.build_request_headers(**kwargs)
        with self.request("GET", url, headers=headers) as response:
            return response.content

    def delete_report(self, download_no: int, **kwargs) -> bool:
        url = self.origin + f"/apis/gfa/v1/adAccounts/{self.account_no}/report/downloads"
        params = {"reportDownloadNos": download_no, "reportType": "PERFORMANCE"}
        headers = self.build_request_headers(**kwargs)
        with self.request("DELETE", url, params=params, headers=headers) as response:
            return response.json()["success"]

    def query_to_filename(self, report_query: dict) -> str:
        start_date = str(report_query["startDate"]).replace('-', '')
        end_date = str(report_query["endDate"]).replace('-', '')
        return f"ReportDownload_aa_{self.account_no}_PERFORMANCE_{start_date}_{end_date}.csv"

    def build_download_json(
            self,
            columns: list[str],
            start_date: dt.date,
            end_date: dt.date,
            date_type: Literal["TOTAL","DAY","WEEK","MONTH","HOUR"] = "DAY",
            **kwargs
        ) -> dict:
        return {
            "needToNoti": False,
            "reportQuery": {
                "startDate": str(start_date),
                "endDate": str(end_date),
                "reportDateUnit": date_type,
                "placeUnit": "TOTAL",
                "reportAdUnit": "CREATIVE",
                "reportDimension": "TOTAL",
                "colList": columns,
                "adAccountNo": self.account_no,
                "reportFilterList": []
            },
            "reportType": "PERFORMANCE"
        }

    def build_request_headers(self, **kwargs: str) -> dict[str,str]:
        return dict(self.get_request_headers(with_token=True), origin=self.origin, referer=self.referer(**kwargs))

    def referer(self, start_date: dt.date, end_date: dt.date, columns: list[str] = list(), **kwargs) -> str:
        params = '&'.join([f"{key}={value}" for key, value in {
            "startDate": str(start_date),
            "endDate": str(end_date),
            "adUnit": "CREATIVE",
            "dateUnit": "DAY",
            "placeUnit": "TOTAL",
            "dimension": "TOTAL",
            "currentPage": 1,
            "pageSize": 100,
            "filterList": "%5B%5D",
            "showColList": ("%5B%22" + "%22,%22".join(columns if columns else self.db_columns) + "%22%5D"),
            "accessAdAccountNo": self.account_no,
        }.items()])
        return "{}/adAccount/accounts/{}/report/performance?{}".format(self.origin, self.account_no, params)

    @property
    def db_columns(self) -> list[str]:
        return ["sales", "impCount", "clickCount", "reachCount", "convCount", "convSales"]
