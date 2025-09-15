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
            Request = dict(request_delay=0.3),
            RequestEach = dict(request_delay=0.3),
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
            **kwargs
        ) -> JsonObject:
        """분석 기간 단위가 '일'인 경우, 조회 기간은 최대 62일을 선택할 수 있습니다."""
        dates = dict(start_date=start_date, end_date=(start_date if end_date == ":start_date:" else end_date))
        return (self.request_each(self.request_json_safe)
                .partial(**dates)
                .expand(creative_no=self.fetch_creative(**dates))
                .run())

    def fetch_creative(self, start_date: dt.date | str, end_date: dt.date | str) -> list[int]:
        import json
        import time
        creative, current_page, total_page = list(), 1, 1
        url = self.origin + f"/apis/stats/v1/adAccounts/{self.account_no}/stats/reportPerformance"
        while current_page <= total_page:
            params = {
                "startDate": str(start_date),
                "endDate": str(end_date),
                "reportAdUnit": "CREATIVE",
                "reportFilterListString": [],
                "pageNumber": current_page,
                "pageSize": 100,
            }
            headers = self.build_request_headers(start_date, end_date)
            with self.request("GET", url, params=params, headers=headers) as response:
                body = json.loads(response.text)
                creative += [row["creativeNo"] for row in body["reportPerformanceDetailResponseList"]]
                current_page += 1
                total_page = int(body["totalPage"])
            time.sleep(self.get_options("Request").get("request_delay") or 1)
        return creative

    def build_request_params(
            self,
            creative_no: int,
            start_date: dt.date | str,
            end_date: dt.date | str,
            **kwargs
        ) -> dict:
        return {
            "adUnitNo": creative_no,
            "startDate": str(start_date),
            "endDate": str(end_date),
            "reportAdUnit": "CREATIVE",
            "reportDateUnit": "DAY",
            "placeUnit": str(),
            "reportDimension": "TOTAL",
        }

    def build_request_headers(self, start_date: dt.date | str, end_date: dt.date | str, **kwargs: str) -> dict[str,str]:
        return dict(self.get_request_headers(with_token=True), referer=self.referer(start_date, end_date))

    def referer(self, start_date: dt.date | str, end_date: dt.date | str) -> str:
        columns = ["result", "sales_per_result", "sales", "imp_count", "cpm", "click_count", "cpc", "ctr", "cvr", "roas"]
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
            "showColList": ("%5B%22" + "%22,%22".join(columns) + "%22%5D"),
            "accessAdAccountNo": self.account_no,
        }.items()])
        return "{}/adAccount/accounts/{}/report/performance?{}".format(self.origin, self.account_no, params)
