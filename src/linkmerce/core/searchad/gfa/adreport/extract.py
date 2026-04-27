from __future__ import annotations
from linkmerce.core.searchad.gfa import SearchAdGfa

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class _MasterReport(SearchAdGfa):
    """네이버 성과형 디스플레이 광고 리포트를 조회하는 공통 클래스.

    - **URL**: https://ads.naver.com

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    account_no: int | str
        성과형 디스플레이 광고 계정 번호

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        페이지별 요청 간 대기 시간. 기본값은 `0.3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachPages` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        매개변수별 요청 간 대기 시간. 기본값은 `0.3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    report_type: Literal["Campaign", "AdSet", "Creative"]
    method = "GET"
    max_page_size = 100
    page_start = 0
    default_options = {
        "PaginateAll": {"request_delay": 0.3},
        "RequestEachPages": {"request_delay": 0.3},
    }

    def count_total(self, response: JsonObject, **kwargs) -> int:
        """HTTP 응답에서 전체 항목 수를 추출한다."""
        return response.get("totalElements") if isinstance(response, dict) else None


class Campaign(_MasterReport):
    """네이버 성과형 디스플레이 광고 캠페인 목록을 조회하는 클래스.

    - **Menu**: 디스플레이 광고 > 광고 관리
    - **API**: https://ads.naver.com/apis/gfa/v1/adAccounts/{account_no}/campaigns
    - **Referer**: https://ads.naver.com/manage/ad-accounts/{account_no}/da/campaigns-by/{campaign_type}

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    account_no: int | str
        성과형 디스플레이 광고 계정 번호

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        페이지별 요청 간 대기 시간. 기본값은 `0.3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachPages` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        매개변수별 요청 간 대기 시간. 기본값은 `0.3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    report_type = "Campaign"
    version = "v1.2"
    path = "/campaigns"

    @_MasterReport.with_session
    def extract(
            self,
            status: Sequence[Literal["RUNNABLE", "DELETED"]] = ["RUNNABLE", "DELETED"],
            **kwargs
        ) -> JsonObject:
        """캠페인 목록을 상태별로 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        status: Sequence[str]
            캠페인 상태 목록
                - `"RUNNABLE"`: 운영가능
                - `"DELETED"`: 삭제

        Returns
        -------
        list[dict]
            캠페인 목록
        """
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
    """네이버 성과형 디스플레이 광고 그룹 목록을 조회하는 클래스.

    - **Menu**: 디스플레이 광고 > 광고 관리 > 캠페인
    - **API**: https://ads.naver.com/apis/gfa/v1/adAccounts/{account_no}/adSets
    - **Referer**: https://ads.naver.com/manage/ad-accounts/{account_no}/da/dashboard/campaign/{campaign_id}

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    account_no: int | str
        성과형 디스플레이 광고 계정 번호

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        페이지별 요청 간 대기 시간. 기본값은 `0.3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachPages` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        매개변수별 요청 간 대기 시간. 기본값은 `0.3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    report_type = "AdSet"
    version = "v1.2"
    path = "/adSets"

    @_MasterReport.with_session
    def extract(
            self,
            status: Sequence[Literal["ALL", "RUNNABLE", "BEFORE_STARTING", "TERMINATED", "DELETED"]] = ["ALL", "DELETED"],
            **kwargs
        ) -> JsonObject:
        """광고 그룹 목록을 상태별로 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        status: Sequence[str]
            광고 그룹 상태 목록
                - `"ALL"`: 모든 상태
                - `"RUNNABLE"`: 운영가능
                - `"BEFORE_STARTING"`: 광고집행전
                - `"TERMINATED"`: 광고집행종료
                - `"DELETED"`: 삭제

        Returns
        -------
        list[dict]
            광고 그룹 목록
        """
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
        """광고 그룹 상태 매핑을 반환한다."""
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
    """네이버 성과형 디스플레이 광고 소재 목록을 조회하는 클래스.

    - **Menu**: 디스플레이 광고 > 광고 관리 > 캠페인 > 광고 그룹
    - **API**: https://ads.naver.com/apis/gfa/v1/adAccounts/{account_no}/creatives/draft/searchByKeyword
    - **Referer**: https://ads.naver.com/manage/ad-accounts/{account_no}/da/dashboard/campaign/{campaign_id}/assetGroup/{adset_id}

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    account_no: int | str
        성과형 디스플레이 광고 계정 번호

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        페이지별 요청 간 대기 시간. 기본값은 `0.3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachPages` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        매개변수별 요청 간 대기 시간. 기본값은 `0.3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    report_type = "Creative"
    version = "v1"
    path = "/creatives/draft/searchByKeyword"

    @_MasterReport.with_session
    def extract(
            self,
            status: Sequence[Literal["ALL", "PENDING", "REJECT", "ACCEPT", "PENDING_IN_OPERATION", "REJECT_IN_OPERATION", "DELETED"]] = ["ALL", "DELETED"],
            **kwargs
        ) -> JsonObject:
        """소재 목록을 검수 상태별로 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        status: Sequence[str]
            소재 검수 상태 목록
                - `"ALL"`: 모든 상태
                - `"PENDING"`: 검수중
                - `"REJECT"`: 반려
                - `"ACCEPT"`: 승인
                - `"PENDING_IN_OPERATION"`: 승인 (수정사항 검수중)
                - `"REJECT_IN_OPERATION"`: 승인 (수정사항 반려)
                - `"DELETED"`: 삭제

        Returns
        -------
        list[dict]
            소재 목록
        """
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
    """네이버 성과형 디스플레이 광고 성과 보고서를 다운로드하는 클래스.

    - **Menu**: 디스플레이 광고 > 보고서 > 성과 보고서 > 성과 보고서 > 다운로드 요청
    - **API**: https://ads.naver.com/apis/gfa/v1/adAccounts/{account_no}/report/downloads
    - **Referer**: https://ads.naver.com/manage/ad-accounts/{account_no}/da/report/performance

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    account_no: int | str
        성과형 디스플레이 광고 계정 번호
    """

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
        """성과 보고서를 생성하고 엑셀 파일로 다운로드한다.

        날짜 범위를 최대 60일 단위로 분할하여 보고서를 요청하고, 다운로드한 보고서는 삭제한다.

        Parameters
        ----------
        start_date: dt.date | str
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
        end_date: dt.date | str | Literal[":start_date:"]
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
        date_type: Literal[str]
            기간 단위
                - `"TOTAL"`: 전체
                - `"DAY"`: 일 (기본값)
                - `"WEEK"`: 주
                - `"MONTH"`: 월
                - `"HOUR"`: 시간
        columns: list[str] | Literal[":default:"]
            열 맞춤 설정
                - `":default:"`: `db_columns` 속성 (기본값)
        ad_unit: Literal[str]
            분석 단위
                - `"AD_ACCOUNT"`: 광고 계정
                - `"CAMPAIGN"`: 캠페인
                - `"AD_SET"`: 광고 그룹
                - `"ASSET_GROUP"`: 애셋 그룹
                - `"CREATIVE"`: 광고 소재
        wait_seconds: int
            보고서 생성 완료를 기다리는 최대 시간(초). 기본값은 `60`   
            시간 내 보고서가 생성 완료되지 않으면 `RequestError`를 발생시킨다.
        wait_interval: int
            보고서 생성 완료 여부를 확인하는 조회 간격(초). 기본값은 `1`
        progress: bool
            다운로드 진행도 출력 여부. 기본값은 `True`

        Returns
        -------
        dict[str, str]
            `{파일명: 엑셀 바이너리}` 형식의 다운로드 결과.   
            파일명은 `ReportDownload_aa_{account_no}_PERFORMANCE_{start_date}_{end_date}.csv` 형식을 따른다.
        """
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
        """분석 기간 단위가 '일'인 경우, 요청 당 조회 기간은 최대 60일로 제한한다."""
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
    """네이버 성과형 디스플레이 광고 캠페인 성과 보고서를 다운로드하는 클래스.

    - **Menu**: 디스플레이 광고 > 보고서 > 성과 보고서 > 성과 보고서 > 캠페인 > 다운로드 요청
    - **API**: https://ads.naver.com/apis/gfa/v1/adAccounts/{account_no}/report/downloads
    - **Referer**: https://ads.naver.com/manage/ad-accounts/{account_no}/da/report/performance

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    account_no: int | str
        성과형 디스플레이 광고 계정 번호
    """

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
        """캠페인 성과 보고서를 생성하고 엑셀 파일로 다운로드한다.

        날짜 범위를 최대 60일 단위로 분할하여 보고서를 요청하고, 다운로드한 보고서는 삭제한다.

        Parameters
        ----------
        start_date: dt.date | str
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
        end_date: dt.date | str | Literal[":start_date:"]
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
        date_type: Literal[str]
            기간 단위
                - `"TOTAL"`: 전체
                - `"DAY"`: 일 (기본값)
                - `"WEEK"`: 주
                - `"MONTH"`: 월
                - `"HOUR"`: 시간
        columns: list[str] | Literal[":default:"]
            열 맞춤 설정
                - `":default:"`: 총비용, 노출수, 클릭수, 총 전환수, 총 전환매출액
        wait_seconds: int
            보고서 생성 완료를 기다리는 최대 시간(초). 기본값은 `60`   
            시간 내 보고서가 생성 완료되지 않으면 `RequestError`를 발생시킨다.
        wait_interval: int
            보고서 생성 완료 여부를 확인하는 조회 간격(초). 기본값은 `1`
        progress: bool
            다운로드 진행도 출력 여부. 기본값은 `True`

        Returns
        -------
        dict[str, bytes]
            `{파일명: 엑셀 바이너리}` 형식의 다운로드 결과
        """
        return super().extract(
            start_date, end_date, date_type, columns, "CAMPAIGN", wait_seconds, wait_interval, progress, **kwargs)

    @property
    def db_columns(self) -> list[str]:
        """캠페인 성과 리포트 측정값 칼럼 목록을 반환한다."""
        return ["sales", "impCount", "clickCount", "convCount", "convSales"]


class CreativeReport(PerformanceReport):
    """네이버 성과형 디스플레이 광고 소재 성과 보고서를 다운로드하는 클래스.

    - **Menu**: 디스플레이 광고 > 보고서 > 성과 보고서 > 성과 보고서 > 광고 소재 > 다운로드 요청
    - **API**: https://ads.naver.com/apis/gfa/v1/adAccounts/{account_no}/report/downloads
    - **Referer**: https://ads.naver.com/manage/ad-accounts/{account_no}/da/report/performance

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    account_no: int | str
        성과형 디스플레이 광고 계정 번호
    """

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
        """광고 소재 성과 보고서를 생성하고 엑셀 파일로 다운로드한다.

        날짜 범위를 최대 60일 단위로 분할하여 보고서를 요청하고, 다운로드한 보고서는 삭제한다.

        Parameters
        ----------
        start_date: dt.date | str
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
        end_date: dt.date | str | Literal[":start_date:"]
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
        date_type: Literal[str]
            기간 단위
                - `"TOTAL"`: 전체
                - `"DAY"`: 일 (기본값)
                - `"WEEK"`: 주
                - `"MONTH"`: 월
                - `"HOUR"`: 시간
        columns: list[str] | Literal[":default:"]
            열 맞춤 설정
                - `":default:"`: 총비용, 노출수, 클릭수, 총 전환수, 총 전환매출액
        wait_seconds: int
            보고서 생성 완료를 기다리는 최대 시간(초). 기본값은 `60`   
            시간 내 보고서가 생성 완료되지 않으면 `RequestError`를 발생시킨다.
        wait_interval: int
            보고서 생성 완료 여부를 확인하는 조회 간격(초). 기본값은 `1`
        progress: bool
            다운로드 진행도 출력 여부. 기본값은 `True`

        Returns
        -------
        dict[str, bytes]
            `{파일명: 엑셀 바이너리}` 형식의 다운로드 결과
        """
        return super().extract(
            start_date, end_date, date_type, columns, "CREATIVE", wait_seconds, wait_interval, progress, **kwargs)

    @property
    def db_columns(self) -> list[str]:
        """소재 성과 리포트 측정값 칼럼 목록을 반환한다."""
        return ["sales", "impCount", "clickCount", "convCount", "convSales"]
