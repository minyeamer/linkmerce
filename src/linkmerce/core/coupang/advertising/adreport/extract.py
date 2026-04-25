from __future__ import annotations
from linkmerce.core.coupang.advertising import CoupangAds

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class Campaign(CoupangAds):
    """쿠팡 광고센터 캠페인 목록을 조회하는 클래스.

    - **Menu**: 광고 관리 > 매출 성장 / 신규 구매 고객 확보 / 인지도 상승
    - **API**: https://advertising.coupang.com/marketing/tetris-api/campaigns
    - **Referer**: https://advertising.coupang.com/marketing/dashboard/sales

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `1`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    path = "/marketing/tetris-api/campaigns"
    max_page_size = 20
    page_start = 0
    date_format = "%Y%m%d"
    default_options = {"PaginateAll": {"request_delay": 1}}

    @CoupangAds.with_session
    def extract(
            self,
            goal_type: Literal["SALES", "NCA", "REACH"] = "SALES",
            is_deleted: bool = False,
            vendor_id: str | None = None,
            **kwargs
        ) -> JsonObject:
        """광고 목표(`goal_type`)별 캠페인 목록을 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        goal_type: Literal["SALES", "NCA", "REACH"]
            조회할 광고 목표
                - `"SALES"`: 매출 성장
                - `"NCA"`: 신규 구매 고객 확보
                - `"REACH"`: 인지도 상승
        is_deleted: bool
            삭제된 캠페인 조회 여부. 기본값은 `False`
        vendor_id: str | None
            업체 코드. 조회 시점에는 사용되지 않고 파서 함수에 전달된다.

        Returns
        -------
        list[dict]
            전체 또는 삭제된 캠페인 목록
        """
        return (self.paginate_all(self.request_json_with_timeout, self.count_total, self.max_page_size, self.page_start)
                .run(goal_type=goal_type, is_deleted=is_deleted, vendor_id=vendor_id, **kwargs))

    def count_total(self, response: JsonObject, **kwargs) -> int:
        """HTTP 응답에서 전체 캠페인 수를 추출한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(response, ["pageInfo", "totalCount"])

    def request_json_with_timeout(self, max_retries: int = 5, **kwargs) -> JsonObject:
        """요청 후 타임아웃(Timeout)이 발생하면 `max_retries` 횟수만큼 성공할 때까지 재시도한다."""
        from requests.exceptions import Timeout
        import random
        session = self.get_session()
        message = self.build_request_message(**kwargs)
        for retry_count in range(1, max_retries+1):
            try:
                with session.request(**message, timeout=random.randint(30, 60)) as response:
                    return response.json()
            except Timeout as error:
                if retry_count == max_retries:
                    raise error

    def build_request_json(
            self,
            goal_type: Literal["SALES", "NCA", "REACH"] = "SALES",
            page: int = 0,
            size: int = 20,
            is_deleted: bool = False,
            **kwargs
        ) -> dict:
        return {
            "isDeleted": is_deleted,
            "pagination": {"page": page, "size": size},
            "sortedBy": "ID",
            "isSortDesc": "DESC",
            "budgetTypes": None,
            "isActive": None,
            "name": "",
            "creationContext": None,
            "objective": None,
            "primaryOrderBy": "DEFAULT",
            "goalType": goal_type,
            "targetCampaignId": None,
            "vendorItemId": None
        }


class Creative(CoupangAds):
    """쿠팡 광고센터 신규 구매 고객 확보(NCA) 캠페인의 소재 정보를 조회하는 클래스.

    - **Menu**: 광고 관리 > 신규 구매 고객 확보 > 캠페인 > 광고
    - **API**: https://advertising.coupang.com/marketing/tetris-api/nca/campaign/{campaign_id}
    - **Referer**: https://advertising.coupang.com/marketing/dashboard/nca

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간. 기본값은 `0.3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    path = "/marketing/tetris-api/nca/campaign/{}"
    max_page_size = 20
    page_start = 0
    date_format = "%Y%m%d"
    default_options = {"RequestEach": {"request_delay": 0.3}}

    @CoupangAds.with_session
    def extract(self, campaign_ids: Sequence[int | str], vendor_id: str | None = None, **kwargs) -> JsonObject:
        """캠페인(`campaign_ids`)별 NCA 소재 목록을 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        campaign_ids: Sequence[int | str]
            조회할 캠페인 ID 목록
        vendor_id: str | None
            업체 코드. 조회 시점에는 사용되지 않고 파서 함수에 전달된다.

        Returns
        -------
        list[dict]
            신규 구매 고객 확보 캠페인별 소재 정보 목록
        """
        return (self.request_each(self.request_json_safe)
                .partial(vendor_id=vendor_id)
                .expand(campaign_id=campaign_ids)
                .run())

    def build_request_message(self, campaign_id: int | str, **kwargs) -> dict:
        """각 HTTP 요청마다 URL에 캠페인 ID를 포맷팅한다."""
        kwargs["url"] = self.url.format(campaign_id)
        return super().build_request_message(**kwargs)

    def set_request_headers(self, **kwargs):
        referer = self.origin + "/marketing/dashboard/nca"
        return super().set_request_headers(contents="json", origin=self.origin, referer=referer, **kwargs)


class _AdReport(CoupangAds):
    """쿠팡 광고센터 광고 보고서를 생성 및 다운로드하는 공통 클래스.

    - **Menu**: 광고보고서 > 광고 보고서 > 매출 성장 / 신규 구매 고객 확보 / 인지도 상승 / 디스플레이광고
    - **API**: https://advertising.coupang.com/marketing-reporting/v2/graphql
    - **Referer**: https://advertising.coupang.com/marketing-reporting/billboard/one-pager

    GraphQL API로 보고서를 요청하고 엑셀 파일로 다운로드한다.

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.
    """

    method = "POST"
    path = "/marketing-reporting/v2/graphql"
    date_format = "%Y%m%d"
    days_limit = 30
    report_type: Literal["pa", "nca"]

    @CoupangAds.with_session
    def extract(
            self,
            start_date: dt.date | str, 
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            date_type: Literal["total", "daily"] = "daily",
            report_level: Literal["campaign", "adGroup", "ad", "vendorItem", "keyword", "creative"] = "vendorItem",
            campaign_ids: Sequence[int | str] = list(),
            vendor_id: str | None = None,
            wait_seconds: int = 60,
            wait_interval: int = 1,
            **kwargs
        ) -> dict[str, bytes]:
        """광고 보고서를 생성 및 다운로드하여 `{파일명: 엑셀 바이너리}` 형식으로 반환한다.

        Parameters
        ----------
        start_date: dt.date | str
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 전달한다.
        end_date: dt.date | str
            조회 종료일. `":start_date:"` 전달 시 `start_date`와 동일한 날짜로 대체된다.
            기본값은 `":start_date:"`
        date_type: Literal["total", "daily"]
            보고서 기간 구분
                - `"total"`: 합계
                - `"daily"`: 일별
        report_level: Literal["campaign", "adGroup", "ad", "vendorItem", "keyword", "creative"]
            보고서 구조. 보고서 유형(`report_type`)에 따라 지원하는 값이 다르다.
                - 매출 성장(`pa`): `campaign`, `adGroup`, `vendorItem`, `keyword`
                - 신규 구매 고객 확보(`nca`): `campaign`, `ad`, `keyword`, `creative`
        campaign_ids: Sequence[int | str]
            조회할 캠페인 ID 목록. 생략 시 기간 내 전체 캠페인을 조회하여 선택한다.
        vendor_id: str | None
            업체 코드. 조회 시점에는 사용되지 않고 파서 함수에 전달된다.
        wait_seconds: int
            보고서 생성 완료를 기다리는 최대 시간(초). 기본값은 `60`   
            시간 내 보고서가 생성 완료되지 않으면 `ValueError`를 발생시킨다.
        wait_interval: int
            보고서 생성 완료 여부를 확인하는 조회 간격(초). 기본값은 `1`

        Returns
        -------
        dict[str, bytes]
            `{파일명: 엑셀 바이너리}` 형식의 다운로드 결과.   
            파일명은 `<vendor_id>_<report_type>_<date_type>_<report_level>_<start_date>_<end_date>.xlsx` 형식을 따른다.
        """
        start_date = self.to_date(start_date)
        end_date = self.to_date(start_date if end_date == ":start_date:" else end_date)

        if not campaign_ids:
            campaign_ids = self.fetch_campaign_ids(start_date, end_date)
        campaign_ids = list(map(str, campaign_ids))

        if not campaign_ids:
            print(f"No campaigns or data found for the period: '{start_date}' - '{end_date}'")
            return dict()

        report = self.request_report(start_date, end_date, date_type, report_level, campaign_ids=campaign_ids)
        report_id = report["data"]["requestReport"]["id"]

        self.wait_report(report_id, wait_seconds, wait_interval)
        file_name = f"{vendor_id or 'A00000000'}_{self.report_type}_{date_type}_{report_level}_{start_date}_{end_date}.xlsx"
        return {file_name: self.download_excel(report_id, vendor_id)}

    def fetch_dashboard(self):
        """광고 보고서 대시보드로 이동한다."""
        super().fetch_dashboard()
        url = self.origin + "/marketing-reporting/billboard"
        headers = self.build_request_headers()
        headers["referer"] = url + "/reports"
        self.request("GET", url, headers=headers)

    def fetch_campaign_ids(self, start_date: int, end_date: int) -> list[str]:
        """기간 내 활성 캠페인 ID 목록을 GraphQL로 조회한다."""
        body = self.build_campaign_body(start_date, end_date)
        with self.request(self.method, self.url, json=body, headers=self.build_request_headers()) as response:
            return [row["id"] for row in response.json()[0]["data"]["getCampaignList"]]

    def request_report(
            self,
            start_date: int,
            end_date: int,
            date_type: Literal["total", "daily"] = "daily",
            report_level: Literal["campaign", "adGroup", "vendorItem", "keyword", "ad", "creative"] = "vendorItem",
            campaign_ids: list[str] = list(),
        ) -> dict:
        """광고 보고서 생성을 GraphQL로 요청한다."""
        body = self.build_mutation_body(start_date, end_date, date_type, report_level, campaign_ids)
        with self.request(self.method, self.url, json=body, headers=self.build_request_headers()) as response:
            return reports[0] if (reports := response.json()) else dict()

    def wait_report(self, report_id: str, wait_seconds: int = 60, wait_interval: int = 1) -> bool:
        """보고서 생성 요청 후 완료 여부를 주기적으로 확인하면서 대기한다."""
        import time
        for _ in range(0, max(wait_seconds, 1), max(wait_interval, 1)):
            time.sleep(wait_interval)
            for report in self.list_report():
                if isinstance(report, dict) and (report["id"] == report_id):
                    if report["status"] == "completed":
                        return True
        raise ValueError("Failed to create the marketing report.")

    def list_report(self, page: int = 1, page_size: int = 10, duration: int = 90) -> list[dict]:
        """생성된 보고서 목록을 GraphQL로 조회한다."""
        body = self.build_query_body(page=page, paege_size=page_size, duration=duration)
        with self.request(self.method, self.url, json=body, headers=self.build_request_headers()) as response:
            data = response.json()
            try:
                return data[0]["data"]["reportList"]["reports"]
            except:
                return list()

    def download_excel(self, report_id: str, vendor_id: str | None = None) -> bytes:
        """생성된 보고서 엑셀 파일을 다운로드한다."""
        url = self.origin + f"/marketing-reporting/v2/api/excel-report?id={report_id}"
        with self.request("GET", url, headers=self.build_request_headers()) as response:
            return self.parse(response.content, vendor_id=vendor_id)

    def build_mutation_body(
            self,
            start_date: int,
            end_date: int,
            date_type: Literal["total", "daily"] = "daily",
            report_level: Literal["campaign", "adGroup", "vendorItem", "keyword", "ad", "creative"] = "vendorItem",
            campaign_ids: list[str] = list(),
        ) -> list[dict]:
        """보고서 생성을 위한 GraphQL 요청 본문을 구성한다."""
        from linkmerce.utils.graphql import GraphQLOperation, GraphQLSelection, GraphQLFragment

        variables = {
            "startDate": start_date,
            "endDate": end_date,
            "campaignIds": campaign_ids,
            "reportType": self.report_type,
            "dateGroup": date_type,
            "granularity": report_level,
            "excludeIfNoClickCount": False,
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
        ).generate_body(query_options = {
            "command": "mutation",
            "selection": {"variables": {"linebreak": True}, "fields": {"linebreak": True}},
            "suffix": '\n',
        })]

    def build_query_body(self, page: int = 1, paege_size: int = 10, duration: int = 90) -> list[dict]:
        """보고서 목록 조회를 위한 GraphQL 요청 본문을 구성한다."""
        from linkmerce.utils.graphql import GraphQLOperation, GraphQLSelection, GraphQLFragment

        variables = {
            "reportType": self.report_type,
            "page": page,
            "pageSize": paege_size,
            "duration": duration,
            "onlyScheduledReport": False,
        }

        types = {
            "reportType": "ReportType!",
            "page": "Int!",
            "pageSize": "Int!",
            "duration": "Int!",
            "onlyScheduledReport": "Boolean",
        }

        return [GraphQLOperation(
            operation = str(),
            variables = variables,
            types = types,
            selection = GraphQLSelection(
                name = "reportList",
                variables = dict(data=list(variables.keys())),
                fields = GraphQLFragment("ReportList", "ReportList", fields=self.report_list_fields),
            ),
        ).generate_body(query_options = {
            "command": "query",
            "selection": {"variables": {"linebreak": True}, "fields": {"linebreak": True}},
            "suffix": '\n',
        })]

    def build_campaign_body(self, start_date: int, end_date: int) -> list[dict]:
        """캠페인 목록 조회를 위한 GraphQL 요청 본문을 구성한다."""
        from linkmerce.utils.graphql import GraphQLOperation, GraphQLSelection

        variables = {"startDate": start_date, "endDate": end_date, "reportType": self.report_type}
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
        ).generate_body(query_options = {
            "selection": {"variables": {"linebreak": True}, "fields": {"linebreak": True}},
            "suffix": '\n',
        })]

    def set_request_headers(self, **kwargs):
        super().set_request_headers(
            authority = self.origin,
            contents = "json",
            origin = self.origin,
            referer = self.origin + f"/marketing-reporting/billboard/reports/{self.report_type}",
            **kwargs
        )

    def to_date(self, date: dt.date | str) -> int:
        """날짜를 `YYYYMMDD` 정수로 변환한다."""
        return int(str(date).replace('-', ''))

    @property
    def report_type(self) -> dict[str, str]:
        return {"pa": "매출 성장 광고 보고서", "nca": "신규 구매 고객 확보 광고 보고서"}

    @property
    def date_type(self) -> dict[str, str]:
        return {"total": "합계", "daily": "일별"}

    @property
    def report_level(self) -> dict[str, dict[str, str]]:
        return {
            "pa": {
                "campaign": "캠페인",
                "adGroup": "캠페인 > 광고그룹",
                "vendorItem": "캠페인 > 광고그룹 > 상품",
                "keyword": "캠페인 > 광고그룹 > 상품 > 키워드",
            },
            "nca": {
                "campaign": "캠페인",
                "ad": "캠페인 > 광고",
                "keyword": "캠페인 > 광고 > 키워드",
                "creative": "캠페인 > 광고 > 키워드 > 소재",
            },
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


class ProductAdReport(_AdReport):
    """쿠팡 매출 성장 광고 보고서를 생성 및 다운로드하는 클래스.

    - **Menu**: 광고보고서 > 광고 보고서 > 매출 성장 광고 보고서
    - **API**: https://advertising.coupang.com/marketing-reporting/v2/graphql
    - **Referer**: https://advertising.coupang.com/marketing-reporting/billboard/reports/pa

    GraphQL API로 보고서를 요청하고 엑셀 파일로 다운로드한다.

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.
    """

    report_type = "pa"


class NewCustomerAdReport(_AdReport):
    """쿠팡 신규 구매 고객 확보 광고 보고서를 생성 및 다운로드하는 클래스.

    - **Menu**: 광고보고서 > 광고 보고서 > 신규 구매 고객 확보 광고 보고서
    - **API**: https://advertising.coupang.com/marketing-reporting/v2/graphql
    - **Referer**: https://advertising.coupang.com/marketing-reporting/billboard/reports/nca

    GraphQL API로 보고서를 요청하고 엑셀 파일로 다운로드한다.

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.
    """

    report_type = "nca"
