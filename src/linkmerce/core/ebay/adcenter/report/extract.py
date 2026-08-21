from __future__ import annotations

from linkmerce.core.ebay.adcenter import GmarketAdCenter, GmarketAdParser

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    import datetime as dt


class Report(GmarketAdCenter):
    """Gmarket 광고센터 상세 리포트를 조회하는 클래스.

    - **Menu**: 리포트 > 상세 리포트
    - **API**: https://adcenter.esmplus.com/report
    - **Referer**: https://adcenter.esmplus.com/report

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        페이지 요청 간 대기 시간(초). 기본값은 `1`
    tqdm_options: dict | None
        페이지 순회 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    path = "/report"
    date_format = "%Y%m%d"
    days_limit = 93
    max_page_size = 1000
    page_start = 1
    default_options = {"PaginateAll": {"request_delay": 1}}

    @GmarketAdCenter.with_session
    def extract(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            report_type: Literal["daily", "campaign", "group", "product", "keyword", "placement", "category"] = "product",
            aggregate_type: Literal["total", "daily"] = "daily",
            **kwargs,
        ) -> list[str]:
        """리포트 유형과 보기 방식에 맞는 상세 리포트를 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        start_date: dt.date | str
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
        end_date: dt.date | str
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
        report_type: str
            리포트 유형
                1. `"daily"`: 일별
                2. `"campaign"`: 캠페인별
                3. `"group"`: 그룹별
                4. `"product"`: 상품별 (기본값)
                5. `"keyword"`: 키워드별
                6. `"placement"`: 노출 영역별
                7. `"category"`: 카테고리별
        aggregate_type: str
            보기 방식
                1. `"total"`: 합계
                2. `"daily"`: 날짜별 (기본값)

        Returns
        -------
        list[str]
            페이지별 상세 리포트 조회 결과
        """
        return (self.paginate_all(
                    self.request_text,
                    counter = self.count_total,
                    max_page_size = self.max_page_size,
                    page_start = self.page_start
                ).run(
                    start_date = start_date,
                    end_date = (start_date if end_date == ":start_date:" else end_date),
                    report_type = self.index_report_type(report_type),
                    aggregate_type = self.index_aggregate_type(aggregate_type),
                ))

    def count_total(self, response: str, **kwargs) -> int:
        """HTTP 응답에서 전체 행 수를 추출한다."""
        return GmarketAdParser().parse(response).get("totalCount", 0)

    def build_request_json(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str,
            report_type: int,
            aggregate_type: int,
            page: int = 1,
            page_size: int = 1000,
            **kwargs,
        ) -> list[dict]:
        """상세 리포트 조회 요청 본문을 구성한다."""
        return [{
            "selectStartDate": str(start_date).replace('-', ''),
            "selectEndDate": str(end_date).replace('-', ''),
            "page": page,
            "pageSize": page_size,
            "reportType": report_type,
            "aggregateType": aggregate_type,
            "selectMetric": self.metrics,
            "campaignGroupTypeList": [107020, 109020, 111020],
        }]

    def build_request_headers(self, **kwargs) -> dict[str, str]:
        return self.get_request_headers() | {
            "next-action": "40d2626681cec51a59d422426865c41a67cb82d20f",
            "referer": (self.origin + "/report"),
        }

    def index_report_type(self, report_type: str) -> int:
        report_types = ["daily", "campaign", "group", "product", "keyword", "placement", "category"]
        return report_types.index(report_type) + 1

    def index_aggregate_type(self, aggregate_type: str) -> int:
        aggregate_types = ["total", "daily"]
        return aggregate_types.index(aggregate_type) + 1

    @property
    def metrics(self) -> list[str]:
        """조회할 필드 목록을 반환한다."""
        return [
            "impressions", "clicks", "ctr", "cpc", "spend", "store_revenue", "roas",
            "store_orders", "store_unit_sold", "store_cvr", "store_a2c", "sku_revenue",
            "product_orders", "unit_sold", "product_cvr", "a2c"
        ]

    @property
    def report_type(self) -> dict[str, str]:
        """리포트 유형 코드와 한글명 매핑을 반환한다."""
        return {
            "daily": "일별", "campaign": "캠페인별", "group": "그룹별", "product": "상품별",
            "keyword": "키워드별", "placement": "노출 영역별", "category": "카테고리별"
        }


class ReportDownload(Report):
    """Gmarket 광고센터 상세 리포트를 엑셀 파일로 다운로드하는 클래스.

    - **Menu**: 리포트 > 상세 리포트 > 엑셀 다운로드
    - **API**: https://adcenter.esmplus.com/report
    - **Referer**: https://adcenter.esmplus.com/report

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.
    """

    method = "POST"
    path = "/report"
    date_format = "%Y%m%d"
    days_limit = 93
    default_options = dict()

    @GmarketAdCenter.with_session
    def extract(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            report_type: Literal["daily", "campaign", "group", "product", "keyword", "placement", "category"] = "product",
            aggregate_type: Literal["total", "daily"] = "daily",
            **kwargs,
        ) -> dict[str, bytes]:
        """리포트 유형과 보기 방식에 맞는 상세 리포트를 엑셀 파일로 다운로드한다.

        Parameters
        ----------
        start_date: dt.date | str
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
        end_date: dt.date | str
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
        report_type: str
            리포트 유형
                1. `"daily"`: 일별
                2. `"campaign"`: 캠페인별
                3. `"group"`: 그룹별
                4. `"product"`: 상품별 (기본값)
                5. `"keyword"`: 키워드별
                6. `"placement"`: 노출 영역별
                7. `"category"`: 카테고리별
        aggregate_type: str
            보기 방식
                - `"total"`: 합계
                - `"daily"`: 날짜별 (기본값)

        Returns
        -------
        dict[str, bytes]
            `{파일명: 엑셀 바이너리}` 구조의 상세 리포트 다운로드 결과
        """
        response = self.request_text(
            start_date = start_date,
            end_date = (start_date if end_date == ":start_date:" else end_date),
            report_type = self.index_report_type(report_type),
            aggregate_type = self.index_aggregate_type(aggregate_type),
        )
        download_url = GmarketAdParser().parse(response)["data"][0]["downloadUrl"]
        return {self.get_file_name(download_url): self.download_report(download_url)}

    def download_report(self, download_url: str) -> bytes:
        """다운로드 주소로부터 엑셀 파일을 다운로드 받는다."""
        from linkmerce.utils.headers import build_headers
        headers = build_headers(host=download_url, referer=(self.origin + "/report"), https=True)
        with self.request("GET", download_url, headers=headers) as response:
            response.raise_for_status()
            return self.parse(response.content)

    def get_file_name(self, download_url: str) -> str:
        """다운로드 주소로부터 엑셀 파일명을 추출한다."""
        return download_url.split('?')[0].split('/')[-1]

    def build_request_json(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str,
            report_type: int,
            aggregate_type: int,
            **kwargs,
        ) -> list[dict]:
        return [{
            "reportTypeList": [report_type],
            "selectStartDate": str(start_date).replace('-', ''),
            "selectEndDate": str(end_date).replace('-', ''),
            "aggregateType": aggregate_type,
            "campaignGroupTypeList": [107020, 109020, 111020],
        }]

    def build_request_headers(self, **kwargs) -> dict[str, str]:
        return self.get_request_headers() | {
            "next-action": "60c5f67c5af4b4c453e627c6bf0df7213ec76c3ee8",
            "referer": (self.origin + "/report"),
        }
