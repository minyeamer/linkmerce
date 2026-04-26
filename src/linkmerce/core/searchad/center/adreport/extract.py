from __future__ import annotations
from linkmerce.core.searchad.center import SearchAdCenter

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class AdvancedReport(SearchAdCenter):
    """네이버 검색광고 시스템에서 다차원 보고서를 다운로드하는 클래스.

    - **Menu**: 검색 광고 > 보고서 > 다차원 보고서 > 보고서 형식 저장
    - **API**: https://ads.naver.com/apis/sa/api/advanced-report/downloads
    - **Referer**: https://ads.naver.com/manage/ad-accounts/{account_no}/sa/reports

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    account_no: int | str
        검색광고 계정 번호
    customer_id: int | str
        검색광고 고객 ID
    """

    method = "POST"
    path = "/advanced-report/downloads"
    date_format = "%Y-%m-%d"
    days_limit = 731

    @SearchAdCenter.with_session
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
        ) -> JsonObject | str:
        """다차원 보고서를 CSV 문자열 형식으로 다운로드 받는다.

        Parameters
        ----------
        report_id: str
            다차원 보고서 ID. 다차원 보고서 화면의 상세 URL 경로에 포함된 값이다.
        report_name: str
            보고서 이름
        userid: str
            네이버 아이디
        attributes: Iterable[str]
            보고서 항목 목록의 구분 목록
            - 예: 캠페인, 광고그룹, 소재, 매체이름, PC/모바일 매체, 검색/콘텐츠 매체, 일별 등
        fields: Iterable[str]
            보고서 항목 목록의 성과 목록
            - 예: 노출수, 클릭수, 총비용, 총 전환수, 평균노출순위 등
        start_date: dt.date | str
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식 문자열을 전달한다.
        end_date: dt.date | str | Literal[":start_date:"]
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식 문자열을 전달한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)

        Returns
        -------
        str
            CSV 형식의 다차원 보고서 다운로드 결과
        """
        response = self.request_text(
            report_id = report_id,
            report_name = report_name,
            userid = userid,
            attributes = attributes,
            fields = fields,
            start_date = start_date,
            end_date = (start_date if end_date == ":start_date:" else end_date),
        )
        return self.parse(response, customer_id=self.customer_id)

    def build_request_data(self, **kwargs) -> str:
        return "{}"

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
            "reportName": report_name,
            "since": str(start_date),
            "until": str(end_date),
            "values": '{"type":"metric","fields":"'+','.join(fields)+'"}',
        }

    def build_request_headers(self, report_id: str, **kwargs: str) -> dict[str, str]:
        referer = f"{self.origin}/manage/ad-accounts/{self.account_no}/sa/reports/{report_id}"
        return dict(self.get_request_headers(), referer=referer)

    def set_request_headers(self, **kwargs: str):
        super().set_request_headers(contents="json", origin=self.origin, **kwargs)


class DailyReport(AdvancedReport):
    """네이버 검색광고 시스템에서 고정된 항목의 다차원 보고서를 다운로드하는 클래스.

    - **Menu**: 검색 광고 > 보고서 > 다차원 보고서 > 보고서 형식 저장
    - **API**: https://ads.naver.com/apis/sa/api/advanced-report/downloads
    - **Referer**: https://ads.naver.com/manage/ad-accounts/{account_no}/sa/reports

    다음의 보고서 항목 목록을 포함한다.
    - **구분**: 소재, 매체이름, PC/모바일 매체, 검색/콘텐츠 매체, 일별
    - **성과**: 노출수, 클릭수, 총비용, 총 전환수, 직접전환수, 총 전환매출액(원), 직접전환매출액(원), 평균노출순위

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    account_no: int | str
        검색광고 계정 번호
    customer_id: int | str
        검색광고 고객 ID
    """

    def extract(
            self,
            report_id: str,
            report_name: str,
            userid: str,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            **kwargs
        ) -> JsonObject | str:
        """다차원 보고서를 CSV 문자열 형식으로 다운로드한다.

        Parameters
        ----------
        report_id: str
            다차원 보고서 ID. 다차원 보고서 화면의 상세 URL 경로에 포함된 값이다.
        report_name: str
            보고서 이름
        userid: str
            네이버 아이디
        start_date: dt.date | str
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식 문자열을 전달한다.
        end_date: dt.date | str | Literal[":start_date:"]
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식 문자열을 전달한다.
                - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)

        Returns
        -------
        str
            CSV 형식의 다차원 보고서 다운로드 결과
        """
        return super().extract(report_id, report_name, userid, self.attributes, self.fields, start_date, end_date)

    @property
    def attributes(self) -> list[str]:
        """보고서 차원(소재, 매체이름, PC/모바일, 검색/콘텐츠, 일별) 목록을 반환한다."""
        # ["소재", "매체이름", "PC/모바일 매체", "검색/콘텐츠 매체", "일별"]
        return ["nccAdId", "mediaNm", "pcMblTp", "ntwkTp", "ymd"]

    @property
    def fields(self) -> list[str]:
        """보고서 측정값(노출수, 클릭수, 총비용, 전환 등) 목록을 반환한다."""
        # ["노출수", "클릭수", "총비용", "총 전환수", "직접전환수", "총 전환매출액(원)", "직접전환매출액(원)", ...]
        return ["impCnt", "clkCnt", "salesAmt", "ccnt", "drtCcnt", "convAmt", "drtConvAmt", "avgRnk", "pv", "stayTm"]
