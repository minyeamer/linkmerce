from __future__ import annotations
from linkmerce.core.searchad.api import NaverSearchAdApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import JsonObject
    from requests import Response
    import datetime as dt


class _ReportsDownload(NaverSearchAdApi):
    """네이버 검색광고 API로 보고서를 요청하는 공통 클래스.

    1. POST 요청으로 보고서를 생성한다.
    2. GET 요청으로 보고서가 "BUILT" 상태가 될 때까지 폴링한다.
    3. 다운로드 URL로 TSV 데이터를 가져온다.
    4. 처리 후 생성된 보고서를 삭제한다."""

    job_type: Literal["master-reports", "stat-reports"]
    report_type: str

    @NaverSearchAdApi.with_session
    def extract(self, from_date: dt.date | str | None = None) -> JsonObject:
        """보고서를 생성하고 TSV 데이터를 다운로드한다."""
        tsv_data = self._extract_backend(self.report_type, from_date)
        return self.parse(tsv_data)

    def _extract_backend(self, report_type: str, from_date: dt.date | str | None = None) -> str:
        """보고서 생성부터 삭제까지의 워크플로우를 실행하는 공통 로직."""
        report_job = self.create_report(report_type, from_date)
        id_column = "reportJobId" if self.job_type == "stat-reports" else "id"
        try:
            download_url = self.get_report(report_job[id_column])
            return self.download_report(download_url)
        finally:
            self.delete_report(report_job[id_column])

    def create_report(self, report_type: str, from_date: dt.date | str | None = None) -> dict:
        """보고서 생성 요청을 보낸다."""
        data = dict(item=report_type, **({"fromTime": f"{from_date}T00:00:00Z"} if from_date else dict()))
        return self.request(method="POST", uri=f"/{self.job_type}", json=data).json()

    def get_report(self, report_job_id: str) -> str:
        """보고서 상태를 폴링하고 다운로드 URL을 반환한다."""
        import time
        uri = f"/{self.job_type}/{report_job_id}"
        while True:
            try:
                report = self.request(method="GET", uri=uri).json()
                if report["status"] == "NONE":
                    return None
                elif report["status"] == "BUILT":
                    return report["downloadUrl"]
                else:
                    time.sleep(.5)
            except:
                raise ValueError("The master report is invalid.")

    def download_report(self, download_url: str | None = None) -> str:
        """보고서 데이터를 다운로드한다."""
        if download_url:
            return self.request(method="GET", uri="/report-download", url=download_url).text

    def delete_report(self, report_job_id: str) -> int:
        """생성된 보고서를 삭제한다."""
        uri = f"/{self.job_type}/{report_job_id}"
        return self.request(method="DELETE", uri=uri).status_code

    def request(self, method: str, uri: str, params = None, data = None, json = None, **kwargs) -> Response:
        """검색광고 API에 인증 헤더를 포함한 HTTP 요청을 보낸다."""
        if "url" not in kwargs:
            kwargs["url"] = self.origin + uri
        if "headers" not in kwargs:
            kwargs["headers"] = self.build_request_headers(method=method, uri=uri)
        return super().request(method, params=params, data=data, json=json, **kwargs)


class _MasterReport(_ReportsDownload):
    """네이버 검색광고 API로 대용량 다운로드 보고서를 다운로드하는 공통 클래스.

    캔페인, 광고그룹, 광고, 상품 등 각 유형(`report_type`)별 대용량 다운로드 보고서를 TSV 형식으로 조회한다.
    - API 문서: https://naver.github.io/searchad-apidoc/"""

    job_type = "master-reports"

    report_type: Literal[
        "Campaign", "Adgroup", "Ad", "ContentsAd", "ShoppingProduct", "ProductGroup", "ProductGroupRel",
        "BrandThumbnailAd", "BrandBannerAd", "BrandAd"
    ]


class Campaign(_MasterReport):
    """네이버 검색광고 캔페인 마스터 데이터를 다운로드하는 클래스."""

    report_type = "Campaign"


class Adgroup(_MasterReport):
    """네이버 검색광고 광고그룹 마스터 데이터를 다운로드하는 클래스."""

    report_type = "Adgroup"


class PowerLinkAd(_MasterReport):
    """네이버 검색광고 소재 마스터 데이터를 다운로드하는 클래스."""

    report_type = "Ad"


class PowerContentsAd(_MasterReport):
    """네이버 검색광고 콘텐츠소재 마스터 데이터를 다운로드하는 클래스."""

    report_type = "ContentsAd"


class ShoppingProductAd(_MasterReport):
    """네이버 검색광고 쇼핑상품 마스터 데이터를 다운로드하는 클래스."""

    report_type = "ShoppingProduct"


class ProductGroup(_MasterReport):
    """네이버 검색광고 상품그룹 마스터 데이터를 다운로드하는 클래스."""

    report_type = "ProductGroup"


class ProductGroupRel(_MasterReport):
    """네이버 검색광고 상품그룹관계 마스터 데이터를 다운로드하는 클래스."""

    report_type = "ProductGroupRel"


class BrandThumbnailAd(_MasterReport):
    """네이버 검색광고 브랜드썸네일소재 마스터 데이터를 다운로드하는 클래스."""

    report_type = "BrandThumbnailAd"


class BrandBannerAd(_MasterReport):
    """네이버 검색광고 브랜드배너소재 마스터 데이터를 다운로드하는 클래스."""

    report_type = "BrandBannerAd"


class BrandAd(_MasterReport):
    """네이버 검색광고 BrandAd 마스터 데이터를 다운로드하는 클래스."""

    report_type = "BrandAd"


class Ad(_MasterReport):
    """모든 소재 유형의 네이버 검색광고 마스터 데이터를 일괄 다운로드하는 클래스."""

    report_type = None

    @NaverSearchAdApi.with_session
    def extract(self, from_date: dt.date | str | None = None) -> JsonObject:
        """모든 소재 유형의 마스터 데이터를 일괄 다운로드하여 `{보고서_유형: 엑셀_바이너리}` 형식으로 반환한다.."""
        report_types = ["Ad", "ContentsAd", "ShoppingProduct", "ProductGroup", "ProductGroupRel", "BrandThumbnailAd", "BrandBannerAd", "BrandAd"]
        tsv_data = {report_type: self._extract_backend(report_type, from_date) for report_type in report_types}
        return self.parse(tsv_data)
