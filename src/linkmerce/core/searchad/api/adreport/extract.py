from __future__ import annotations
from linkmerce.core.searchad.api import NaverSearchAdAPI

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import JsonObject
    from requests import Response
    import datetime as dt


class _ReportsDownload(NaverSearchAdAPI):
    """
    The Extractor workflow operates as follows:
    1. Sends a POST request to the Naver Search Ad API to create a report.
    2. Repeatedly send GET requests to check status of the Report Job ID until it becomes "BUILT".
    3. Once the report is confirmed as built, retrieves the download URL and fetches the data via a GET request.
    4. The output data is returned in TSV text format without headers.
    5. After processing, the created report is deleted regardless of success or failure.
    """

    job_type: Literal["master-reports", "stat-reports"]
    report_type: str

    @NaverSearchAdAPI.with_session
    def extract(self, from_date: dt.date | str | None = None) -> JsonObject:
        tsv_data = self._extract_backend(self.report_type, from_date)
        return self.parse(tsv_data)

    def _extract_backend(self, report_type: str, from_date: dt.date | str | None = None) -> str:
        report_job = self.create_report(report_type, from_date)
        id_column = "reportJobId" if self.job_type == "stat-reports" else "id"
        try:
            download_url = self.get_report(report_job[id_column])
            return self.download_report(download_url)
        finally:
            self.delete_report(report_job[id_column])

    def create_report(self, report_type: str, from_date: dt.date | str | None = None) -> dict:
        data = dict(item=report_type, **(dict(fromTime=f"{from_date}T00:00:00Z") if from_date else dict()))
        return self.request(method="POST", uri=f"/{self.job_type}", json=data).json()

    def get_report(self, report_job_id: str) -> str:
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
        if download_url:
            return self.request(method="GET", uri="/report-download", url=download_url).text

    def delete_report(self, report_job_id: str) -> int:
        uri = f"/{self.job_type}/{report_job_id}"
        return self.request(method="DELETE", uri=uri).status_code

    def request(self, method: str, uri: str, params = None, data = None, json = None, **kwargs) -> Response:
        if "url" not in kwargs:
            kwargs["url"] = self.origin + uri
        if "headers" not in kwargs:
            kwargs["headers"] = self.build_request_headers(method=method, uri=uri)
        return super().request(method, params=params, data=data, json=json, **kwargs)


class _MasterReport(_ReportsDownload):
    """
    Download various types of Master Report using the Naver Search Ad API.
    The Master Report provides an informational list of content elements
    (campaigns, ad groups, ads, etc.) that exist from the input time.

    For detailed API documentation, see:
    - create: https://naver.github.io/searchad-apidoc/#/operations/POST/~2Fmaster-reports
    - get (by id): https://naver.github.io/searchad-apidoc/#/operations/GET/~2Fmaster-reports~2F%7Bid%7D
    - delete (by id): https://naver.github.io/searchad-apidoc/#/operations/DELETE/~2Fmaster-reports~2F%7Bid%7D

    Additionally, a PDF document is available that describes the column definitions of the Master Report:
    - https://searchad.naver.com/File/downloadfilen/?type=10&filename=Report_MasterSpecification_02.pdf.pdf
    """

    job_type = "master-reports"

    report_type: Literal[
        "Campaign", "Adgroup", "Ad", "ContentsAd", "ShoppingProduct", "ProductGroup", "ProductGroupRel",
        "BrandThumbnailAd", "BrandBannerAd", "BrandAd"
    ]


class Campaign(_MasterReport):
    report_type = "Campaign"


class Adgroup(_MasterReport):
    report_type = "Adgroup"


class PowerLinkAd(_MasterReport):
    report_type = "Ad"


class PowerContentsAd(_MasterReport):
    report_type = "ContentsAd"


class ShoppingProductAd(_MasterReport):
    report_type = "ShoppingProduct"


class ProductGroup(_MasterReport):
    report_type = "ProductGroup"


class ProductGroupRel(_MasterReport):
    report_type = "ProductGroupRel"


class BrandThumbnailAd(_MasterReport):
    report_type = "BrandThumbnailAd"


class BrandBannerAd(_MasterReport):
    report_type = "BrandBannerAd"


class BrandAd(_MasterReport):
    report_type = "BrandAd"


class Ad(_MasterReport):
    report_type = None

    @NaverSearchAdAPI.with_session
    def extract(self, from_date: dt.date | str | None = None) -> JsonObject:
        report_types = ["Ad", "ContentsAd", "ShoppingProduct", "ProductGroup", "ProductGroupRel", "BrandThumbnailAd", "BrandBannerAd", "BrandAd"]
        tsv_data = {report_type: self._extract_backend(report_type, from_date) for report_type in report_types}
        return self.parse(tsv_data)
