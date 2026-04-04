from __future__ import annotations
from linkmerce.core.searchad.api import NaverSearchAdApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import JsonObject
    from requests import Response
    import datetime as dt


class _ReportsDownload(NaverSearchAdApi):
    """네이버 검색광고 API로 대용량 다운로드 보고서를 요청하는 공통 클래스.

    1. POST 요청으로 보고서를 생성한다.
    2. GET 요청으로 보고서가 "BUILT" 상태가 될 때까지 폴링한다.
    3. 다운로드 URL로 TSV 데이터를 가져온다.
    4. 처리 후 생성된 보고서를 삭제한다."""

    job_type: Literal["master-reports", "stat-reports"]
    report_type: str
    columns: list[str] = list()
    date_format: str | None = None
    MAX_LOOKBACK_DAYS: int | None = None

    @NaverSearchAdApi.with_session
    def extract(self, **kwargs) -> JsonObject | str:
        """마스터 보고서 또는 대용량 보고서에 대한 다운로드 워크플로우를 구현해야 한다."""
        raise NotImplementedError("The 'extract' method must be implemented.")

    def _extract_backend(self, report_type: str, **kwargs) -> str:
        """하나의 보고서 유형에 대해 보고서 생성부터 삭제까지의 워크플로우를 실행하는 공통 로직. TSV 데이터를 반환한다."""
        report_job = self.create_report(report_type, **kwargs)
        id_column = "reportJobId" if self.job_type == "stat-reports" else "id"
        try:
            download_url = self.get_report(report_job[id_column])
            return self.download_report(download_url)
        finally:
            self.delete_report(report_job[id_column])

    def create_report(self, report_type: str, date: dt.date | str | None = None) -> dict:
        """마스터 보고서와 대용량 보고서는 보고서 생성 방식이 다르다."""
        raise NotImplementedError("The 'create_report' method must be implemented.")

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


###################################################################
########################## Master Report ##########################
###################################################################

class _MasterReport(_ReportsDownload):
    """네이버 검색광고 API로 광고 정보 마스터 보고서를 다운로드하는 공통 클래스.

    캠페인, 광고그룹, 광고, 상품 등 각 유형(`report_type`)별 마스터 보고서를 TSV 형식으로 조회한다.
    - API 문서: https://naver.github.io/searchad-apidoc/#/tags/MasterReport"""

    job_type = "master-reports"

    report_type: Literal[
        "Campaign", "CampaignBudget", "BusinessChannel", "Adgroup", "AdgroupBudget",
        "Keyword", "Account", "Ad", "AdExtension", "Qi", "Label", "LabelRef", "Media",
        "Biz", "ShoppingProduct", "ContentsAd", "CatalogAd", "ProductGroup", "ProductGroupRel",
        "BrandAd", "BrandThumbnailAd", "BrandBannerAd", "Criterion"
    ]

    date_format = "%Y-%m-%dT%H:%M:%SZ"
    MAX_LOOKBACK_DAYS = 730

    @NaverSearchAdApi.with_session
    def extract(self, from_date: dt.date | str | None = None) -> JsonObject | str:
        """마스터 보고서를 생성하고 TSV 형식의 보고서를 다운로드 받는다. 다운로드 후 생성된 보고서를 삭제한다."""
        tsv_data = self._extract_backend(self.report_type, from_date=from_date)
        return self.parse(tsv_data)

    def create_report(self, report_type: str, from_date: dt.date | str | None = None) -> dict:
        """마스터 보고서 생성 요청을 보낸다."""
        data = {"item": report_type} | ({"fromTime": f"{from_date}T00:00:00Z"} if from_date else dict())
        return self.request(method="POST", uri=f"/{self.job_type}", json=data).json()


class Account(_MasterReport):
    """네이버 검색광고 계정 마스터 데이터를 다운로드하는 클래스."""
    report_type = "Account"
    columns = [
        "Customer ID", "LOGIN ID", "COMPANY NAME", "Link Status", "Owner Type", "regTm"
    ]


class Campaign(_MasterReport):
    """네이버 검색광고 캠페인 마스터 데이터를 다운로드하는 클래스."""
    report_type = "Campaign"
    columns = [
        "Customer ID", "Campaign ID", "Campaign Name", "Campaign Type",
        "Delivery Method", "Using Period", "Period Start Date", "Period End Date",
        "regTm", "delTm", "ON/OFF"
    ]


class CampaignBudget(_MasterReport):
    """네이버 검색광고 캠페인 예산 마스터 데이터를 다운로드하는 클래스."""
    report_type = "CampaignBudget"
    columns = [
        "Customer ID", "Campaign ID", "Using daily budget", "Daily Budget",
        "regTm", "delTm"
    ]


class BusinessChannel(_MasterReport):
    """네이버 검색광고 비즈니스채널 마스터 데이터를 다운로드하는 클래스."""
    report_type = "BusinessChannel"
    columns = [
        "Customer ID", "Name", "Business Channel ID", "Business Channel Type",
        "Channel Contents", "PC Inspect Status", "Mobile Inspect Status",
        "regTm", "delTm"
    ]


class Adgroup(_MasterReport):
    """네이버 검색광고 광고그룹 마스터 데이터를 다운로드하는 클래스."""
    report_type = "Adgroup"
    columns = [
        "Customer ID", "Ad Group ID", "Campaign ID", "Ad Group Name",
        "Ad Group Bid amount", "ON/OFF", "Using contents network bid",
        "Contents network bid", "PC network bidding weight",
        "Mobile network bidding weight", "Using KeywordPlus",
        "KeywordPlus bidding weight", "Business Channel ID(Mobile)",
        "Business Channel ID(PC)", "regTm", "delTm", "Content Type", "Ad group type"
    ]


class AdgroupBudget(_MasterReport):
    """네이버 검색광고 광고그룹 예산 마스터 데이터를 다운로드하는 클래스."""
    report_type = "AdgroupBudget"
    columns = [
        "Customer ID", "Ad Group ID", "Using Daily Budget", "Daily Budget",
        "regTm", "delTm"
    ]


class Keyword(_MasterReport):
    """네이버 검색광고 등록 키워드 마스터 데이터를 다운로드하는 클래스."""
    report_type = "Keyword"
    columns = [
        "Customer ID", "Ad Group ID", "Ad Keyword ID", "Ad Keyword",
        "Ad Keyword Bid Amount", "landing URL(PC)", "landing URL(Mobile)",
        "ON/OFF", "Ad Keyword Inspect Status", "Using Ad Group Bid Amount",
        "regTm", "delTm", "Ad Keyword type"
    ]


class Ad(_MasterReport):
    """네이버 검색광고 파워링크 단일형 소재 마스터 데이터를 다운로드하는 클래스."""
    report_type = "Ad"
    columns = [
        "Customer ID", "Ad Group ID", "Ad ID", "Ad Creative Inspect Status",
        "Subject", "Description", "Landing URL(PC)", "Landing URL(Mobile)",
        "ON/OFF", "regTm", "delTm"
    ]


class AdExtension(_MasterReport):
    """네이버 검색광고 확장소재 마스터 데이터를 다운로드하는 클래스."""
    report_type = "AdExtension"
    columns = [
        "Customer ID", "Ad Extension ID", "Type", "Owner ID",
        "Biz channel ID(PC)", "Biz channel ID(Mobile)",
        "Time Targeting(Monday)", "Time Targeting(Tuesday)",
        "Time Targeting(Wednesday)", "Time Targeting(Thursday)",
        "Time Targeting(Friday)", "Time Targeting(Saturday)",
        "Time Targeting(Sunday)", "ON/OFF", "Ad Extension Inspect Status",
        "regTm", "delTm"
    ]


class Qi(_MasterReport):
    """네이버 검색광고 품질지수 마스터 데이터를 다운로드하는 클래스."""
    report_type = "Qi"
    columns = [
        "Customer ID", "Ad Group ID", "Ad Keyword ID", "Ad Keyword", "Quality Index"
    ]


class Label(_MasterReport):
    """네이버 검색광고 즐겨찾기 마스터 데이터를 다운로드하는 클래스."""
    report_type = "Label"
    columns = ["Customer ID", "Label ID", "Label name", "regTm", "updateTm"]


class LabelRef(_MasterReport):
    """네이버 검색광고 즐겨찾기설정 마스터 데이터를 다운로드하는 클래스."""
    report_type = "LabelRef"
    columns = ["Customer ID", "Label ID", "Reference ID", "regTm", "updateTm"]


class Media(_MasterReport):
    """네이버 검색광고 광고매체 마스터 데이터를 다운로드하는 클래스."""
    report_type = "Media"
    columns = [
        "Type", "ID", "Media name", "URL", "NAVER Ad Networks", "Portal Site",
        "PC Media", "Mobile Media", "Search Ad Networks", "Contents Ad Networks",
        "Media Group ID", "Date of conclusion of a contract",
        "Date of revocation of a contract"
    ]


class Biz(_MasterReport):
    """네이버 검색광고 업종코드 마스터 데이터를 다운로드하는 클래스."""
    report_type = "Biz"
    columns = ["ID", "BusinessName", "SuperBusinessId", "Level"]


class ShoppingProduct(_MasterReport):
    """네이버 검색광고 쇼핑검색 쇼핑몰상품형 상품 마스터 데이터를 다운로드하는 클래스."""
    report_type = "ShoppingProduct"
    columns = [
        "Customer ID", "Ad Group ID", "Ad ID", "Ad Creative Inspect Status",
        "ON/OFF", "Ad Product Name", "Ad Image URL", "Bid",
        "Using Ad Group Bid Amount", "Ad Link Status", "regTm", "delTm",
        "Product ID", "Product ID Of Mall", "Product Name", "Product Image URL",
        "PC Landing URL", "Mobile Landing URL",
        "Price(PC)", "Price(Mobile)", "Delivery Fee",
        "NAVER Shopping Category Name 1", "NAVER Shopping Category Name 2",
        "NAVER Shopping Category Name 3", "NAVER Shopping Category Name 4",
        "NAVER Shopping Category ID 1", "NAVER Shopping Category ID 2",
        "NAVER Shopping Category ID 3", "NAVER Shopping Category ID 4",
        "Category Name of Mall"
    ]


class ContentsAd(_MasterReport):
    """네이버 검색광고 파워컨텐츠 소재 마스터 데이터를 다운로드하는 클래스."""
    report_type = "ContentsAd"
    columns = [
        "Customer ID", "Ad Group ID", "Ad ID", "Ad Creative Inspect Status",
        "Subject", "Description", "Landing URL(PC)", "Landing URL(Mobile)",
        "Image URL", "Company Name", "Contents Issue Date", "Release Date",
        "ON/OFF", "regTm", "delTm"
    ]


class CatalogAd(_MasterReport):
    """네이버 검색광고 쇼핑검색 제품카탈로그형 상품 마스터 데이터를 다운로드하는 클래스."""
    report_type = "CatalogAd"
    columns = [
        "Customer ID", "Ad Group ID", "Ad ID", "Ad Creative Inspect Status",
        "ON/OFF", "Bid", "Using Ad Group Bid Amount", "Ad Link Status",
        "regTm", "delTm", "Product ID", "Product Name", "Product Title",
        "Brand", "Maker", "openDate", "Product Image URL", "PC Landing URL",
        "Lowest Price(PC)", "Lowest Price(Mobile)", "mallCount", "reviewCount",
        "scoreInfo",
        "NAVER Shopping Category Name 1", "NAVER Shopping Category Name 2",
        "NAVER Shopping Category Name 3", "NAVER Shopping Category Name 4",
        "NAVER Shopping Category ID 1", "NAVER Shopping Category ID 2",
        "NAVER Shopping Category ID 3", "NAVER Shopping Category ID 4"
    ]


class ProductGroup(_MasterReport):
    """네이버 검색광고 쇼핑검색 쇼핑브랜드형 상품그룹 마스터 데이터를 다운로드하는 클래스."""
    report_type = "ProductGroup"
    columns = [
        "Customer ID", "Product group ID", "Business channel ID", "Name",
        "Registration method", "Registered product type", "Attribute json1",
        "regTm", "delTm"
    ]


class ProductGroupRel(_MasterReport):
    """네이버 검색광고 쇼핑검색 쇼핑브랜드형 상품그룹관계 마스터 데이터를 다운로드하는 클래스."""
    report_type = "ProductGroupRel"
    columns = [
        "Customer ID", "Product Group Relation ID", "Product Group ID",
        "AD group ID", "regTm", "delTm"
    ]


class BrandAd(_MasterReport):
    """네이버 검색광고 쇼핑검색 쇼핑브랜드형 소재 마스터 데이터를 다운로드하는 클래스."""
    report_type = "BrandAd"
    columns = [
        "Customer ID", "Ad Group ID", "Ad ID", "Ad Creative Inspect Status",
        "ON/OFF", "Headline", "description", "Logo image path", "Link URL",
        "Image path", "regTm", "delTm"
    ]


class BrandThumbnailAd(_MasterReport):
    """네이버 검색광고 쇼핑검색 쇼핑브랜드형 썸네일소재 마스터 데이터를 다운로드하는 클래스."""
    report_type = "BrandThumbnailAd"
    columns = [
        "Customer ID", "Ad Group ID", "Ad ID", "Ad Creative Inspect Status",
        "ON/OFF", "Headline", "description", "extra Description",
        "Logo image path", "Link URL", "Thumbnail Image path",
        "regTm", "delTm"
    ]


class BrandBannerAd(_MasterReport):
    """네이버 검색광고 쇼핑검색 쇼핑브랜드형 배너소재 마스터 데이터를 다운로드하는 클래스."""
    report_type = "BrandBannerAd"
    columns = [
        "Customer ID", "Ad Group ID", "Ad ID", "Ad Creative Inspect Status",
        "ON/OFF", "Headline", "description", "Logo image path", "Link URL",
        "Thumbnail Image path", "regTm", "delTm"
    ]


class MasterCriterion(_MasterReport):
    """네이버 검색광고 타겟팅 대상 마스터 데이터를 다운로드하는 클래스."""
    report_type = "Criterion"
    columns = [
        "Customer ID", "Dictionary code", "Type", "Owner ID", "Bid_weight",
        "negative", "On/OFF", "Content_Value", "regTm", "delTm"
    ]


class MasterAd(_MasterReport):
    """모든 소재 유형의 네이버 검색광고 마스터 데이터를 일괄 다운로드하는 클래스."""
    report_type = None

    @NaverSearchAdApi.with_session
    def extract(self, from_date: dt.date | str | None = None) -> JsonObject | dict[str, str]:
        """모든 소재 유형의 마스터 데이터를 일괄 다운로드하여 `{보고서_유형: 엑셀_바이너리}` 형식으로 반환한다.."""
        tsv_data = dict()
        for report_type in [
                "Ad", "ContentsAd", "ShoppingProduct", "ProductGroup", "ProductGroupRel",
                "BrandThumbnailAd", "BrandBannerAd", "BrandAd"
            ]:
            tsv_data[report_type] = self._extract_backend(report_type, from_date=from_date)
        return self.parse(tsv_data, customer_id=self.customer_id)


###################################################################
########################### Stat Report ###########################
###################################################################

class _StatReport(_ReportsDownload):
    """네이버 검색광고 API로 대용량 보고서를 다운로드하는 공통 클래스.

    광고성과, 전환 등 각 유형(`report_type`)별 대용량 보고서를 TSV 형식으로 조회한다.
    - API 문서: https://naver.github.io/searchad-apidoc/#/tags/StatReport"""

    job_type = "stat-reports"

    report_type: Literal[
        "AD", "AD_DETAIL", "AD_CONVERSION", "AD_CONVERSION_DETAIL",
        "ADEXTENSION", "ADEXTENSION_CONVERSION", "EXPKEYWORD",
        "SHOPPINGKEYWORD_DETAIL", "SHOPPINGKEYWORD_CONVERSION_DETAIL",
        "SHOPPINGBRANDPRODUCT", "SHOPPINGBRANDPRODUCT_CONVERSION",
        "CRITERION", "CRITERION_CONVERSION"
    ]

    date_format = "%Y%m%d"

    @NaverSearchAdApi.with_session
    def extract(self, date: dt.date | str) -> JsonObject | str:
        """마스터 보고서를 생성하고 TSV 형식의 보고서를 다운로드 받는다. 다운로드 후 생성된 보고서를 삭제한다."""
        tsv_data = self._extract_backend(self.report_type, date=date)
        return self.parse(tsv_data)

    def create_report(self, report_type: str, date: dt.date | str) -> dict:
        """대용량 보고서 생성 요청을 보낸다."""
        data = {"reportTp": report_type, "statDt": str(date).replace('-', '')}
        return self.request(method="POST", uri=f"/{self.job_type}", json=data).json()


class AdStat(_StatReport):
    """네이버 검색광고 광고성과 보고서를 다운로드하는 클래스."""
    report_type = "AD"
    columns = [
        "Date", "CUSTOMER ID", "Campaign ID", "AD Group ID",
        "AD Keyword ID", "AD ID", "Business Channel ID",
        "Media Code", "PC Mobile Type",
        "Impression", "Click", "Cost", "Sum of AD rank", "View count"
    ]
    MAX_LOOKBACK_DAYS = 365


class AdStatDetail(_StatReport):
    """네이버 검색광고 광고성과 상세 보고서를 다운로드하는 클래스."""
    report_type = "AD_DETAIL"
    columns = [
        "Date", "CUSTOMER ID", "Campaign ID", "AD Group ID",
        "AD Keyword ID", "AD ID", "Business Channel ID",
        "Hours", "Region Code", "Media Code", "PC Mobile Type",
        "Impression", "Click", "Cost", "Sum of AD rank", "View count"
    ]
    MAX_LOOKBACK_DAYS = 180


class AdConversion(_StatReport):
    """네이버 검색광고 전환 보고서를 다운로드하는 클래스."""
    report_type = "AD_CONVERSION"
    columns = [
        "Date", "CUSTOMER ID", "Campaign ID", "AD Group ID",
        "AD Keyword ID", "AD ID", "Business Channel ID", "Media Code", "PC Mobile Type",
        "Conversion Method", "Conversion Type", "Conversion count", "Sales by conversion"
    ]
    MAX_LOOKBACK_DAYS = 365


class AdConversionDetail(_StatReport):
    """네이버 검색광고 전환 상세 보고서를 다운로드하는 클래스."""
    report_type = "AD_CONVERSION_DETAIL"
    columns = [
        "Date", "CUSTOMER ID", "Campaign ID", "AD Group ID",
        "AD Keyword ID", "AD ID", "Business Channel ID",
        "Hours", "Region Code", "Media Code", "PC Mobile Type",
        "Conversion Method", "Conversion Type", "Conversion count", "Sales by conversion"
    ]
    MAX_LOOKBACK_DAYS = 45


class AdExtension(_StatReport):
    """네이버 검색광고 확장소재광고 성과 보고서를 다운로드하는 클래스."""
    report_type = "ADEXTENSION"
    columns = [
        "Date", "CUSTOMER ID", "Campaign ID", "AD Group ID", "AD Keyword ID",
        "AD ID", "AD extension ID", "AD extension Business Channel ID",
        "Media Code", "PC Mobile Type",
        "Impression", "Click", "Cost", "Sum of AD rank", "View count"
    ]
    MAX_LOOKBACK_DAYS = 365


class AdExtensionConversion(_StatReport):
    """네이버 검색광고 확장소재 전환 보고서를 다운로드하는 클래스."""
    report_type = "ADEXTENSION_CONVERSION"
    columns = [
        "Date", "CUSTOMER ID", "Campaign ID", "AD Group ID", "AD Keyword ID",
        "AD ID", "AD extension ID", "AD extension Business Channel ID",
        "Media Code", "PC Mobile Type",
        "Conversion Method", "Conversion Type", "Conversion count", "Sales by conversion"
    ]
    MAX_LOOKBACK_DAYS = 365


class ExpKeyword(_StatReport):
    """네이버 검색광고 파워링크 검색어 보고서를 다운로드하는 클래스."""
    report_type = "EXPKEYWORD"
    columns = [
        "Date", "CUSTOMER ID", "Campaign ID", "AD Group ID",
        "Search Keyword", "Media Code", "PC Mobile Type",
        "Impression", "Click", "Cost", "Sum of AD rank", "View count"
    ]
    MAX_LOOKBACK_DAYS = 365


class ShoppingKeywordDetail(_StatReport):
    """네이버 검색광고 쇼핑검색 검색어 상세 보고서를 다운로드하는 클래스."""
    report_type = "SHOPPINGKEYWORD_DETAIL"
    columns = [
        "Date", "CUSTOMER ID", "Campaign ID", "AD Group ID",
        "Search Keyword", "AD ID", "Business Channel ID",
        "Hours", "Region Code", "Media Code", "PC Mobile Type",
        "Impression", "Click", "Cost", "Sum of AD rank", "View count"
    ]
    MAX_LOOKBACK_DAYS = 180


class ShoppingKeywordConversionDetail(_StatReport):
    """네이버 검색광고 쇼핑검색 검색어 전환 상세 보고서를 다운로드하는 클래스."""
    report_type = "SHOPPINGKEYWORD_CONVERSION_DETAIL"
    columns = [
        "Date", "CUSTOMER ID", "Campaign ID", "AD Group ID",
        "keyword", "AD ID", "Business Channel ID",
        "Hours", "Region Code", "Media Code", "PC Mobile Type",
        "Conversion Method", "Conversion Type", "Conversion count", "Sales by conversion"
    ]
    MAX_LOOKBACK_DAYS = 45


class ShoppingBrandProduct(_StatReport):
    """네이버 검색광고 쇼핑검색 쇼핑브랜드형 상품별 성과 보고서를 다운로드하는 클래스."""
    report_type = "SHOPPINGBRANDPRODUCT"
    columns = [
        "Date", "CUSTOMER ID", "Campaign ID", "AD Group ID", "NV_MID",
        "Business Channel ID", "Media Code", "PC Mobile Type",
        "Impression", "Click", "Cost", "Sum of AD rank", "View count"
    ]
    MAX_LOOKBACK_DAYS = 180


class ShoppingBrandProductConversion(_StatReport):
    """네이버 검색광고 쇼핑검색 쇼핑브랜드형 상품별 전환 보고서를 다운로드하는 클래스."""
    report_type = "SHOPPINGBRANDPRODUCT_CONVERSION"
    columns = [
        "Date", "CUSTOMER ID", "Campaign ID", "AD Group ID", "NV_MID",
        "Business Channel ID", "Media Code", "PC Mobile Type",
        "Conversion Method", "Conversion Type", "Conversion count", "Sales by conversion"
    ]
    MAX_LOOKBACK_DAYS = 180


class Criterion(_StatReport):
    """네이버 검색광고 타게팅 성과 보고서를 다운로드하는 클래스."""
    report_type = "CRITERION"
    columns = [
        "Date", "CUSTOMER ID", "Criterion ID", "PC Mobile Type",
        "Impression", "Click", "Cost"
    ]
    MAX_LOOKBACK_DAYS = 365


class CriterionConversion(_StatReport):
    """네이버 검색광고 타게팅 전환 보고서를 다운로드하는 클래스."""
    report_type = "CRITERION_CONVERSION"
    columns = [
        "Date", "CUSTOMER ID", "Criterion ID", "PC Mobile Type",
        "Conversion Method", "Conversion Type", "Conversion count", "Sales by conversion"
    ]
    MAX_LOOKBACK_DAYS = 365


class AdvancedReport(_StatReport):
    """다차원 보고서의 바탕이 되는 광고성과 및 전환 보고서를 다운로드하는 클래스."""
    report_type = None
    MAX_LOOKBACK_DAYS = 365

    @property
    def default_options(self) -> dict:
        return {"RequestEach": {"request_delay": 1}}

    @NaverSearchAdApi.with_session
    def extract(
            self,
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        ) -> JsonObject | list[dict[str, str]]:
        """광고성과 및 전환 보고서를 다운로드하여 `{보고서_유형: 엑셀_바이너리}` 형식으로 반환한다."""
        return (self.request_each(self.request_daily_report)
                .partial(customer_id=self.customer_id)
                .expand(date=self.generate_date_range(start_date, end_date, freq='D'))
                .run())

    def request_daily_report(self, date: dt.date, **kwargs) -> dict[str, str]:
        """일별 보고서를 다운로드한다. """
        tsv_data = dict()
        for report_type in ["AD", "AD_CONVERSION"]:
            tsv_data[report_type] = self._extract_backend(report_type, date=date)
        return tsv_data
