from __future__ import annotations
from linkmerce.core.google.api import GoogleApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class GoogleAds(GoogleApi):
    """구글 광고 API 요청을 처리하는 공통 클래스.

    GAQL(Google Ads Query Language)로 searchStream 요청을 보내 데이터를 조회한다.
    - **API**: https://googleads.googleapis.com/v23/customers/{customer_id}/googleAds:searchStream
    - **Docs**: https://developers.google.com/google-ads/api/fields/v23/overview
    - **Referer**: https://ads.google.com/aw/overview

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    customer_id: int | str
        구글 광고 고객 ID
    manager_id: int | str
        구글 광고 관리자 계정의 고객 ID
    developer_token: str
        구글 광고 API 개발자 토큰
    service_account: str | Path | dict[str, str]
        구글 서비스 계정 키 파일 경로 또는 파일 내용을 파싱한 딕셔너리
    """

    service: str = "googleads"
    method: str = "POST"
    version: str = "v23"
    table: str
    config_fields = [{
        "service_account": ["client_email", "private_key"]},
        {"scope": "https://www.googleapis.com/auth/adwords"},
        "customer_id", "manager_id", "developer_token",
    ]

    @property
    def customer_id(self) -> int | str:
        return self.get_config("customer_id")

    @property
    def fields(self) -> list[str]:
        """조회할 GAQL 필드 목록을 정의한다."""
        return list()

    @property
    def url(self) -> str:
        """구글 광고 searchStream API URL을 조합해 반환한다."""
        return self.concat_path(self.origin, self.version, "/customers/", self.customer_id, "/googleAds:searchStream")

    @GoogleApi.with_session
    @GoogleApi.with_token
    def extract(
            self,
            start_date: dt.date | str | None = None,
            end_date: dt.date | str | None = None,
            date_range: Literal[
                "TODAY", "YESTERDAY", "LAST_7_DAYS", "LAST_14_DAYS", "LAST_30_DAYS", "LAST_BUSINESS_WEEK",
                "THIS_MONTH", "LAST_MONTH", "THIS_WEEK_SUN_TODAY", "THIS_WEEK_MON_TODAY",
                "LAST_WEEK_SUN_SAT", "LAST_WEEK_MON_SUN"] | None = "LAST_30_DAYS",
            fields: Sequence[str] = list(),
            **kwargs
        ) -> JsonObject:
        """GAQL 쿼리로 구글 광고 데이터를 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        start_date: dt.date | str | None
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 전달한다.   
            시작일을 지정하면 `date_range`는 무시된다. 기본값은 `None`
        end_date: dt.date | str | None
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 전달한다.   
            종료일을 지정하면 `date_range`는 무시된다. 기본값은 `None`
        date_range: str | None
            GAQL 사전 정의 조회 기간. `start_date` 또는 `end_date`가 전달되면 무시된다.   
            기본값은 `"LAST_30_DAYS"`
        fields: Sequence[str]
            조회할 GAQL 필드 목록. 생략 시 클래스에 정의된 `fields` 속성을 사용한다.

        Returns
        -------
        list[dict]
            구글 광고 데이터 조회 결과
        """
        response = self.request_json(
            start_date = start_date,
            end_date = end_date,
            date_range = (None if (start_date is not None) or (end_date is not None) else date_range),
            fields = fields,
        )
        return self.parse(response, customer_id=str(self.customer_id))

    def build_request_json(self, fields: Sequence[str] = list(), **kwargs) -> dict[str, str]:
        """GAQL 쿼리를 구성한다."""
        fields = ', '.join(fields if fields else self.fields)
        where = f" WHERE {cond}" if (cond := self.where(**kwargs)) else str()
        return {"query": f"SELECT {fields} FROM {self.table}{where}"}

    def build_request_headers(self, **kwargs) -> dict[str, str]:
        """구글 광고 API 요청 헤더를 구성한다."""
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
            "developer-token": self.get_config("developer_token"),
            "login-customer-id": str(self.get_config("manager_id")),
        }

    def where(
            self, 
            start_date: dt.date | str | None = None,
            end_date: dt.date | str | None = None,
            date_range: str | None = None,
            **kwargs
        ) -> str:
        """GAQL WHERE 절을 구성한다."""
        if date_range is not None:
            return f"segments.date DURING {date_range}"

        has_start, has_end = (start_date is not None), (end_date is not None)
        if has_start and has_end:
            return f"segments.date BETWEEN '{start_date}' AND '{end_date}'"
        elif has_start and (not has_end):
            return f"segments.date >= '{start_date}'"
        elif (not has_start) and has_end:
            return f"segments.date <= '{end_date}'"
        else:
            return str()

    def fetch_date_range(
            self, 
            start_date: dt.date | str | None = None,
            end_date: dt.date | str | None = None,
            date_range: str | None = None,
        ) -> tuple[str, str]:
        """조회 기간의 시작일과 종료일을 반환한다."""
        response = self.request_json(
            start_date = start_date,
            end_date = end_date,
            date_range = date_range,
            fields = ["segments.date"],
        )
        dates = [row["segments"]["date"] for row in response[0]["results"]]
        return min(dates), max(dates)


class Campaign(GoogleAds):
    """구글 광고 캠페인 보고서를 조회하는 클래스.

    GAQL(Google Ads Query Language)로 searchStream 요청을 보내 데이터를 조회한다.
    - **API**: https://googleads.googleapis.com/v23/customers/{customer_id}/googleAds:searchStream
    - **Docs**: https://developers.google.com/google-ads/api/fields/v23/campaign
    - **Referer**: https://ads.google.com/aw/campaigns

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    customer_id: int | str
        구글 광고 고객 ID
    manager_id: int | str
        구글 광고 관리자 계정의 고객 ID
    developer_token: str
        구글 광고 API 개발자 토큰
    service_account: str | Path | dict[str, str]
        구글 서비스 계정 키 파일 경로 또는 파일 내용을 파싱한 딕셔너리
    """

    table = "campaign"

    @property
    def fields(self) -> list[str]:
        return [
            "campaign.id",
            "campaign.name",
            "campaign_budget.amount_micros",
            "campaign.status",
            "campaign.advertising_channel_type",
            "metrics.impressions",
            "metrics.cost_micros",
            "campaign.bidding_strategy_type",
            "metrics.clicks",
            # "metrics.conversions",
            # "metrics.conversions_value",
            "campaign.start_date_time",
        ]


class AdGroup(GoogleAds):
    """구글 광고그룹 보고서를 조회하는 클래스.

    GAQL(Google Ads Query Language)로 searchStream 요청을 보내 데이터를 조회한다.
    - **API**: https://googleads.googleapis.com/v23/customers/{customer_id}/googleAds:searchStream
    - **Docs**: https://developers.google.com/google-ads/api/fields/v23/ad_group
    - **Referer**: https://ads.google.com/aw/adgroups

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    customer_id: int | str
        구글 광고 고객 ID
    manager_id: int | str
        구글 광고 관리자 계정의 고객 ID
    developer_token: str
        구글 광고 API 개발자 토큰
    service_account: str | Path | dict[str, str]
        구글 서비스 계정 키 파일 경로 또는 파일 내용을 파싱한 딕셔너리
    """

    table = "ad_group"

    @property
    def fields(self) -> list[str]:
        return [
            "ad_group.id",
            "ad_group.name",
            "campaign.id",
            # "campaign.name",
            "ad_group.status",
            "ad_group.target_cpa_micros",
            "ad_group.type",
            "metrics.impressions",
            "metrics.cost_micros",
            "metrics.clicks",
            # "metrics.conversions",
            # "metrics.conversions_value",
        ]


class Ad(GoogleAds):
    """구글 광고 소재 보고서를 조회하는 클래스.

    GAQL(Google Ads Query Language)로 searchStream 요청을 보내 데이터를 조회한다.
    - **API**: https://googleads.googleapis.com/v23/customers/{customer_id}/googleAds:searchStream
    - **Docs**: https://developers.google.com/google-ads/api/fields/v23/ad_group_ad
    - **Referer**: https://ads.google.com/aw/ads

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    customer_id: int | str
        구글 광고 고객 ID
    manager_id: int | str
        구글 광고 관리자 계정의 고객 ID
    developer_token: str
        구글 광고 API 개발자 토큰
    service_account: str | Path | dict[str, str]
        구글 서비스 계정 키 파일 경로 또는 파일 내용을 파싱한 딕셔너리
    """

    table = "ad_group_ad"

    @property
    def fields(self) -> list[str]:
        return [
            "ad_group_ad.ad.id",
            "campaign.id",
            # "campaign.name",
            "ad_group.id",
            # "ad_group.name",
            "ad_group_ad.status",
            "ad_group_ad.ad.type",
            *[f"ad_group_ad.ad.{ad_type}.headline" for ad_type in self.single_types],
            *[f"ad_group_ad.ad.{ad_type}.headlines" for ad_type in self.list_types],
            "ad_group_ad.ad.expanded_text_ad.headline_part1",
            "metrics.impressions",
            "metrics.clicks",
            "metrics.cost_micros",
            # "metrics.conversions",
            # "metrics.conversions_value",
        ]

    @property
    def single_types(self) -> list[str]:
        return [
            "demand_gen_carousel_ad", "demand_gen_product_ad", "shopping_comparison_listing_ad", "text_ad"
        ]

    @property
    def list_types(self) -> list[str]:
        return [
            "app_ad", "app_engagement_ad", "app_pre_registration_ad",
            "demand_gen_multi_asset_ad", "demand_gen_video_responsive_ad",
            "local_ad", "responsive_display_ad", "responsive_search_ad",
            "smart_campaign_ad", "video_responsive_ad"
        ]


class Insight(GoogleAds):
    """구글 광고 소재 보고서를 날짜/기기별로 구분해 조회하는 클래스.

    GAQL(Google Ads Query Language)로 searchStream 요청을 보내 데이터를 조회한다.
    - **API**: https://googleads.googleapis.com/v23/customers/{customer_id}/googleAds:searchStream
    - **Docs**: https://developers.google.com/google-ads/api/fields/v23/ad_group_ad
    - **Referer**: https://ads.google.com/aw/ads

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    customer_id: int | str
        구글 광고 고객 ID
    manager_id: int | str
        구글 광고 관리자 계정의 고객 ID
    developer_token: str
        구글 광고 API 개발자 토큰
    service_account: str | Path | dict[str, str]
        구글 서비스 계정 키 파일 경로 또는 파일 내용을 파싱한 딕셔너리

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    table = "ad_group_ad"
    default_options = {"RequestEach": {"request_delay": 1}}

    @GoogleApi.with_session
    @GoogleApi.with_token
    def extract(
            self,
            start_date: dt.date | str | None = None,
            end_date: dt.date | str | None = None,
            date_freq: Literal['D', 'W', 'M'] = 'D',
            date_range: Literal[
                "TODAY", "YESTERDAY", "LAST_7_DAYS", "LAST_14_DAYS", "LAST_30_DAYS", "LAST_BUSINESS_WEEK",
                "THIS_MONTH", "LAST_MONTH", "THIS_WEEK_SUN_TODAY", "THIS_WEEK_MON_TODAY",
                "LAST_WEEK_SUN_SAT", "LAST_WEEK_MON_SUN"] | None = "YESTERDAY",
            fields: Sequence[str] = list(),
            **kwargs
        ) -> JsonObject:
        """소재별 성과 데이터를 기간별로 구분해 조회하여 JSON 형식으로 반환한다.

        Parameters
        ----------
        start_date: dt.date | str | None
            조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 전달한다.   
            시작일을 지정하면 `date_range`는 무시된다. 기본값은 `None`
        end_date: dt.date | str | None
            조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 전달한다.   
            종료일을 지정하면 `date_range`는 무시된다. 기본값은 `None`
        date_freq: Literal['D', 'W', 'M']
            조회 범위 및 데이터 집계 기간. 기본값은 `'D'`(일별)
        date_range: str | None
            GAQL 사전 정의 조회 기간. `start_date` 또는 `end_date`가 전달되면 무시된다.   
            기본값은 `"YESTERDAY"`
        fields: Sequence[str]
            조회할 GAQL 필드 목록. 생략 시 클래스에 정의된 `fields` 속성을 사용한다.

        Returns
        -------
        list[dict]
            구글 광고 소재별 성과 데이터
        """
        if (start_date is not None) and (end_date is not None):
            context = self.generate_date_context(start_date, end_date, freq=date_freq)
            return (self.request_each(self.request_json, context)
                    .partial(fields=fields, customer_id=str(self.customer_id))
                    .run())
        else:
            response = self.request_json(
                start_date = start_date,
                end_date = end_date,
                date_range = (None if (start_date is not None) or (end_date is not None) else date_range),
                fields = fields,
            )
            return self.parse(response, customer_id=str(self.customer_id))

    @property
    def fields(self) -> list[str]:
        return [
            "ad_group_ad.ad.id",
            "campaign.id",
            "ad_group.id",
            "segments.date",
            "segments.device",
            "metrics.impressions",
            "metrics.clicks",
            "metrics.cost_micros",
            # "metrics.conversions",
            # "metrics.conversions_value",
        ]


class Asset(GoogleAds):
    """구글 광고 애셋 보고서를 조회하는 클래스.

    GAQL(Google Ads Query Language)로 searchStream 요청을 보내 데이터를 조회한다.
    - **API**: https://googleads.googleapis.com/v23/customers/{customer_id}/googleAds:searchStream
    - **Docs**: https://developers.google.com/google-ads/api/fields/v23/asset
    - **Referer**: https://ads.google.com/aw/assetreport/associations

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    customer_id: int | str
        구글 광고 고객 ID
    manager_id: int | str
        구글 광고 관리자 계정의 고객 ID
    developer_token: str
        구글 광고 API 개발자 토큰
    service_account: str | Path | dict[str, str]
        구글 서비스 계정 키 파일 경로 또는 파일 내용을 파싱한 딕셔너리
    """

    table = "asset"

    @GoogleApi.with_session
    @GoogleApi.with_token
    def extract(self, fields: Sequence[str] = list(), **kwargs) -> JsonObject:
        """애셋 보고서를 조회하여 JSON 형식으로 반환한다.

        Parameters
        ----------
        fields: Sequence[str]
            조회할 GAQL 필드 목록. 생략 시 클래스에 정의된 `fields` 속성을 사용한다.

        Returns
        -------
        list[dict]
            구글 광고 애셋 목록
        """
        response = self.request_json(fields = fields)
        return self.parse(response, customer_id=self.customer_id)

    @property
    def fields(self) -> list[str]:
        return [
            "asset.id",
            "asset.name",
            "asset.type",
            "asset.callout_asset.callout_text",
            "asset.image_asset.full_size.url",
            "asset.structured_snippet_asset.header",
            "asset.text_asset.text",
            "asset.youtube_video_asset.youtube_video_title",
        ]


class AssetView(Insight):
    """구글 광고 소재-애셋 관계를 조회하는 클래스.

    GAQL(Google Ads Query Language)로 searchStream 요청을 보내 데이터를 조회한다.
    - **API**: https://googleads.googleapis.com/v23/customers/{customer_id}/googleAds:searchStream
    - **Docs**: https://developers.google.com/google-ads/api/fields/v23/ad_group_ad_asset_view
    - **Referer**: https://ads.google.com/aw/unifiedassetreport/rsaassetdetails

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    customer_id: int | str
        구글 광고 고객 ID
    manager_id: int | str
        구글 광고 관리자 계정의 고객 ID
    developer_token: str
        구글 광고 API 개발자 토큰
    service_account: str | Path | dict[str, str]
        구글 서비스 계정 키 파일 경로 또는 파일 내용을 파싱한 딕셔너리
    """

    table = "ad_group_ad_asset_view"

    @property
    def fields(self) -> list[str]:
        return [
            # customers/{customer_id}/adGroupAdAssetViews/{ad_group_id}~{ad_id}~{asset_id}~{field_type}
            "ad_group_ad_asset_view.resource_name",
            "ad_group_ad_asset_view.field_type",
            "segments.date",
            "segments.device",
            "metrics.impressions",
            "metrics.clicks",
            "metrics.cost_micros",
            # "metrics.conversions",
            # "metrics.conversions_value",
        ]
