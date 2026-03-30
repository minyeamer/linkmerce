from __future__ import annotations
from linkmerce.core.google.api import GoogleApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    from pathlib import Path
    import datetime as dt


class GoogleAds(GoogleApi):
    """구글 Ads API로 광고 데이터를 조회하는 공통 클래스.

    GAQL(구글 Ads Query Language)로 searchStream 요청을 보내 데이터를 조회한다.
    - API 문서: https://developers.google.com/google-ads/api/fields/v23/overview"""

    service: str = "googleads"
    method: str = "POST"
    version: str = "v23"
    table: str

    @property
    def url(self) -> str:
        """구글 Ads searchStream API URL을 조합해 반환한다."""
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
        """GAQL 쿼리로 구글 Ads 데이터를 조회해 JSON 형식으로 반환한다."""
        response = self.request_json(
            start_date = start_date,
            end_date = end_date,
            date_range = (None if (start_date is not None) or (end_date is not None) else date_range),
            fields = fields,
        )
        # data_start, data_end = self.fetch_date_range(start_date, end_date, date_range)
        return self.parse(response, customer_id=str(self.customer_id))

    def set_service_account(
            self,
            customer_id: int | str,
            manager_id: int | str,
            developer_token: str,
            service_account: str | Path | dict[str, str],
            version: str = str(),
            **configs,
        ):
        """구글 서비스 계정 인증 정보를 설정한다."""
        super().set_service_account(
            service_account = service_account,
            scope = "https://www.googleapis.com/auth/adwords",
            customer_id = customer_id,
            manager_id = manager_id,
            developer_token = developer_token,
            version = version,
            **configs
        )

    def build_request_json(self, fields: Sequence[str] = list(), **kwargs) -> dict[str, str]:
        """GAQL 쿼리를 구성한다."""
        fields = ', '.join(fields if fields else self.fields)
        where = f" WHERE {cond}" if (cond := self.where(**kwargs)) else str()
        return {"query": f"SELECT {fields} FROM {self.table}{where}"}

    def build_request_headers(self, **kwargs) -> dict[str, str]:
        """구글 Ads API 요청 헤더를 구성한다."""
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
            "developer-token": self.developer_token,
            "login-customer-id": str(self.manager_id),
        }

    @property
    def customer_id(self) -> int | str:
        return self.get_config("customer_id")

    @property
    def manager_id(self) -> int | str:
        return self.get_config("manager_id")

    @property
    def developer_token(self) -> str:
        return self.get_config("developer_token")

    @property
    def fields(self) -> list[str]:
        return list()

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
    """구글 광고 캠페인 목록을 조회하는 클래스.
    - API 문서: https://developers.google.com/google-ads/api/fields/v23/campaign"""

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
    """구글 광고그룹 목록을 조회하는 클래스.
    - API 문서: https://developers.google.com/google-ads/api/fields/v23/ad_group"""

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
    """구글 광고 소재 목록을 조회하는 클래스.
    - API 문서: https://developers.google.com/google-ads/api/fields/v23/ad_group_ad"""

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
    """구글 광고 소재의 성과 데이터를 날짜/기기별로 구분해 조회하는 클래스.

    `RequestEach` Task를 사용하여 기간별 데이터를 조회한다.
    - API 문서: https://developers.google.com/google-ads/api/fields/v23/ad_group_ad"""

    table = "ad_group_ad"

    @property
    def default_options(self) -> dict:
        return {"RequestEach": {"request_delay": 1}}

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
        """소재의 성과 데이터를 기간별로 조회하여 JSON 형식으로 반환한다."""
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
    """구글 광고 애셋 목록을 조회하는 클래스.
    - API 문서: https://developers.google.com/google-ads/api/fields/v23/asset"""

    table = "asset"

    @GoogleApi.with_session
    @GoogleApi.with_token
    def extract(self, fields: Sequence[str] = list(), **kwargs) -> JsonObject:
        """애셋 목록을 조회하여 JSON 형식으로 반환한다."""
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
    - API 문서: https://developers.google.com/google-ads/api/fields/v23/ad_group_ad_asset_view"""

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
