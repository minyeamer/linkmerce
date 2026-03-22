from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


def _common_config(fields: list) -> dict:
    return dict(
        dtype = dict,
        scope = "data",
        fields = fields,
    )


class Campaigns(DuckDBTransformer):
    """메타 광고 캠페인 목록을 `meta_campaigns` 테이블에 적재하는 클래스."""

    extractor = "Campaigns"
    tables = {"table": "meta_campaigns"}
    parser = "json"
    parser_config = _common_config(
        fields = ["id", "name", "objective", "effective_status", "created_time"],
    )
    params = {"account_id": "$account_id"}


class Adsets(DuckDBTransformer):
    """메타 광고세트 목록을 `meta_adsets` 테이블에 적재하는 클래스."""

    extractor = "Adsets"
    tables = {"table": "meta_adsets"}
    parser = "json"
    parser_config = _common_config(
        fields = ["id", "name", "campaign_id", "effective_status", {"daily_budget": None}, "created_time"],
    )
    params = {"account_id": "$account_id"}


class Ads(DuckDBTransformer):
    """메타 광고 목록을 `meta_ads` 테이블에 적재하는 클래스."""

    extractor = "Ads"
    tables = {"table": "meta_ads"}
    parser = "json"
    parser_config = _common_config(
        fields = ["id", "name", "campaign_id", "adset_id", "effective_status", "created_time"],
    )
    params = {"account_id": "$account_id"}


class Insights(DuckDBTransformer):
    """메타 광고 성과 보고서와 캠페인, 광고세트, 소재 목록을 각각의 테이블에 변환 및 적재하는 클래스.

    테이블 키 | 테이블명 | 설명
    - `campaigns` | `meta_campaigns` | 메타 광고 캠페인 목록
    - `adsets` | `meta_adsets` | 메타 광고세트 목록
    - `ads` | `meta_ads` | 메타 광고 목록
    - `insights` | `meta_insights` | 메타 광고 성과 보고서"""

    extractor = "Insights"
    tables = {"campaigns": "meta_campaigns", "adsets": "meta_adsets", "ads": "meta_ads", "insights": "meta_insights"}
    parser = "json"
    parser_config = _common_config(
        fields = [
            "campaign_id", "campaign_name", "adset_id", "adset_name", "ad_id", "ad_name",
            "impressions", "reach", "clicks", "inline_link_clicks", "spend", "date_start"
        ],
    )
    params = {"account_id": "$account_id"}
