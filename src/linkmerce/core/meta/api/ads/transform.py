from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal


def _common_config(fields: list, on_missing: Literal["ignore", "raise"] = "raise") -> dict:
    return dict(
        dtype = dict,
        scope = "data",
        fields = fields,
        defaults = {"account_id": "$account_id"},
        on_missing = on_missing,
    )


class Campaigns(DuckDBTransformer):
    tables = {"table": "meta_campaigns"}
    parser = "json"
    parser_config = _common_config(
        fields = ["id", "name", "objective", "effective_status", "created_time"],
        on_missing = "raise",
    )


class Adsets(DuckDBTransformer):
    tables = {"table": "meta_adsets"}
    parser = "json"
    parser_config = _common_config(
        fields = ["id", "name", "campaign_id", "effective_status", "daily_budget", "created_time"],
        on_missing = "ignore",
    )


class Ads(DuckDBTransformer):
    tables = {"table": "meta_ads"}
    parser = "json"
    parser_config = _common_config(
        fields = ["id", "name", "campaign_id", "adset_id", "effective_status", "created_time"],
        on_missing = "raise",
    )


class Insights(DuckDBTransformer):
    tables = {"campaigns": "meta_campaigns", "adsets": "meta_adsets", "ads": "meta_ads", "insights": "meta_insights"}
    parser = "json"
    parser_config = _common_config(
        fields = [
            "campaign_id", "campaign_name", "adset_id", "adset_name", "ad_id", "ad_name",
            "impressions", "reach", "clicks", "inline_link_clicks", "spend", "date_start"
        ],
        on_missing = "raise",
    )
