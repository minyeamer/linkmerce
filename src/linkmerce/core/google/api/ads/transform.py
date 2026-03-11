from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject


def _common_config(fields: dict) -> dict:
    return dict(
        dtype = list,
        scope = "0.results",
        fields = fields,
        defaults = {"customerId": "$customer_id"},
        on_missing = "raise",
    )


class _CommonParser(JsonTransformer):
    dtype = list
    scope = "0.results"
    defaults = {"customerId": "$customer_id"}
    on_missing = "raise"
    identifier: str

    def parse(self, results: JsonObject, inplace: bool = True, **kwargs) -> list[dict]:
        if not isinstance(results, list):
            self.raise_parse_error("Could not parse the results.")

        from linkmerce.utils.nested import hier_get
        data = list()
        for result in results:
            if isinstance(result, dict) and hier_get(result, self.identifier):
                result = self.parse_result(result if inplace else self.copy(result))
                data.append(result)
        return data

    def parse_result(self, result: dict) -> dict:
        return result

    def copy(self, result: dict) -> dict:
        from copy import deepcopy
        return deepcopy(result)


class Campaign(DuckDBTransformer):
    tables = {"table": "google_campaign"}
    parser = "json"
    parser_config = _common_config(
        fields = {
            "campaign": ["id", "name", "advertisingChannelType", "status", "biddingStrategyType", "startDateTime"],
            "campaignBudget.amountMicros": None,
            "metrics": ["impressions", "clicks", "costMicros"]
        },
    )


class AdGroup(DuckDBTransformer):
    tables = {"table": "google_adgroup"}
    parser = "json"
    parser_config = _common_config(
        fields = {
            "campaign": ["id"],
            "adGroup": ["id", "name", "type", "status", "targetCpaMicros"],
            "metrics": ["impressions", "clicks", "costMicros"]
        },
    )


class AdParser(_CommonParser):
    fields = {
        "campaign": ["id"],
        "adGroup": ["id"],
        "adGroupAd.ad": ["id", "name", "type"],
        "adGroupAd": ["status"],
        "metrics": ["impressions", "clicks", "costMicros"]
    }
    identifier = "adGroupAd.ad.type"

    def parse_result(self, result: dict) -> dict:
        self.set_ad_name(result["adGroupAd"]["ad"])
        return result

    def set_ad_name(self, ad: dict, name: str | None = None):
        keywords = str(ad["type"]).lower().split('_')
        key = keywords[0] + ''.join([keyword.capitalize() for keyword in keywords[1:]])
        if key in ad:
            details = ad[key]
            if "headlines" in details:
                name = " | ".join([headline["text"] for headline in details["headlines"]])
            elif "headline" in details:
                name = details["headline"]
            elif "headline_part1" in details:
                name = details["headline_part1"]
        ad["name"] = name


class Ad(DuckDBTransformer):
    tables = {"table": "google_ad"}
    parser = AdParser


class Insight(DuckDBTransformer):
    tables = {"table": "google_insight"}
    parser = "json"
    parser_config = _common_config(
        fields = {
            "campaign": ["id"],
            "adGroup": ["id"],
            "adGroupAd": ["ad.id"],
            "segments": ["date", "device"],
            "metrics": ["impressions", "clicks", "costMicros"]
        },
    )
    identifier = "asset.type"


class AssetParser(_CommonParser):
    fields = {"asset": ["id", "type", "name", "url"]}

    def parse_result(self, result: dict) -> dict:
        self.set_asset_name(result["asset"])
        return result

    def set_asset_name(self, asset: dict, name: str | None = None, url: str | None = None):
        from linkmerce.utils.nested import hier_get
        type = asset["type"]
        if type == 'TEXT':
            name = hier_get(asset, "textAsset.text")
        elif type == 'IMAGE':
            name = hier_get(asset, "name")
            url = hier_get(asset, "imageAsset.fullSize.url")
        elif type == 'YOUTUBE_VIDEO':
            name = hier_get(asset, "youtubeVideoAsset.youtubeVideoTitle")
        elif type == 'CALLOUT':
            name = hier_get(asset, "calloutAsset.calloutText")
        elif type == 'STRUCTURED_SNIPPET':
            name = hier_get(asset, "structuredSnippetAsset.header")
        asset.update(name=name, url=url)


class Asset(DuckDBTransformer):
    tables = {"table": "google_asset"}
    parser = AssetParser


class AssetViewList(_CommonParser):
    fields = {
        "adGroup": ["id"],
        "adGroupAd": ["ad.id"],
        "asset": ["id"],
        "adGroupAdAssetView": ["fieldType"],
        "segments": ["date", "device"],
        "metrics": ["impressions", "clicks", "costMicros"]
    }
    identifier = "adGroupAdAssetView.resourceName"

    def parse_result(self, result: dict) -> dict:
        self.set_ad_id(result)
        return result

    def set_ad_id(self, result: dict):
        resource = str(result["adGroupAdAssetView"]["resourceName"])
        ids = resource.split('/')[-1].split('~')
        adgroup_id, ad_id, asset_id = ids[:3] if len(ids) == 4 else ([None] * 3)
        result.update({
            "adGroup": {"id": adgroup_id},
            "adGroupAd": {"ad": {"id": ad_id}},
            "asset": {"id": asset_id},
        })


class AssetView(DuckDBTransformer):
    tables = {"table": "google_asset_view"}
    parser = "json"
    parser_config = _common_config(
        fields = {
            "adGroup": ["id"],
            "adGroupAd": ["ad.id"],
            "asset": ["id"],
            "adGroupAdAssetView": ["fieldType"],
            "segments": ["date", "device"],
            "metrics": ["impressions", "clicks", "costMicros"]
        },
    )
