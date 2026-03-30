from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


def _common_config(fields: dict) -> dict:
    """구글 Ads API 응답 데이터에 대한 공통적인 파서 설정."""
    return dict(
        dtype = list,
        scope = "0.results",
        fields = fields,
    )


class _CommonParser(JsonTransformer):
    """구글 Ads API 응답 데이터에 대해 공통적인 파싱 로직을 구현한 공통 클래스.

    `identifier` 필드가 존재하는 항목만 선별하며, `parse_result` 후크를 통해 개별 항목을 추가 가공할 수 있다.
    """

    dtype = list
    scope = "0.results"
    identifier: str

    def parse(self, results: list[dict], inplace: bool = True, **kwargs) -> list[dict]:
        """`identifier` 필드가 있는 항목만 선별하여 `parse_result`로 추가 가공 후 반환한다."""
        from linkmerce.utils.nested import hier_get
        data = list()
        for result in results:
            if isinstance(result, dict) and hier_get(result, self.identifier):
                result = self.parse_result(result if inplace else self.copy(result))
                data.append(result)
        return data

    def parse_result(self, result: dict) -> dict:
        """개별 항목을 추가로 가공한다. 서브클래스에서 재정의한다."""
        return result

    def copy(self, result: dict) -> dict:
        from copy import deepcopy
        return deepcopy(result)


class Campaign(DuckDBTransformer):
    """구글 광고 캠페인 목록을 `google_campaign` 테이블에 적재하는 클래스."""

    extractor = "Campaign"
    tables = {"table": "google_campaign"}
    parser = "json"
    parser_config = _common_config(
        fields = {
            "campaign": ["id", "name", "advertisingChannelType", "status", "biddingStrategyType", "startDateTime"],
            "campaignBudget": ["amountMicros"],
            "metrics": ["impressions", "clicks", "costMicros"]
        },
    )
    params = {"customer_id": "$customer_id"}


class AdGroup(DuckDBTransformer):
    """구글 광고그룹 목록을 `google_adgroup` 테이블에 적재하는 클래스."""

    extractor = "AdGroup"
    tables = {"table": "google_adgroup"}
    parser = "json"
    parser_config = _common_config(
        fields = {
            "campaign": ["id"],
            "adGroup": ["id", "name", "type", "status", "targetCpaMicros"],
            "metrics": ["impressions", "clicks", "costMicros"]
        },
    )
    params = {"customer_id": "$customer_id"}


class AdParser(_CommonParser):
    """구글 광고 소재 목록을 추출하는 파서 클래스."""

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
        """광고 유형에 따라 정해진 경로에서 `name`을 추출한다."""
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
    """구글 광고 소재 목록을 `google_ad` 테이블에 적재하는 클래스."""

    extractor = "Ad"
    tables = {"table": "google_ad"}
    parser = AdParser
    params = {"customer_id": "$customer_id"}


class Insight(DuckDBTransformer):
    """구글 광고 소재의 성과 데이터를 날짜/기기별로 구분하여 `google_insight` 테이블에 적재하는 클래스."""

    extractor = "Insight"
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
    params = {"customer_id": "$customer_id"}


class AssetParser(_CommonParser):
    """구글 광고 애셋 목록을 추출하는 파서 클래스."""

    fields = {"asset": ["id", "type", "name", "url"]}
    identifier = "asset.type"

    def parse_result(self, result: dict) -> dict:
        self.set_asset_name(result["asset"])
        return result

    def set_asset_name(self, asset: dict, name: str | None = None, url: str | None = None):
        """애셋 유형에 따라 `name`과 `url`을 추출해 데이터에 반영한다."""
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
    """구글 광고 애셋 데이터를 `google_asset` 테이블에 적재하는 클래스."""

    extractor = "Asset"
    tables = {"table": "google_asset"}
    parser = AssetParser
    params = {"customer_id": "$customer_id"}


class AssetViewParser(_CommonParser):
    """구글 광고 소재-애셋 관계를 파싱하는 클래스."""

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
        """`resourceName`으로부터 `광고그룹ID`, `소재ID`, `애셋ID`를 추출해 결과에 반영한다."""
        resource = str(result["adGroupAdAssetView"]["resourceName"])
        ids = resource.split('/')[-1].split('~')
        adgroup_id, ad_id, asset_id = ids[:3] if len(ids) == 4 else ([None] * 3)
        result.update({
            "adGroup": {"id": adgroup_id},
            "adGroupAd": {"ad": {"id": ad_id}},
            "asset": {"id": asset_id},
        })


class AssetView(DuckDBTransformer):
    """구글 광고 소재-애셋 관계를 `google_asset_view` 테이블에 적재하는 클래스."""

    extractor = "AssetView"
    tables = {"table": "google_asset_view"}
    parser = AssetViewParser
    params = {"customer_id": "$customer_id"}
