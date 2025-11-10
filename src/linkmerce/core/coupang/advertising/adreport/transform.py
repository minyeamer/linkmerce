from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject


class CampaignList(JsonTransformer):
    dtype = dict
    path = ["campaigns"]


class AdgroupList(JsonTransformer):
    dtype = dict
    path = ["campaigns"]

    def transform(self, campaigns: list[dict], **kwargs) -> list[dict]:
        return [self.validate_adgroup(adgroup)
                for campaign in campaigns
                for adgroup in (campaign.get("groupList") or list()) if isinstance(adgroup, dict)]

    def validate_adgroup(self, adgroup: dict) -> dict:
        if "paCampaignId" in adgroup:
            adgroup["campaignId"] = adgroup["paCampaignId"]
        if "timestamp" in adgroup:
            adgroup.update(adgroup["timestamp"])
        return adgroup


class Campaign(DuckDBTransformer):
    queries = ["create_campaign", "select_campaign", "insert_campaign", "create_adgroup", "select_adgroup", "insert_adgroup"]

    def set_tables(self, tables: dict | None = None):
        base = dict(campaign="coupang_campaign", adgroup="coupang_adgroup")
        super().set_tables(dict(base, **(tables or dict())))

    def create_table(self, **kwargs):
        super().create_table(key="create_campaign", table=":campaign:")
        super().create_table(key="create_adgroup", table=":adgroup:")

    def transform(self, obj: JsonObject, goal_type: str = "SALES", vendor_id: str | None = None, **kwargs):
        campaigns = CampaignList().transform(obj)
        if campaigns:
            result = self.insert_into_table(campaigns,
                key="insert_campaign", table=":campaign:", values=":select_campaign:", params=dict(vendor_id=vendor_id))

            adgroups = AdgroupList().transform(campaigns)
            if adgroups:
                params = dict(goal_type=goal_type, vendor_id=vendor_id)
                return [result, self.insert_into_table(adgroups,
                    key="insert_adgroup", table=":adgroup:", values=":select_adgroup:", params=params)]
            else:
                return [result]


class CreativeList(JsonTransformer):
    dtype = dict
    path = ["adGroup", "videoAds"]

    def transform(self, obj: JsonObject, **kwargs) -> list[dict]:
        return [creative
                for ad in (self.parse(obj) or list()) if isinstance(ad, dict)
                for creative in (ad.get("creatives") or list())]


class Creative(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, vendor_id: str | None = None, **kwargs):
        creatives = CreativeList().transform(obj)
        if creatives:
            return self.insert_into_table(creatives, params=dict(vendor_id=vendor_id))


class ProductAdReport(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, vendor_id: str | None = None, **kwargs):
        from linkmerce.utils.excel import excel2json
        reports = excel2json(obj, warnings=False)
        if reports:
            return self.insert_into_table(reports, params=dict(vendor_id=vendor_id))


class NewCustomerAdReport(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, vendor_id: str | None = None, **kwargs):
        from linkmerce.utils.excel import excel2json
        reports = excel2json(obj, warnings=False)
        if reports:
            return self.insert_into_table(reports, params=dict(vendor_id=vendor_id))
