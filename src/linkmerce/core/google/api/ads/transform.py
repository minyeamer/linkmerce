from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject


class AdResults(JsonTransformer):
    dtype = list
    path = [0,"results"]


class _AdTransformer(DuckDBTransformer):
    queries: list[str] = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, customer_id: str, **kwargs):
        objects = AdResults().transform(obj)
        if objects:
            self.insert_into_table(objects, params=dict(customer_id=customer_id))


class Campaign(_AdTransformer):
    queries = ["create", "select", "insert"]


class AdGroup(_AdTransformer):
    queries = ["create", "select", "insert"]


class Asset(_AdTransformer):
    queries = ["create", "select", "insert"]


class AdList(JsonTransformer):
    dtype = list
    path = [0,"results"]

    def transform(self, obj: JsonObject, **kwargs) -> list[dict]:
        from linkmerce.utils.map import hier_get
        obj = super().transform(obj) or list()
        return [self.parse_ad_name(m.copy()) for m in obj
            if isinstance(m, dict) and hier_get(m, ["adGroupAd","ad","type"])]

    def parse_ad_name(self, m: dict) -> dict:
        ad_type, ad_name = str(m["adGroupAd"]["ad"]["type"]), None
        keywords = ad_type.lower().split('_')
        key = keywords[0] + ''.join([keyword.capitalize() for keyword in keywords[1:]])
        if key in m["adGroupAd"]["ad"]:
            details = m["adGroupAd"]["ad"][key]
            if "headlines" in details:
                ad_name = " | ".join([headline["text"] for headline in details["headlines"]])
            elif "headline" in details:
                ad_name = details["headline"]
            elif "headline_part1" in details:
                ad_name = details["headline_part1"]
        m["adGroupAd"]["ad"]["name"] = ad_name
        return m


class Ad(_AdTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, customer_id: str, **kwargs):
        objects = AdList().transform(obj)
        if objects:
            self.insert_into_table(objects, params=dict(customer_id=customer_id))


class AssetViewList(JsonTransformer):
    dtype = list
    path = [0,"results"]

    def transform(self, obj: JsonObject, **kwargs) -> list[dict]:
        from linkmerce.utils.map import hier_get
        obj = super().transform(obj) or list()
        return [self.parse_ad_id(m.copy()) for m in obj
            if isinstance(m, dict) and hier_get(m, ["adGroupAdAssetView","resourceName"])]

    def parse_ad_id(self, m: dict) -> dict:
        resource = str(m["adGroupAdAssetView"]["resourceName"])
        ids = resource.split('/')[-1].split('~')
        adgroup_id, ad_id, asset_id = ids[:3] if len(ids) == 4 else ([None] * 3)
        m.update({
            "adGroup": {"id": adgroup_id},
            "adGroupAd": {"ad": {"id": ad_id}},
            "asset": {"id": asset_id},
        })
        return m


class AssetView(_AdTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, customer_id: str, **kwargs):
        objects = AssetViewList().transform(obj)
        if objects:
            self.insert_into_table(objects, params=dict(customer_id=customer_id))
