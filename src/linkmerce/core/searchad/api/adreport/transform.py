from __future__ import annotations

from linkmerce.common.transform import Transformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Callable, Literal
    import datetime as dt


def _convert(type: Literal["STRING","INTEGER","DATETIME","BOOLEAN"]) -> Callable[[str], str | float | int | dt.date]:
    if type == "INTEGER":
        from linkmerce.utils.cast import safe_int
        return lambda x: safe_int(x)
    elif type == "DATETIME":
        from linkmerce.utils.date import safe_strptime
        return lambda x: safe_strptime(x, format="%Y-%m-%dT%H:%M:%SZ", tzinfo="UTC", astimezone="Asia/Seoul", droptz=True)
    elif type == "BOOLEAN":
        return lambda x: {"TRUE":True, "FALSE":False}.get(str(x).upper())
    else:
        return lambda x: x


class TsvTransformer(Transformer):

    def transform(self, obj: str, convert_dtypes: bool = True, **kwargs) -> list[list[str]]:
        from io import StringIO
        import csv
        if obj:
            reader = csv.reader(StringIO(obj), delimiter='\t')
            header, types = zip(*self.columns)
            apply = [_convert(dtype) if convert_dtypes else (lambda x: x) for dtype in types]
            return [list(header)] + [[func(value) for func, value in zip(apply, row)] for row in reader]
        else:
            return list()

    @property
    def columns(self) -> list[tuple[str,Literal["STRING","INTEGER","DATETIME","BOOLEAN"]]]:
        return list()


class CampaignList(TsvTransformer):

    @property
    def columns(self) -> list[tuple]:
        return [
            ("Customer ID", "INTEGER"),
            ("Campaign ID", "STRING"),
            ("Campaign Name", "STRING"),
            ("Campaign Type", "INTEGER"),
            ("Delivery Method", "INTEGER"),
            ("Using Period", "INTEGER"),
            ("Period Start Date", "DATETIME"),
            ("Period End Date", "DATETIME"),
            ("regTm", "DATETIME"),
            ("delTm", "DATETIME"),
            ("ON/OFF", "INTEGER"),
            ("Shared budget id", "STRING"),
        ]


class Campaign(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: str, **kwargs):
        report_csv = CampaignList().transform(obj, convert_dtypes=True)
        if len(report_csv) > 1:
            report_json = [dict(zip(report_csv[0], row)) for row in report_csv[1:]]
            self.insert_into_table(report_json)


class AdgroupList(TsvTransformer):

    @property
    def columns(self) -> list[tuple]:
        return [
            ("Customer ID", "INTEGER"),
            ("Ad Group ID", "STRING"),
            ("Campaign ID", "STRING"),
            ("Ad Group Name", "STRING"),
            ("Ad Group Bid amount", "INTEGER"),
            ("ON/OFF", "INTEGER"),
            ("Using contents network bid", "INTEGER"),
            ("Contents network bid", "INTEGER"),
            ("PC network bidding weight", "INTEGER"),
            ("Mobile network bidding weight", "BIDDING"),
            ("Business Channel Id(Mobile)", "STRING"),
            ("Business Channel Id(PC)", "STRING"),
            ("regTm", "DATETIME"),
            ("delTm", "DATETIME"),
            ("Content Type", "STRING"),
            ("Ad group type", "INTEGER"),
            ("Shared budget id", "STRING"),
            ("Using Expanded Search", "INTEGER"),
        ]


class Adgroup(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: str, **kwargs):
        report_csv = AdgroupList().transform(obj, convert_dtypes=True)
        if len(report_csv) > 1:
            report_json = [dict(zip(report_csv[0], row)) for row in report_csv[1:]]
            self.insert_into_table(report_json)


class PowerLinkAd(TsvTransformer):

    @property
    def columns(self) -> list[tuple]:
        return [
            ("Customer ID", "INTEGER"),
            ("Ad Group ID", "STRING"),
            ("Ad ID", "STRING"),
            ("Ad Creative Inspect Status", "INTEGER"),
            ("Subject", "STRING"),
            ("Description", "STRING"),
            ("Landing URL(PC)", "STRING"),
            ("Landing URL(Mobile)", "STRING"),
            ("ON/OFF", "INTEGER"),
            ("regTm", "DATETIME"),
            ("delTm", "DATETIME"),
        ]


class PowerContentsAd(TsvTransformer):

    @property
    def columns(self) -> list[tuple]:
        return [
            ("Customer ID", "INTEGER"),
            ("Ad Group ID", "STRING"),
            ("Ad ID", "STRING"),
            ("Ad Creative Inspect Status", "INTEGER"),
            ("Subject", "STRING"),
            ("Description", "STRING"),
            ("Landing URL(PC)", "STRING"),
            ("Landing URL(Mobile)", "STRING"),
            ("Image URL", "STRING"),
            ("Company Name", "STRING"),
            ("Contents Issue Date", "DATETIME"),
            ("Release Date", "DATETIME"),
            ("ON/OFF", "INTEGER"),
            ("regTm", "DATETIME"),
            ("delTm", "DATETIME"),
        ]


class ShoppingProductAd(TsvTransformer):

    @property
    def columns(self) -> list[tuple]:
        return [
            ("Customer ID", "INTEGER"),
            ("Ad Group ID", "STRING"),
            ("Ad ID", "STRING"),
            ("Ad Creative Inspect Status", "INTEGER"),
            ("ON/OFF", "INTEGER"),
            ("Ad Product Name", "STRING"),
            ("Ad Image URL", "STRING"),
            ("Bid", "INTEGER"),
            ("Using Ad Group Bid Amount", "BOOLEAN"),
            ("Ad Link Status", "INTEGER"),
            ("regTm", "DATETIME"),
            ("delTm", "DATETIME"),
            ("Product ID", "STRING"),
            ("Product ID Of Mall", "STRING"),
            ("Product Name", "STRING"),
            ("Product Image URL", "STRING"),
            ("PC Landing URL", "STRING"),
            ("Mobile Landing URL", "STRING"),
            ("Price", "STRING"),
            ("Delivery Fee", "STRING"),
            ("NAVER Shopping Category Name 1", "STRING"),
            ("NAVER Shopping Category Name 2", "STRING"),
            ("NAVER Shopping Category Name 3", "STRING"),
            ("NAVER Shopping Category Name 4", "STRING"),
            ("NAVER Shopping Category ID 1", "STRING"),
            ("NAVER Shopping Category ID 2", "STRING"),
            ("NAVER Shopping Category ID 3", "STRING"),
            ("NAVER Shopping Category ID 4", "STRING"),
            ("Category Name of Mall", "STRING"),
        ]


class ProductGroup(TsvTransformer):

    @property
    def columns(self) -> list[tuple]:
        return [
            ("Customer ID", "INTEGER"),
            ("Product group ID", "STRING"),
            ("Business channel ID", "STRING"),
            ("Name", "STRING"),
            ("Registration method", "INTEGER"),
            ("Registered product type", "INTEGER"),
            ("Attribute json1", "STRING"),
            ("regTm", "DATETIME"),
            ("delTm", "DATETIME"),
        ]


class ProductGroupRel(TsvTransformer):

    @property
    def columns(self) -> list[tuple]:
        return [
            ("Customer ID", "INTEGER"),
            ("Product Group Relation ID", "STRING"),
            ("Product Group ID", "STRING"),
            ("AD group ID", "STRING"),
            ("regTm", "DATETIME"),
            ("delTm", "DATETIME"),
        ]


class BrandThumbnailAd(TsvTransformer):

    @property
    def columns(self) -> list[tuple]:
        return [
            ("Customer ID", "INTEGER"),
            ("Ad Group ID", "STRING"),
            ("Ad ID", "STRING"),
            ("Ad Creative Inspect Status", "INTEGER"),
            ("ON/OFF", "INTEGER"),
            ("Headline", "STRING"),
            ("description", "STRING"),
            ("extra Description", "STRING"),
            ("Logo image path", "STRING"),
            ("Link URL", "STRING"),
            ("Thumbnail Image path", "STRING"),
            ("regTm", "DATETIME"),
            ("delTm", "DATETIME"),
        ]


class BrandBannerAd(TsvTransformer):

    @property
    def columns(self) -> list[tuple]:
        return [
            ("Customer ID", "INTEGER"),
            ("Ad Group ID", "STRING"),
            ("Ad ID", "STRING"),
            ("Ad Creative Inspect Status", "INTEGER"),
            ("ON/OFF", "INTEGER"),
            ("Headline", "STRING"),
            ("description", "STRING"),
            ("Logo image path", "STRING"),
            ("Link URL", "STRING"),
            ("Thumbnail Image path", "STRING"),
            ("regTm", "DATETIME"),
            ("delTm", "DATETIME"),
        ]


class BrandAd(TsvTransformer):

    @property
    def columns(self) -> list[tuple]:
        return [
            ("Customer ID", "INTEGER"),
            ("Ad Group ID", "STRING"),
            ("Ad ID", "STRING"),
            ("Ad Creative Inspect Status", "INTEGER"),
            ("ON/OFF", "INTEGER"),
            ("Headline", "STRING"),
            ("description", "STRING"),
            ("Logo image path", "STRING"),
            ("Link URL", "STRING"),
            ("Image path", "STRING"),
            ("regTm", "DATETIME"),
            ("delTm", "DATETIME"),
        ]


class Ad(DuckDBTransformer):
    queries = ([f"{keyword}_{report_table}"
        for report_table in [
            "power_link_ad",
            "power_contents_ad",
            "shopping_product_ad",
            "product_group",
            "product_group_rel",
            "brand_thumbnail_ad",
            "brand_banner_ad",
            "brand_ad"
        ]
        for keyword in (
            ["create", "select", "insert"]
            + (["load"] if report_table in {"power_link_ad", "power_contents_ad", "shopping_product_ad", "brand_ad"} else [])
        )
    ] + ["create_ad"])

    def transform(self, obj: dict[str,str], table: str = ":default:", **kwargs):
        transformer = self.transformer
        for report_type, tsv_data in obj.items():
            report_table, func = transformer[report_type]
            rows = func(tsv_data)
            if len(rows) > 1:
                json_data = [dict(zip(rows[0], row)) for row in rows[1:]]
                self.insert_into_table(json_data, key=f"insert_{report_table}", table=report_table, values=f":select_{report_table}:")
        for report_table in ["power_link_ad", "power_contents_ad", "shopping_product_ad", "brand_ad"]:
            self.insert_into(key=f"load_{report_table}", render=dict(table=table))

    def create_table(self, **kwargs):
        for query in self.queries:
            if query.startswith("create_"):
                table = ":default:" if query == "create_ad" else query[7:]
                super().create_table(key=query, table=table)

    @property
    def transformer(self) -> dict[str, tuple[str, Callable[[str],list[list[str]]]]]:
        return {
            "Ad":
                ("power_link_ad", PowerLinkAd().transform),
            "ContentsAd":
                ("power_contents_ad", PowerContentsAd().transform),
            "ShoppingProduct":
                ("shopping_product_ad", ShoppingProductAd().transform),
            "ProductGroup":
                ("product_group", ProductGroup().transform),
            "ProductGroupRel":
                ("product_group_rel", ProductGroupRel().transform),
            "BrandThumbnailAd":
                ("brand_thumbnail_ad", BrandThumbnailAd().transform),
            "BrandBannerAd":
                ("brand_banner_ad", BrandBannerAd().transform),
            "BrandAd":
                ("brand_ad", BrandAd().transform),
        }
