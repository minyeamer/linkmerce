from __future__ import annotations

from linkmerce.common.transform import ResponseTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Callable, Literal, TypeVar
    from linkmerce.common.transform import TableKey, TableName
    import datetime as dt
    Column = TypeVar("Column", bound=str)
    ColumnType = Literal["STRING", "INTEGER", "DATETIME", "BOOLEAN"]


def _cast(type: ColumnType) -> Callable[[str], str | float | int | dt.date]:
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


class TsvTransformer(ResponseTransformer):
    columns: list[tuple[Column, ColumnType]] = list()
    convert_dtypes: bool = True

    def pre_init(self, columns: list[tuple] | None = None, convert_dtypes: bool | None = None, **kwargs):
        self.set_columns(columns)
        if convert_dtypes is not None:
            self.convert_dtypes = convert_dtypes

    def set_columns(self, columns: list[tuple] | None = None):
        if columns is not None:
            self.columns = columns
        if not self.columns:
            self.raise_parse_error("TSV data parsing requires columns list.")

    def parse(self, obj: str, **kwargs) -> list[dict]:
        from io import StringIO
        import csv
        data = list()

        if obj:
            reader = csv.reader(StringIO(obj), delimiter='\t')
            columns, types = zip(*self.columns)
            functions = [_cast(dtype) if self.convert_dtypes else (lambda x: x) for dtype in types]
            for row in reader:
                values = [cast(value) for cast, value in zip(functions, row)]
                data.append(dict(zip(columns, values)))
        return data


class Campaign(DuckDBTransformer):
    tables = {"table": "searchad_campaign"}
    parser = TsvTransformer
    parser_config = dict(
        columns = [
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
    )


class Adgroup(DuckDBTransformer):
    tables = {"table": "searchad_adgroup"}
    parser = TsvTransformer
    parser_config = dict(
        columns = [
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
    )


class PowerLinkAd(TsvTransformer):
    columns = [
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
    columns = [
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
    columns = [
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
    columns = [
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
    columns = [
        ("Customer ID", "INTEGER"),
        ("Product Group Relation ID", "STRING"),
        ("Product Group ID", "STRING"),
        ("AD group ID", "STRING"),
        ("regTm", "DATETIME"),
        ("delTm", "DATETIME"),
    ]


class BrandThumbnailAd(TsvTransformer):
    columns = [
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
    columns = [
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
    columns = [
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


AD_TABLE_KEYS: list[TableKey] = [
    "power_link_ad",
    "power_contents_ad",
    "shopping_product_ad",
    "brand_ad",
    # 위 4개 테이블은 `ad` 테이블로의 `transform` 대상
    "product_group",
    "product_group_rel",
    "brand_thumbnail_ad",
    "brand_banner_ad"
]

class Ad(DuckDBTransformer):
    queries = (
        ["create"]
        + [f"bulk_insert_{key}" for key in AD_TABLE_KEYS]
        + [f"transform_{key}" for key in AD_TABLE_KEYS[:4]]
    )
    tables = {"table": "searchad_ad", **{key: key for key in AD_TABLE_KEYS}}

    def transform(self, obj: dict[str,str], **kwargs) -> dict:
        result_set = dict()
        table_parser = self.table_parser

        for report_type, tsv_data in obj.items():
            table_key, parser = table_parser[report_type]
            query_key = f"bulk_insert_{table_key}"
            result_set[query_key] = self.bulk_insert(parser().transform(tsv_data), query_key, render=self.get_table(table_key))

        for table_key in AD_TABLE_KEYS[:4]:
            query_key = f"transform_{table_key}"
            result_set[query_key] = self.insert_into(query_key, render={"table": self.table, **self.get_table(table_key)})

        return result_set

    def get_table(self, table_key: TableKey) -> dict[TableKey, TableName]:
        return {table_key: self.tables[table_key]}

    @property
    def table_parser(self) -> dict[str, tuple[TableKey, type[TsvTransformer]]]:
        return {
            "Ad":
                ("power_link_ad", PowerLinkAd),
            "ContentsAd":
                ("power_contents_ad", PowerContentsAd),
            "ShoppingProduct":
                ("shopping_product_ad", ShoppingProductAd),
            "ProductGroup":
                ("product_group", ProductGroup),
            "ProductGroupRel":
                ("product_group_rel", ProductGroupRel),
            "BrandThumbnailAd":
                ("brand_thumbnail_ad", BrandThumbnailAd),
            "BrandBannerAd":
                ("brand_banner_ad", BrandBannerAd),
            "BrandAd":
                ("brand_ad", BrandAd),
        }
