from __future__ import annotations

from linkmerce.common.transform import ResponseTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Callable, Literal, TypeVar
    from linkmerce.common.transform import TableKey, TableName
    import datetime as dt
    Column = TypeVar("Column", bound=str)
    ColumnType = Literal["STRING", "INTEGER", "FLOAT", "DATETIME", "BOOLEAN"]


def _cast(type: ColumnType) -> Callable[[str], str | float | int | dt.date | dt.datetime]:
    if type == "INTEGER":
        from linkmerce.utils.cast import safe_int
        return lambda x: safe_int(x)
    elif type == "FLOAT":
        from linkmerce.utils.cast import safe_float
        return lambda x: safe_float(x)
    elif type == "DATE":
        from linkmerce.utils.date import safe_strpdate
        return lambda x: safe_strpdate(x, format="%Y%m%d")
    elif type == "DATETIME":
        from linkmerce.utils.date import safe_strptime
        return lambda x: safe_strptime(x, format="%Y-%m-%dT%H:%M:%SZ", tzinfo="UTC", astimezone="Asia/Seoul", droptz=True)
    elif type == "BOOLEAN":
        return lambda x: {"TRUE":True, "FALSE":False}.get(str(x).upper())
    else:
        return lambda x: x


class TsvTransformer(ResponseTransformer):
    """네이버 검색광고 대용량 다운로드 보고서 (TSV 형식) 데이터를 JSON 형식으로 파싱하는 클래스.

    주요 설정 변수:
    - `columns` - `(칼럼명, 타입)` 리스트
    - `convert_dtypes` - 데이터 타입 변환 여부 (기본값 `True`)
    
    보고서 유형별 칼럼 구성은
    [대용량 다운로드 보고서 데이터 정의서](https://searchad.naver.com/File/downloadfilen/?type=10&filename=masterreport808.pdf)를
    참고한다."""

    columns: list[tuple[Column, ColumnType]] = list()
    convert_dtypes: bool = True

    def pre_init(self, columns: list[tuple] | None = None, convert_dtypes: bool | None = None, **kwargs):
        self.set_columns(columns)
        if convert_dtypes is not None:
            self.convert_dtypes = convert_dtypes

    def set_columns(self, columns: list[tuple] | None = None):
        """칼럼 목록을 설정하고, 칼럼 목록이 없으면 `ParseError`를 발생시킨다."""
        if columns is not None:
            self.columns = columns
        if not self.columns:
            self.raise_parse_error("TSV data parsing requires columns list.")

    def parse(self, obj: str, **kwargs) -> list[dict]:
        """TSV 문자열을 `csv` 모듈로 파싱하고, `convert_dtypes` 여부에 따라 형변환한다."""
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

    def select_fields(self, data: list[dict], **kwargs) -> list[dict]:
        """TSV 파싱 결과를 그대로 반환한다. (칼럼 선택은 `columns` 속성으로 이미 처리됨)"""
        return data


###################################################################
########################## Master Report ##########################
###################################################################

class Campaign(DuckDBTransformer):
    """네이버 검색광고 캠페인 마스터 데이터를 `searchad_campaign` 테이블에 적재하는 클래스."""

    extractor = "Campaign"
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
        ]
    )


class Adgroup(DuckDBTransformer):
    """네이버 검색광고 광고그룹 마스터 데이터를 `searchad_adgroup` 테이블에 적재하는 클래스."""

    extractor = "Adgroup"
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
            ("Mobile network bidding weight", "INTEGER"),
            ("Business Channel Id(Mobile)", "STRING"),
            ("Business Channel Id(PC)", "STRING"),
            ("regTm", "DATETIME"),
            ("delTm", "DATETIME"),
            ("Content Type", "STRING"),
            ("Ad group type", "INTEGER"),
        ]
    )


class Ad(TsvTransformer):
    """네이버 검색광고 파워링크 단일형 소재 마스터 데이터를 JSON 형식으로 파싱하는 클래스."""

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


class ContentsAd(TsvTransformer):
    """네이버 검색광고 파워컨텐츠 소재 마스터 데이터를 JSON 형식으로 파싱하는 클래스."""

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


class ShoppingProduct(TsvTransformer):
    """네이버 검색광고 쇼핑검색 쇼핑몰상품형 상품 마스터 데이터를 JSON 형식으로 파싱하는 클래스."""

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
    """네이버 검색광고 쇼핑검색 쇼핑브랜드형 상품그룹 마스터 데이터를 JSON 형식으로 파싱하는 클래스."""

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
    """네이버 검색광고 쇼핑검색 쇼핑브랜드형 상품그룹관계 마스터 데이터를 JSON 형식으로 파싱하는 클래스."""

    columns = [
        ("Customer ID", "INTEGER"),
        ("Product Group Relation ID", "STRING"),
        ("Product Group ID", "STRING"),
        ("AD group ID", "STRING"),
        ("regTm", "DATETIME"),
        ("delTm", "DATETIME"),
    ]


class BrandAd(TsvTransformer):
    """네이버 검색광고 쇼핑검색 쇼핑브랜드형 소재 마스터 데이터를 JSON 형식으로 파싱하는 클래스."""

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


class BrandThumbnailAd(TsvTransformer):
    """네이버 검색광고 쇼핑검색 쇼핑브랜드형 썸네일소재 마스터 데이터를 JSON 형식으로 파싱하는 클래스."""

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
    """네이버 검색광고 쇼핑검색 쇼핑브랜드형 배너소재 마스터 데이터를 JSON 형식으로 파싱하는 클래스."""

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


AD_TABLE_KEYS: list[TableKey] = [
    "link_ad",
    "contents_ad",
    "shopping_product",
    "brand_ad",
    # 위 4개 테이블은 `ad` 테이블을 구성하기 위한 부분
    "product_group",
    "product_group_rel",
    "brand_thumbnail_ad",
    "brand_banner_ad",
    # 위 4개 테이블은 `brand_ad` 테이블을 구성하기 위한 부분
]

class MasterAd(DuckDBTransformer):
    """모든 소재 유형의 네이버 검색광고 마스터 데이터를 파싱하고,
    통합된 소재 목록을 `searchad_ad` 테이블에 적재하는 클래스.

    테이블 키 | 테이블명 | 설명
    - `table` | `searchad_ad` | 모든 유형의 소재 목록
    - `link_ad` | `link_ad` | 파워링크 소재 목록
    - `contents_ad` | `contents_ad` | 파워컨텐츠 소재 목록
    - `shopping_product` | `shopping_product` | 쇼핑상품 소재 목록
    - `brand_ad` | `brand_ad` | 모든 유형의 쇼핑브랜드 소재 목록
    - `product_group` | `product_group` | 쇼핑브랜드 상품 그룹
    - `product_group_rel` | `product_group_rel` | 쇼핑브랜드 상품그룹-광고그룹 관계
    - `brand_thumbnail_ad` | `brand_thumbnail_ad` | 쇼핑브랜드 썸네일 이미지형 소재 목록
    - `brand_banner_ad` | `brand_banner_ad` | 쇼핑브랜드 배너 이미지형 소재 목록"""

    extractor = "MasterAd"
    queries = (
        ["create"]
        + [f"bulk_insert_{key}" for key in AD_TABLE_KEYS]
        + [f"transform_{key}" for key in AD_TABLE_KEYS[:4]]
    )
    tables = {"table": "searchad_ad", **{key: key for key in AD_TABLE_KEYS}}
    # params = {"customer_id": "$customer_id"}

    def transform(self, obj: dict[str, str], customer_id: int, **kwargs) -> dict:
        """`{report_type: tsv_string}` 인자로부터
        각각의 `report_type`에 맞는 파서로 TSV 형식의 마스터 데이터를 변환하고,   
        개별 테이블 및 `searchad_ad` 테이블에 적재한다."""
        result_set = dict()
        table_parser = self.table_parser

        for report_type, tsv_data in obj.items():
            table_key, parser = table_parser[report_type]
            result = parser().transform(tsv_data)
            query_key = f"bulk_insert_{table_key}"
            result_set[query_key] = self.bulk_insert(result, query_key, render=self.get_table(table_key))

        for table_key in AD_TABLE_KEYS[:4]:
            query_key = f"transform_{table_key}"
            params = {"customer_id": customer_id}
            result_set[query_key] = self.insert_into(query_key, render=self.tables, params=params)

        return result_set

    def get_table(self, table_key: TableKey) -> dict[TableKey, TableName]:
        """`transform` 쿼리의 렌더 컨텍스트에 추가될 소스 테이블 명칭을 반환한다."""
        return {table_key: self.tables[table_key]}

    @property
    def table_parser(self) -> dict[str, tuple[TableKey, type[TsvTransformer]]]:
        """`{report_type: (table_key, parser)}` 객체를 반환한다."""
        return {
            "Ad":
                ("link_ad", Ad),
            "ContentsAd":
                ("contents_ad", ContentsAd),
            "ShoppingProduct":
                ("shopping_product", ShoppingProduct),
            "ProductGroup":
                ("product_group", ProductGroup),
            "ProductGroupRel":
                ("product_group_rel", ProductGroupRel),
            "BrandAd":
                ("brand_ad", BrandAd),
            "BrandThumbnailAd":
                ("brand_thumbnail_ad", BrandThumbnailAd),
            "BrandBannerAd":
                ("brand_banner_ad", BrandBannerAd),
        }


class Media(DuckDBTransformer):
    """네이버 검색광고 광고매체 마스터 데이터를 다운로드하는 클래스."""

    extractor = "Media"
    tables = {"table": "searchad_media"}
    parser = TsvTransformer
    parser_config = dict(
        columns = [
            ("Type", "STRING"),
            ("ID", "INTEGER"),
            ("Media name", "STRING"),
            ("URL", "STRING"),
            ("NAVER Ad Networks", "BOOLEAN"),
            ("Portal Site", "BOOLEAN"),
            ("PC Media", "BOOLEAN"),
            ("Mobile Media", "BOOLEAN"),
            ("Search Ad Networks", "BOOLEAN"),
            ("Contents Ad Networks", "BOOLEAN"),
            ("Media Group ID", "INTEGER"),
            ("Date of conclusion of a contract", "DATETIME"),
            ("Date of revocation of a contract", "DATETIME"),
        ]
    )
    params = {"root_only": True}


###################################################################
########################### Stat Report ###########################
###################################################################

class AdStat(TsvTransformer):
    """네이버 검색광고 광고성과 보고서로부터 일별 소재 광고 성과를 추출하는 파서 클래스."""

    columns = [
        ("Date", "DATE"),
        ("CUSTOMER ID", "INTEGER"),
        ("Campaign ID", "STRING"),
        ("AD Group ID", "STRING"),
        ("AD Keyword ID", "STRING"),
        ("AD ID", "STRING"),
        ("Business Channel ID", "STRING"),
        ("Media Code", "INTEGER"),
        ("PC Mobile Type", "STRING"),
        ("Impression", "INTEGER"),
        ("Click", "INTEGER"),
        ("Cost", "INTEGER"),
        ("Sum of AD rank", "INTEGER"),
        ("View count", "INTEGER"),
    ]


class AdConversion(TsvTransformer):
    """네이버 검색광고 전환 보고서로부터 일별 소재 전환 성과를 추출하는 파서 클래스."""

    columns = [
        ("Date", "DATE"),
        ("CUSTOMER ID", "INTEGER"),
        ("Campaign ID", "STRING"),
        ("AD Group ID", "STRING"),
        ("AD Keyword ID", "STRING"),
        ("AD ID", "STRING"),
        ("Business Channel ID", "STRING"),
        ("Media Code", "INTEGER"),
        ("PC Mobile Type", "STRING"),
        ("Conversion Method", "INTEGER"),
        ("Conversion Type", "STRING"),
        ("Conversion count", "INTEGER"),
        ("Sales by conversion", "INTEGER"),
    ]


class AdvancedReport(DuckDBTransformer):
    """일별 광고성과 및 전환 보고서를 파싱하고,
    통합된 다차원 보고서를 `searchad_report` 테이블에 적재하는 클래스.

    테이블 키 | 테이블명 | 설명
    - `table` | `searchad_report` | 일별 다차원 보고서
    - `ad_stat` | `ad_stat_report` | 일별 소재 광고 성과
    - `ad_conv` | `ad_conv_report` | 일별 소재 전환 성과"""

    extractor = "AdvancedReport"
    queries = ["create", "bulk_insert_ad_stat", "bulk_insert_ad_conv", "merge_insert"]
    tables = {"table": "searchad_report", "ad_stat": "ad_stat_report", "ad_conv": "ad_conv_report"}
    # params = {"customer_id": "$customer_id", "report_date": "$date"}

    def transform(self, obj: dict[str, str], customer_id: int, date: dt.date, **kwargs) -> dict:
        """`{report_type: tsv_string}` 인자로부터
        각각의 `report_type`에 맞는 파서로 TSV 형식의 마스터 데이터를 변환하고,   
        개별 테이블 및 `searchad_report` 테이블에 적재한다."""
        result_set = dict()
        table_parser = self.table_parser

        for report_type, tsv_data in obj.items():
            table_key, parser = table_parser[report_type]
            result = (parser().transform(tsv_data) or list())

            query_key = f"bulk_insert_{table_key}"
            result_set[query_key] = self.bulk_insert(result, query_key, render=self.get_table(table_key))

        query_key = "merge_insert"
        params = {"customer_id": customer_id, "report_date": date}
        result_set[query_key] = self.insert_into(query_key, render=self.tables, params=params)

        return result_set

    def get_table(self, table_key: TableKey) -> dict[TableKey, TableName]:
        """`transform` 쿼리의 렌더 컨텍스트에 추가될 소스 테이블 명칭을 반환한다."""
        return {table_key: self.tables[table_key]}

    @property
    def table_parser(self) -> dict[str, tuple[TableKey, type[TsvTransformer]]]:
        """`{report_type: (table_key, parser)}` 객체를 반환한다."""
        return {
            "AD":
                ("ad_stat", AdStat),
            "AD_CONVERSION":
                ("ad_conv", AdConversion),
        }
