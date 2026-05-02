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
    """네이버 검색광고 대용량 다운로드 보고서를 파싱하는 클래스.

    **NOTE** `ResponseTransformer`의 5단계 파이프라인을 따른다.

    TSV 형식의 보고서에 대한 열이름과 설명은 대용량 다운로드 보고서 데이터 정의서 PDF를 참고한다.
    - https://searchad.naver.com/File/downloadfilen/?type=10&filename=masterreport808.pdf

    Attributes
    ----------
    columns: list[tuple[str, str]]
        보고서에서 각각의 열과 순서대로 대응되는 튜플의 리스트. 튜플은 아래 항목으로 구성된다.
            1. **열이름**: 보고서에는 헤더가 없다. PDF를 참고하여 열이름을 옮겨 적는다.
            2. **데이터형**: 보고서의 값은 기본적으로 문자열로 인식된다.   
                다른 타입으로 인식되어야 한다면 `_cast` 함수에서 정의된 `type` 상수를 입력한다.
    convert_dtypes: bool
        데이터형 변환 여부. 기본값은 `True`
    """

    columns: list[tuple[Column, ColumnType]] = list()
    convert_dtypes: bool = True

    def pre_init(self, columns: list[tuple] | None = None, convert_dtypes: bool | None = None, **kwargs):
        self.set_columns(columns)
        if convert_dtypes is not None:
            self.convert_dtypes = convert_dtypes

    def set_columns(self, columns: list[tuple] | None = None):
        """열 목록을 설정하고, 열 목록이 없으면 `ParseError`를 발생시킨다."""
        if columns is not None:
            self.columns = columns
        if not self.columns:
            self.raise_parse_error("TSV data parsing requires columns list.")

    def parse(self, obj: str, **kwargs) -> list[dict]:
        """TSV 문자열을 `csv` 모듈로 파싱하고, `convert_dtypes` 여부에 따라 형변환한다."""
        data = list()

        if obj:
            columns, types = zip(*self.columns)
            functions = [_cast(dtype) if self.convert_dtypes else (lambda x: x) for dtype in types]
            for row in obj.split('\n'):
                if row:
                    data.append({column: cast(value) for column, cast, value in zip(columns, functions, row.split('\t'))})
        return data

    def select_fields(self, data: list[dict], **kwargs) -> list[dict]:
        """TSV 파싱 결과를 그대로 반환한다. (열 선택은 `columns` 속성으로 이미 처리됨)"""
        return data


###################################################################
########################## Master Report ##########################
###################################################################

class Campaign(DuckDBTransformer):
    """네이버 검색광고 캠페인 마스터 데이터를 변환 및 적재하는 클래스.

    - **Extractor**: `Campaign`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `TsvTransformer: str -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: searchad_campaign`
    """

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
    """네이버 검색광고 광고그룹 마스터 데이터를 변환 및 적재하는 클래스.

    - **Extractor**: `Adgroup`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `TsvTransformer: str -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: searchad_adgroup`
    """

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
    """네이버 검색광고 소재 마스터 데이터를 파싱하는 클래스."""

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
    """네이버 검색광고 파워컨텐츠 소재 마스터 데이터를 파싱하는 클래스."""

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
    """네이버 검색광고 쇼핑검색 쇼핑몰상품형 상품 마스터 데이터를 파싱하는 클래스."""

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
    """네이버 검색광고 쇼핑검색 쇼핑브랜드형 상품그룹 마스터 데이터를 파싱하는 클래스."""

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
    """네이버 검색광고 쇼핑검색 쇼핑브랜드형 상품그룹관계 마스터 데이터를 파싱하는 클래스."""

    columns = [
        ("Customer ID", "INTEGER"),
        ("Product Group Relation ID", "STRING"),
        ("Product Group ID", "STRING"),
        ("AD group ID", "STRING"),
        ("regTm", "DATETIME"),
        ("delTm", "DATETIME"),
    ]


class BrandAd(TsvTransformer):
    """네이버 검색광고 쇼핑검색 쇼핑브랜드형 소재 마스터 데이터를 파싱하는 클래스."""

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
    """네이버 검색광고 쇼핑검색 쇼핑브랜드형 썸네일소재 마스터 데이터를 파싱하는 클래스."""

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
    """네이버 검색광고 쇼핑검색 쇼핑브랜드형 배너소재 마스터 데이터를 파싱하는 클래스."""

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
    """모든 소재 유형의 네이버 검색광고 마스터 데이터를 변환 및 적재하는 클래스.

    - **Extractor**: `MasterAd`

    - **Parsers** ( *parser_class: input_type -> output_type* ):
        1. `Ad: str -> list[dict]`
        2. `ContentsAd: str -> list[dict]`
        3. `ShoppingProduct: str -> list[dict]`
        4. `ProductGroup: str -> list[dict]`
        5. `ProductGroupRel: str -> list[dict]`
        6. `BrandAd: str -> list[dict]`
        7. `BrandThumbnailAd: str -> list[dict]`
        8. `BrandBannerAd: str -> list[dict]`

    - **Tables** ( *table_key: table_name (description)* ):
        1. `table: searchad_ad` (전체 소재 목록)
        2. `link_ad: link_ad` (파워링크 소재 목록)
        3. `contents_ad: contents_ad` (파워컨텐츠 소재 목록)
        4. `shopping_product: shopping_product` (쇼핑상품 소재 목록)
        5. `brand_ad: brand_ad` (쇼핑브랜드 소재 목록)
        6. `product_group: product_group` (상품그룹 목록)
        7. `product_group_rel: product_group_rel` (상품그룹-광고그룹 관계)
        8. `brand_thumbnail_ad: brand_thumbnail_ad` (썸네일 이미지형 소재 목록)
        9. `brand_banner_ad: brand_banner_ad` (배너 이미지형 소재 목록)

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    customer_id: int
        광고계정의 CUSTOMER_ID
    """

    extractor = "MasterAd"
    queries = (
        ["create"]
        + [f"bulk_insert_{key}" for key in AD_TABLE_KEYS]
        + [f"transform_{key}" for key in AD_TABLE_KEYS[:4]]
    )
    tables = {"table": "searchad_ad", **{key: key for key in AD_TABLE_KEYS}}
    # params = {"customer_id": "$customer_id"}

    def transform(self, obj: dict[str, str], customer_id: int, **kwargs) -> dict:
        """모든 소재 유형의 마스터 보고서를 파싱하여 하나의 테이블로 병합한다.

        Parameters
        ----------
        obj: dict[str, str]
            `{보고서 유형: TSV 텍스트}` 구조의 보고서 다운로드 결과
        customer_id: int
            광고계정의 CUSTOMER_ID
        """
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
    """네이버 검색광고 광고매체 마스터 데이터를 변환 및 적재하는 클래스.

    - **Extractor**: `Media`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `TsvTransformer: str -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: searchad_media`
    """

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
    """네이버 검색광고 광고성과 보고서를 파싱하는 클래스."""

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
    """네이버 검색광고 전환 보고서를 파싱하는 클래스."""

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
    """다차원 보고서의 바탕이 되는 광고성과 및 전환 보고서를 변환 및 적재하는 클래스.

    - **Extractor**: `AdvancedReport`

    - **Parsers** ( *parser_class: input_type -> output_type* ):
        1. `AdStat: str -> list[dict]`
        2. `AdConversion: str -> list[dict]`

    - **Tables** ( *table_key: table_name (description)* ):
        1. `table: searchad_report` (다차원 보고서)
        2. `ad_stat: ad_stat_report` (광고성과 보고서)
        3. `ad_conv: ad_conv_report` (전환 보고서)

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    customer_id: int
        광고계정의 CUSTOMER_ID
    date: dt.date
        조회 일자
    """

    extractor = "AdvancedReport"
    queries = ["create", "bulk_insert_ad_stat", "bulk_insert_ad_conv", "merge_insert"]
    tables = {"table": "searchad_report", "ad_stat": "ad_stat_report", "ad_conv": "ad_conv_report"}
    # params = {"customer_id": "$customer_id", "report_date": "$date"}

    def transform(self, obj: dict[str, str], customer_id: int, date: dt.date, **kwargs) -> dict:
        """광고성과 및 전환 보고서를 파싱하여 하나의 테이블로 병합한다.

        Parameters
        ----------
        obj: dict[str, str]
            `{보고서 유형: TSV 텍스트}` 구조의 보고서 다운로드 결과
        customer_id: int
            광고계정의 CUSTOMER_ID
        date: dt.date
            조회 일자
        """
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
