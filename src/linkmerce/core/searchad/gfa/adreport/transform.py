from __future__ import annotations

from linkmerce.common.transform import ExcelTransformer, DuckDBTransformer


class Campaign(DuckDBTransformer):
    """네이버 성과형 디스플레이 광고 캠페인 목록을 변환 및 적재하는 클래스.

    - **Extractor**: `Campaign`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: searchad_campaign_gfa`
    """

    extractor = "Campaign"
    tables = {"table": "searchad_campaign_gfa"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "content",
        fields = ["no", "name", "objective", "adAccountNo", "activated", "deleted"],
    )


class AdSet(DuckDBTransformer):
    """네이버 성과형 디스플레이 광고 그룹 목록을 변환 및 적재하는 클래스.

    - **Extractor**: `AdSet`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: searchad_adset_gfa`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    account_no: int | str
        성과형 디스플레이 광고 계정 번호
    """

    extractor = "AdSet"
    tables = {"table": "searchad_adset_gfa"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "content",
        fields = ["no", "campaignNo", "name", "bidGoal", "activated", "status", "bidPrice"],
    )
    params = {"account_no": "$account_no"}


class Creative(DuckDBTransformer):
    """네이버 성과형 디스플레이 광고 소재 목록을 변환 및 적재하는 클래스.

    - **Extractor**: `Creative`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: searchad_creative_gfa`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    account_no: int | str
        성과형 디스플레이 광고 계정 번호
    """

    extractor = "Creative"
    tables = {"table": "searchad_creative_gfa"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "content",
        fields = [
            "no", "realCreativeNo", "adSetNo", "creativeType", "name", {"message": None},
            {"medias.1.content.linkUrl": None}, "activated", "status"
        ],
    )
    params = {"account_no": "$account_no"}


class ZipCsvTransformer(ExcelTransformer):
    """네이버 성과형 디스플레이 광고 성과 보고서 다운로드 결과를 파싱하는 클래스."""

    header = 1

    def parse(self, obj: bytes, **kwargs) -> list[dict]:
        """ZIP 압축 파일에서 CSV를 추출한 후 UTF-8 BOM 인코딩으로 읽어서 반환한다."""
        from linkmerce.utils.excel import csv2json
        return csv2json(self.unzip(obj), header=self.header, encoding="utf-8-sig")

    def unzip(self, obj: bytes) -> bytes:
        """ZIP 압축 파일에서 첫 번째 CSV 파일을 추출한다."""
        from io import BytesIO
        import zipfile
        with zipfile.ZipFile(BytesIO(obj)) as zf:
            for name in zf.namelist():
                if name.endswith(".csv"):
                    return zf.read(name)
        self.raise_parse_error("No CSV file found in the compressed file.")


class CampaignReport(DuckDBTransformer):
    """네이버 성과형 디스플레이 광고 캠페인 성과 보고서를 변환 및 적재하는 클래스.

    - **Extractor**: `CampaignReport`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `ZipCsvTransformer: bytes -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: searchad_campaign_report`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    account_no: int | str
        성과형 디스플레이 광고 계정 번호
    """

    extractor = "CampaignReport"
    tables = {"table": "searchad_campaign_report"}
    parser = ZipCsvTransformer
    parser_config = dict(
        fields = ["캠페인 ID", "노출수", "클릭수", "총비용", "총 전환수", "총 전환매출액", "기간"],
    )
    params = {"account_no": "$account_no"}


class CreativeReport(DuckDBTransformer):
    """네이버 성과형 디스플레이 광고 소재 성과 보고서를 변환 및 적재하는 클래스.

    - **Extractor**: `CreativeReport`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `ZipCsvTransformer: bytes -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: searchad_creative_report`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    account_no: int | str
        성과형 디스플레이 광고 계정 번호
    """

    extractor = "CreativeReport"
    tables = {"table": "searchad_creative_report"}
    parser = ZipCsvTransformer
    parser_config = dict(
        fields = [
            "캠페인 ID", "광고 그룹 ID", "광고 소재 ID", "노출수", "클릭수",
            "총비용", "총 전환수", "총 전환매출액", "기간"
        ],
    )
    params = {"account_no": "$account_no"}
