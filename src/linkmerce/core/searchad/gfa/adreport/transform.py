from __future__ import annotations

from linkmerce.common.transform import ExcelTransformer, DuckDBTransformer


class Campaign(DuckDBTransformer):
    tables = {"table": "searchad_campaign_gfa"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "content",
        fields = ["no", "name", "objective", "adAccountNo", "activated", "deleted"],
    )


class AdSet(DuckDBTransformer):
    tables = {"table": "searchad_adgroup_gfa"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "content",
        fields = ["no", "campaignNo", "name", "bidGoal", "activated", "status", "bidPrice"],
        defaults = {"accountNo": "$account_no"},
    )


class Creative(DuckDBTransformer):
    tables = {"table": "searchad_creative_gfa"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "content",
        fields = [
            "realCreativeNo", "adSetNo", "creativeType", "name", {"message": None},
            {"medias.1.content.linkUrl": None}, "activated", "status"
        ],
        defaults = {"accountNo": "$account_no"},
    )


class CsvTransformer(ExcelTransformer):
    header = 1

    def parse(self, obj: bytes, **kwargs) -> list[dict]:
        from linkmerce.utils.excel import csv2json
        return csv2json(self.unzip(obj), header=self.header, encoding="utf-8-sig")

    def unzip(self, obj: bytes) -> bytes:
        from io import BytesIO
        import zipfile
        with zipfile.ZipFile(BytesIO(obj)) as zf:
            for name in zf.namelist():
                if name.endswith(".csv"):
                    return zf.read(name)
        self.raise_parse_error("No CSV file found in the compressed file.")


class CampaignReport(DuckDBTransformer):
    tables = {"table": "searchad_campaign_report"}
    parser = CsvTransformer
    parser_config = dict(
        fields = ["캠페인 ID", "노출", "클릭", "총 비용", "총 전환수", "총 전환 매출액", "기간"],
        defaults = {"accountNo": "$account_no"},
    )


class CreativeReport(DuckDBTransformer):
    tables = {"table": "searchad_creative_report"}
    parser = CsvTransformer
    parser_config = dict(
        fields = [
            "캠페인 ID", "광고 그룹 ID", "광고 소재 ID", "노출", "클릭", "도달",
            "총 비용", "총 전환수", "총 전환 매출액", "기간"
        ],
        defaults = {"accountNo": "$account_no"},
    )
