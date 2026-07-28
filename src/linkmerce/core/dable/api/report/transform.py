from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


class DailyReportParser(JsonTransformer):
    """데이블 광고 보고서 조회 결과를 파싱하는 클래스."""

    dtype = dict
    fields = [
        "ymd", {"campaign_id": None}, {"campaign_name": None},
        "exposes", "impressions", "clicks", "cost_spent", "convertion_cnt"
    ]

    def parse(self, report: dict, group_by_campaign: bool = True, **kwargs) -> list[dict]:
        """날짜 키와 집계 결과로 구성된 딕셔너리를 리스트 형식으로 가공해 반환한다."""
        if group_by_campaign:
            return self.parse_by_campaign(report)

        data = list()
        for date, values in report.items():
            if isinstance(values, dict) and values:
                data.append({"ymd": date} | values)
        return data

    def parse_by_campaign(self, report: dict) -> list[dict]:
        """캠페인 단위의 광고 보고서는 날짜 키 하위의 캠페인ID도 평탄화한 리스트 형식으로 가공해 반환한다."""
        data = list()
        for date, campaigns in report.items():
            if isinstance(campaigns, dict) and campaigns:
                for campaign_id, values in campaigns.items():
                    if isinstance(values, dict) and values:
                        data.append({"ymd": date, "campaign_id": campaign_id} | values)
        return data


class DailyReport(DuckDBTransformer):
    """데이블 광고 보고서를 변환 및 적재하는 클래스.

    - **Extractor**: `DailyReport`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Tables** ( *table_key: table_name (description)* ):
        1. `report: dable_report` (광고 보고서)
        2. `campaign: dable_campaign` (캠페인 목록)
    """

    extractor = "DailyReport"
    tables = {"report": "dable_report", "campaign": "dable_campaign"}
    parser = DailyReportParser
