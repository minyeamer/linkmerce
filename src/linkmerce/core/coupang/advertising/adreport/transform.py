from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


class CampaignParser(JsonTransformer):
    """쿠팡 광고센터 캠페인 목록을 파싱하는 클래스."""

    dtype = dict
    scope = "campaigns"
    fields = [
        "id", "name", "campaignType", "vendorType", "goalType", "isActive", "isDeleted",
        *[{key: None} for key in ["roasTarget", "capType", "calculatedBudget", "spentBudget"]],
        "createdAt", "updatedAt"
    ]


class AdgroupParser(JsonTransformer):
    """쿠팡 광고센터 캠페인 목록에서 광고그룹 목록을 파싱하는 클래스."""

    dtype = dict
    scope = "campaigns"
    fields = [
        "id", {"campaignId": None}, "paCampaignId", "name", "goalType", "isActive", "isDeleted",
        {"roasTarget": None}, "createdAt", "updatedAt"
    ]

    def parse(self, obj: list, **kwargs) -> list[dict]:
        """캠페인 목록에서 `groupList`를 평탄화해 광고그룹 목록을 반환한다."""
        adgroups, goal_type = list(), kwargs.get("goal_type")
        for campaign in obj:
            if not isinstance(campaign, dict):
                continue
            for adgroup in (campaign.get("groupList") or list()):
                if isinstance(adgroup, dict):
                    adgroups.append(adgroup | {
                        "goalType": campaign.get("goalType", goal_type),
                        "campaignId": adgroup.get("paCampaignId", campaign.get("id")),
                    })
        return adgroups


class Campaign(DuckDBTransformer):
    """쿠팡 광고센터 캠페인 목록을 변환 및 적재하는 클래스.

    - **Extractor**: `Campaign`

    - **Parsers** ( *parser_class: input_type -> output_type* ):
        1. `CampaignParser: dict -> list[dict]`
        2. `AdgroupParser: dict -> list[dict]`

    - **Tables** ( *table_key: table_name (description)* ):
        1. `campaign: coupang_campaign` (캠페인 목록)
        2. `adgroup: coupang_adgroup` (광고그룹 목록)

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    vendor_id: str
        업체 코드
    """

    extractor = "Campaign"
    tables = {"campaign": "coupang_campaign", "adgroup": "coupang_adgroup"}
    parser = {"campaign": CampaignParser, "adgroup": AdgroupParser}
    params = {"vendor_id": "$vendor_id"}


class Creative(DuckDBTransformer):
    """쿠팡 광고센터 신규 구매 고객 확보(NCA) 캠페인의 소재 정보를 변환 및 적재하는 클래스.

    - **Extractor**: `Creative`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: coupang_creative`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    vendor_id: str
        업체 코드
    """

    extractor = "Creative"
    tables = {"table": "coupang_creative"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "adGroup.videoAds",
        fields = ["id", "vendorItemId", "creativeType", "headlineText", "description", "imageUrl", "ordering"],
    )
    params = {"vendor_id": "$vendor_id"}


class ProductAdReport(DuckDBTransformer):
    """쿠팡 광고센터 매출 성장 광고 보고서를 변환 및 적재하는 클래스.

    - **Extractor**: `ProductAdReport`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `ExcelTransformer: bytes -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: coupang_adreport_pa`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    vendor_id: str
        업체 코드
    """

    extractor = "ProductAdReport"
    tables = {"table": "coupang_adreport_pa"}
    parser = "excel"
    parser_config = dict(
        fields = [
            "캠페인 ID", "광고집행 옵션ID", "광고전환매출발생 옵션ID", "광고 노출 지면",
            "노출수", "클릭수", "광고비", "총 주문수(1일)", "직접 판매수량(1일)",
            "총 전환매출액(1일)", "직접 전환매출액(1일)", "날짜"
        ],
    )
    params = {"vendor_id": "$vendor_id"}


class NewCustomerAdReport(DuckDBTransformer):
    """쿠팡 광고센터 신규 구매 고객 확보 광고 보고서를 변환 및 적재하는 클래스.

    - **Extractor**: `NewCustomerAdReport`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `ExcelTransformer: bytes -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: coupang_adreport_nca`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    vendor_id: str
        업체 코드
    """

    extractor = "NewCustomerAdReport"
    tables = {"table": "coupang_adreport_nca"}
    parser = "excel"
    parser_config = dict(
        fields = [
            "캠페인 ID", "소재 ID", "소재", "광고집행 옵션 ID", "광고 노출 지면",
            "노출수", "클릭수", "집행 광고비", "참여수", "평균 재생 시간", "날짜"
        ],
    )
    params = {"vendor_id": "$vendor_id"}
