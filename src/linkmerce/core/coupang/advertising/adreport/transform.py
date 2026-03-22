from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


class CampaignParser(JsonTransformer):
    """쿠팡 광고 캠페인 목록을 추출하는 파서 클래스."""

    dtype = dict
    scope = "campaigns"
    fields = [
        "id", "name", "campaignType", "vendorType", "goalType", "isActive", "isDeleted",
        {"roasTarget": None}, "capType", "calculatedBudget", "spentBudget", "createdAt", "updatedAt"
    ]


class AdgroupParser(JsonTransformer):
    """쿠팡 광고그룹 목록을 추출하는 파서 클래스."""

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
    """쿠팡 광고 캠페인 및 광고그룹 목록을 각각의 테이블에 변환 및 적재하는 클래스.

    테이블 키 | 테이블명 | 설명
    - `campaign` | `coupang_campaign` | 쿠팡 광고 캠페인 목록
    - `adgroup` | `coupang_adgroup` | 쿠팡 광고그룹 목록"""

    extractor = "Campaign"
    tables = {"campaign": "coupang_campaign", "adgroup": "coupang_adgroup"}
    parser = {"campaign": CampaignParser, "adgroup": AdgroupParser}
    params = {"vendor_id": "$vendor_id"}


class Creative(DuckDBTransformer):
    """쿠팡 광고 동영상 소재 데이터를 `coupang_creative` 테이블에 적재하는 클래스."""

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
    """쿠팡 PA(Product Ad) 광고 성과 보고서(Excel) 데이터를 `coupang_adreport_pa` 테이블에 적재하는 클래스."""

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
    """쿠팡 신규고객광고(NCA) 성과 보고서(Excel) 데이터를 `coupang_adreport_nca` 테이블에 적재하는 클래스."""

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
