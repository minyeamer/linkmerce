from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject


class CampaignParser(JsonTransformer):
    dtype = dict
    scope = "campaigns"
    fields = [
        "id", "name", "campaignType", "vendorType", "goalType", "isActive", "isDeleted",
        {"roasTarget": None}, "capType", "calculatedBudget", "spentBudget", "createdAt", "updatedAt"
    ]
    defaults = {"vendorId": "$vendor_id"}


class AdgroupParser(JsonTransformer):
    dtype = dict
    scope = "campaigns"
    fields = ["id", "paCampaignId", "name", "isActive", "isDeleted", {"roasTarget": None}, "createdAt", "updatedAt"]
    defaults = {"vendorId": "$vendor_id", "goalType": "$goal_type"}

    def parse(self, campaigns: JsonObject, **kwargs) -> list[dict]:
        adgroups = list()
        for campaign in campaigns:
            if not isinstance(campaign, dict):
                continue
            for adgroup in (campaign.get("groupList") or list()):
                adgroups.append(adgroup)
        return adgroups


class Campaign(DuckDBTransformer):
    tables = {"campaign": "coupang_campaign", "adgroup": "coupang_adgroup"}
    parser = {"campaign": CampaignParser, "adgroup": AdgroupParser}


class Creative(DuckDBTransformer):
    tables = {"table": "coupang_creative"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "adGroup.videoAds",
        fields = ["id", "vendorItemId", "creativeType", "headlineText", "description", "imageUrl", "ordering"],
        defaults = {"vendorId": "$vendor_id"},
    )


class ProductAdReport(DuckDBTransformer):
    tables = {"table": "coupang_adreport_pa"}
    parser = "excel"
    parser_config = dict(
        fields = [
            "캠페인 ID", "광고집행 옵션ID", "광고전환매출발생 옵션ID", "광고 노출 지면",
            "노출수", "클릭수", "광고비", "총 주문수(1일)", "직접 판매수량(1일)",
            "총 전환매출액(1일)", "직접 전환매출액(1일)", "날짜"
        ],
        defaults = {"vendorId": "$vendor_id"},
    )


class NewCustomerAdReport(DuckDBTransformer):
    tables = {"table": "coupang_adreport_nca"}
    parser = "excel"
    parser_config = dict(
        fields = [
            "캠페인 ID", "소재 ID", "소재", "광고집행 옵션 ID", "광고 노출 지면",
            "노출수", "클릭수", "집행 광고비", "참여수", "평균 재생 시간", "날짜"
        ],
        defaults = {"vendorId": "$vendor_id"},
    )
