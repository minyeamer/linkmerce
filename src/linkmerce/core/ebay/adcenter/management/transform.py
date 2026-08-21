from linkmerce.common.transform import DuckDBTransformer
from linkmerce.core.ebay.adcenter import GmarketAdParser


class CampaignGroup(DuckDBTransformer):
    """Gmarket 광고센터 캠페인 그룹 목록을 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `CampaignGroup`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        - `GmarketAdParser: str -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: ebay_campaign_group`
    """

    extractor = "CampaignGroup"
    tables = {"table": "ebay_campaign_group"}
    parser = GmarketAdParser
    parser_config = dict(
        scope = "data.items",
        fields = [
            "campaignGroupId", "campaignGroupName", "campaignGroupType", "campaignGroupStatus",
            # "impressions", "clicks", "spend", "storeOrders", "storeRevenue"
        ]
    )


class Campaign(DuckDBTransformer):
    """Gmarket 광고센터 캠페인 목록을 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `Campaign`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        - `GmarketAdParser: str -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: ebay_campaign`
    """

    extractor = "Campaign"
    tables = {"table": "ebay_campaign"}
    parser = GmarketAdParser
    parser_config = dict(
        scope = "data.items",
        fields = [
            "campaignId", "campaignGroupId", "campaignName", "campaignStatus",
            "dailyBudget", # "impressions", "clicks", "spend", "storeOrders", "storeRevenue"
        ]
    )


class Product(DuckDBTransformer):
    """Gmarket 광고센터 상품 목록을 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `Product`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        - `GmarketAdParser: str -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: ebay_product`
    """

    extractor = "Product"
    tables = {"table": "ebay_product"}
    parser = GmarketAdParser
    parser_config = dict(
        scope = "data.items",
        fields = [
            "campaignId", "adgroupId", "itemId", "itemName", "adgroupStatus", "itemImage",
            "bidPrice", # "impressions", "clicks", "spend", "storeOrders", "storeRevenue"
        ]
    )
