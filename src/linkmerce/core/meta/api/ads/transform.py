from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


def _common_config(fields: list) -> dict:
    return dict(
        dtype = dict,
        scope = "data",
        fields = fields,
    )


class Campaigns(DuckDBTransformer):
    """메타 광고 캠페인 보고서를 변환 및 적재하는 클래스.

    - **Extractor**: `Campaigns`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: meta_campaigns`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    account_id: str
        메타 광고 계정 ID
    """

    extractor = "Campaigns"
    tables = {"table": "meta_campaigns"}
    parser = "json"
    parser_config = _common_config(
        fields = ["id", "name", "objective", "effective_status", "created_time"],
    )
    params = {"account_id": "$account_id"}


class Adsets(DuckDBTransformer):
    """메타 광고세트 보고서를 변환 및 적재하는 클래스.

    - **Extractor**: `Adsets`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: meta_adsets`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    account_id: str
        메타 광고 계정 ID
    """

    extractor = "Adsets"
    tables = {"table": "meta_adsets"}
    parser = "json"
    parser_config = _common_config(
        fields = ["id", "name", "campaign_id", "effective_status", {"daily_budget": None}, "created_time"],
    )
    params = {"account_id": "$account_id"}


class Ads(DuckDBTransformer):
    """메타 광고 보고서를 변환 및 적재하는 클래스.

    - **Extractor**: `Ads`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: meta_ads`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    account_id: str
        메타 광고 계정 ID
    """

    extractor = "Ads"
    tables = {"table": "meta_ads"}
    parser = "json"
    parser_config = _common_config(
        fields = ["id", "name", "campaign_id", "adset_id", "effective_status", "created_time"],
    )
    params = {"account_id": "$account_id"}


class Insights(DuckDBTransformer):
    """메타 광고 성과 보고서를 변환 및 적재하는 클래스.

    - **Extractor**: `Insights`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Tables** ( *table_key: table_name (description)* ):
        1. `campaigns: meta_campaigns` (캠페인 보고서)
        2. `adsets: meta_adsets` (광고세트 보고서)
        3. `ads: meta_ads` (광고 보고서)
        4. `insights: meta_insights` (성과 보고서)

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    account_id: str
        메타 광고 계정 ID
    """

    extractor = "Insights"
    tables = {"campaigns": "meta_campaigns", "adsets": "meta_adsets", "ads": "meta_ads", "insights": "meta_insights"}
    parser = "json"
    parser_config = _common_config(
        fields = [
            "campaign_id", "campaign_name", "adset_id", "adset_name", "ad_id", "ad_name",
            "impressions", "reach", "clicks", "inline_link_clicks", "spend", "date_start"
        ],
    )
    params = {"account_id": "$account_id"}
