from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class MarketingChannel(DuckDBTransformer):
    """스마트스토어 상품/마케팅 채널 API 응답 데이터를 `marketing_channel` 테이블에 적재하는 클래스."""

    extractor = "MarketingChannel"
    tables = {"table": "marketing_channel"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "rows",
        fields = [
            "deviceCategory", "ntSource", "ntMedium", "ntDetail", "ntKeyword",
            "numUsers", "numInteractions", "pv", "numPurchases", "payAmount",
        ],
    )
    params = {"channel_seq": "$channel_seq", "ymd": "$date"}
