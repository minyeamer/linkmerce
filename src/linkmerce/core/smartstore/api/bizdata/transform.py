from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class MarketingChannel(DuckDBTransformer):
    tables = {"table": "marketing_channel"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "rows",
        fields = [
            "deviceCategory", "ntSource", "ntMedium", "ntDetail", "ntKeyword",
            "numUsers", "numInteractions", "pv", "numPurchases", "payAmount",
        ],
        defaults = {"channelSeq": "$channel_seq", "ymd": "$date"},
    )
