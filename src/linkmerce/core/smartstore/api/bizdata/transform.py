from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject
    import datetime as dt


class MarketingChannelList(JsonTransformer):
    dtype = dict
    path = ["rows"]


class MarketingChannel(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, channel_seq: int | str, date: dt.date, **kwargs):
        channels = MarketingChannelList().transform(obj)
        if channels:
            self.insert_into_table(channels, params=dict(channel_seq=channel_seq, ymd=date))
