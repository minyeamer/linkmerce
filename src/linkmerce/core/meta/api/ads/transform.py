from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.transform import JsonObject


class AdObjects(JsonTransformer):
    dtype = dict
    path = ["data"]


class _AdTransformer(DuckDBTransformer):
    queries: list[str] = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, account_id: str, **kwargs):
        objects = AdObjects().transform(obj)
        if objects:
            self.insert_into_table(objects, params=dict(account_id=account_id))


class Campaigns(_AdTransformer):
    queries = ["create", "select", "insert"]


class Adsets(_AdTransformer):
    queries = ["create", "select", "insert"]


class Ads(_AdTransformer):
    queries = ["create", "select", "insert"]


INSIGHTS_TABLES = ["campaigns", "adsets", "ads", "metrics"]

class Insights(_AdTransformer):
    queries = [f"{keyword}_{table}"
        for table in INSIGHTS_TABLES
            for keyword in ["create", "select", "insert"]]

    def set_tables(self, tables: dict | None = None):
        base = {table: f'meta_{"insights" if table == "metrics" else table}' for table in INSIGHTS_TABLES}
        super().set_tables(dict(base, **(tables or dict())))

    def create_table(self, **kwargs):
        for table in INSIGHTS_TABLES:
            super().create_table(key=f"create_{table}", table=f":{table}:")

    def transform(self, obj: JsonObject, account_id: str, **kwargs):
        insights = AdObjects().transform(obj)
        if insights:
            for table in INSIGHTS_TABLES:
                self.insert_into_table(insights,
                    key = f"insert_{table}",
                    table = f":{table}:",
                    values = f":select_{table}:",
                    params = dict(account_id=account_id),
                )
