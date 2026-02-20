from __future__ import annotations
from linkmerce.core.meta.api import MetaAPI

from typing import TypedDict, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class AdAccount(TypedDict):
    account_status: int
    id: str # act_{ACCOUNT_ID}
    name: str


class MetaAds(MetaAPI):
    method = "GET"

    @property
    def default_options(self) -> dict:
        return dict(RequestEach = dict(request_delay=1))

    def _extract_backend(self, account_ids: Sequence[str] = list(), **partial) -> JsonObject:
        if not account_ids:
            account_ids = [account["id"] for account in self.list_accounts()]
        return (self.request_each(self.request_json_by_account)
                .partial(**partial)
                .expand(account_id=account_ids)
                .run())

    def request_json_by_account(self, account_id: str, **kwargs) -> JsonObject:
        kwargs["url"] = self.concat_path(self.origin, self.version, account_id, self.path)
        return self.request_json_safe(**kwargs)

    def list_accounts(self) -> list[AdAccount]:
        import json
        url = self.concat_path(self.origin, self.version, "/me/adaccounts")
        params = {"access_token": self.access_token, "fields": "id,name"}
        with self.request("GET", url, params=params) as response:
            return json.loads(response.text)["data"]

    def time_range(self, since: dt.date | str, until: dt.date | str) -> str:
        import json
        return json.dumps({"since": str(since), "until": str(until)})


class _AdObjects(MetaAds):

    @MetaAds.with_session
    @MetaAds.auto_refresh_token
    def extract(
            self,
            start_date: dt.date | str | None = None,
            end_date: dt.date | str | None = None,
            account_ids: Sequence[str] = list(),
            fields: Sequence[str] = list(),
            **kwargs
        ) -> JsonObject:
        return self._extract_backend(account_ids, start_date=start_date, end_date=end_date, fields=fields)

    def build_request_params(
            self,
            start_date: dt.date | str | None = None,
            end_date: dt.date | str | None = None,
            fields: Sequence[str] = list(),
            **kwargs
        ) -> dict[str,str]:
        return {
            "access_token": self.access_token,
            "fields": ','.join(fields if fields else self.fields),
            **({"time_range": self.time_range(start_date, end_date)} if start_date and end_date else {}),
        }

    @property
    def fields(self) -> list[str]:
        return list()


class Campaigns(_AdObjects):
    path = "/campaigns"

    @property
    def fields(self) -> list[str]:
        return [
            "id", "name", "objective", "status", "effective_status", "created_time", # "insights",
        ]


class Adsets(_AdObjects):
    path = "/adsets"

    @property
    def fields(self) -> list[str]:
        return [
            "id", "name", "campaign_id", "status", "effective_status", "daily_budget", "created_time",
            # "targeting", "insights",
        ]


class Ads(_AdObjects):
    path = "/ads"

    @property
    def fields(self) -> list[str]:
        return [
            "id", "name", "campaign_id", "adset_id", "status", "effective_status", "creative", "created_time",
            # "configured_status", "source_ad_id", "tracking_specs", "insights",
        ]


class Insights(MetaAds):
    path = "/insights"

    @MetaAds.with_session
    @MetaAds.auto_refresh_token
    def extract(
            self,
            ad_level: Literal["campaign","adset","ad"],
            start_date: dt.date | str,
            end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
            date_type: Literal["daily","total"] = "daily",
            account_ids: Sequence[str] = list(),
            fields: Sequence[str] = list(),
            **kwargs
        ) -> JsonObject:
        dates = dict(start_date=start_date, end_date=(start_date if end_date == ":start_date:" else end_date))
        return self._extract_backend(account_ids, ad_level=ad_level, **dates, date_type=date_type, fields=fields)

    def build_request_params(
            self,
            ad_level: Literal["campaign","adset","ad"],
            start_date: dt.date | str,
            end_date: dt.date | str,
            date_type: Literal["daily","total"] = "daily",
            fields: Sequence[str] = list(),
            **kwargs
        ) -> dict[str,str]:
        return {
            "access_token": self.access_token,
            "fields": ','.join(fields if fields else self.fields),
            "level": ad_level,
            "time_range": self.time_range(start_date, end_date),
            **({"time_increment": 1} if date_type == "daily" else {}),
            "limit": 5000,
        }

    @property
    def fields(self) -> list[str]:
        return [
            "date_start", "date_stop",
            "campaign_id","campaign_name", "adset_id", "adset_name", "ad_id", "ad_name",
            "impressions", "reach", "frequency", "clicks", "inline_link_clicks",
            "spend", # "actions", "action_values",
        ]
