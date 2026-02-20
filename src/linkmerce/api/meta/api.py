from __future__ import annotations

from linkmerce.common.api import run_with_duckdb, update_options

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


def get_module(name: str) -> str:
    return (".meta.api" + name) if name.startswith('.') else name


def get_options(
        request_delay: float | int = 1,
        progress: bool = True,
    ) -> dict:
    return dict(
        RequestEach = dict(request_delay=request_delay, tqdm_options=dict(disable=(not progress))),
    )


def _ad_objects(
        access_token: str,
        object_type: Literal["Campaigns","Adsets","Ads"],
        app_id: str = str(),
        app_secret: str = str(),
        start_date: dt.date | str | None = None,
        end_date: dt.date | str | None = None,
        account_ids: Sequence[str] = list(),
        fields: Sequence[str] = list(),
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.meta.api.ads.extract import _AdObjects
    # from linkmerce.core.meta.api.ads.transform import _AdTransformer
    return run_with_duckdb(
        module = get_module(".ads"),
        extractor = object_type,
        transformer = object_type,
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (start_date, end_date, account_ids, fields),
        extract_options = update_options(
            extract_options,
            variables = dict(access_token=access_token, app_id=app_id, app_secret=app_secret),
            options = get_options(request_delay, progress),
        ),
        transform_options = transform_options,
    )


def campaigns(
        access_token: str,
        app_id: str = str(),
        app_secret: str = str(),
        start_date: dt.date | str | None = None,
        end_date: dt.date | str | None = None,
        account_ids: Sequence[str] = list(),
        fields: Sequence[str] = list(),
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.meta.api.ads.extract import Campaigns
    # from linkmerce.core.meta.api.ads.transform import Campaigns
    return _ad_objects(
        access_token, "Campaigns", app_id, app_secret, start_date, end_date, account_ids, fields,
        connection, tables, request_delay, progress, return_type, extract_options, transform_options)


def adsets(
        access_token: str,
        app_id: str = str(),
        app_secret: str = str(),
        start_date: dt.date | str | None = None,
        end_date: dt.date | str | None = None,
        account_ids: Sequence[str] = list(),
        fields: Sequence[str] = list(),
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.meta.api.ads.extract import Adsets
    # from linkmerce.core.meta.api.ads.transform import Adsets
    return _ad_objects(
        access_token, "Adsets", app_id, app_secret, start_date, end_date, account_ids, fields,
        connection, tables, request_delay, progress, return_type, extract_options, transform_options)


def ads(
        access_token: str,
        app_id: str = str(),
        app_secret: str = str(),
        start_date: dt.date | str | None = None,
        end_date: dt.date | str | None = None,
        account_ids: Sequence[str] = list(),
        fields: Sequence[str] = list(),
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.meta.api.ads.extract import Ads
    # from linkmerce.core.meta.api.ads.transform import Ads
    return _ad_objects(
        access_token, "Ads", app_id, app_secret, start_date, end_date, account_ids, fields,
        connection, tables, request_delay, progress, return_type, extract_options, transform_options)


def insights(
        access_token: str,
        ad_level: Literal["campaign","adset","ad"],
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["daily","total"] = "daily",
        app_id: str = str(),
        app_secret: str = str(),
        account_ids: Sequence[str] = list(),
        fields: Sequence[str] = list(),
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """```python
    tables = {
        'campaigns': 'meta_campaigns',
        'adsets': 'meta_adsets',
        'ads': 'meta_ads',
        'metrics': 'meta_insights'
    }"""
    # from linkmerce.core.meta.api.ads.extract import Insights
    # from linkmerce.core.meta.api.ads.transform import Insights
    return run_with_duckdb(
        module = get_module(".ads"),
        extractor = "Insights",
        transformer = "Insights",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (ad_level, start_date, end_date, date_type, account_ids, fields),
        extract_options = update_options(
            extract_options,
            variables = dict(access_token=access_token, app_id=app_id, app_secret=app_secret),
            options = get_options(request_delay, progress),
        ),
        transform_options = transform_options,
    )
