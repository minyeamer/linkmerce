from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


def _get_api_configs(
        access_token: str,
        app_id: str = str(),
        app_secret: str = str(),
    ) -> dict:
    """메타 API 인증에 필요한 설정을 구성한다."""
    return {
        "access_token": access_token,
        "app_id": app_id,
        "app_secret": app_secret,
    }


@with_duckdb_connection(table="meta_campaigns")
def campaigns(
        access_token: str,
        app_id: str = str(),
        app_secret: str = str(),
        start_date: dt.date | str | None = None,
        end_date: dt.date | str | None = None,
        account_ids: Sequence[str] = list(),
        fields: Sequence[str] = list(),
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """메타 광고 캠페인 목록을 조회하고 `meta_campaigns` 테이블에 적재한다."""
    from linkmerce.core.meta.api.ads.extract import Campaigns
    from linkmerce.core.meta.api.ads.transform import Campaigns as T
    return Campaigns(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(access_token, app_id, app_secret),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(start_date, end_date, account_ids, fields)


@with_duckdb_connection(table="meta_adsets")
def adsets(
        access_token: str,
        app_id: str = str(),
        app_secret: str = str(),
        start_date: dt.date | str | None = None,
        end_date: dt.date | str | None = None,
        account_ids: Sequence[str] = list(),
        fields: Sequence[str] = list(),
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """메타 광고세트 목록을 조회하고 `meta_adsets` 테이블에 적재한다."""
    from linkmerce.core.meta.api.ads.extract import Adsets
    from linkmerce.core.meta.api.ads.transform import Adsets as T
    return Adsets(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(access_token, app_id, app_secret),
        options ={
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(start_date, end_date, account_ids, fields)


@with_duckdb_connection(table="meta_ads")
def ads(
        access_token: str,
        app_id: str = str(),
        app_secret: str = str(),
        start_date: dt.date | str | None = None,
        end_date: dt.date | str | None = None,
        account_ids: Sequence[str] = list(),
        fields: Sequence[str] = list(),
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """메타 광고 목록을 조회하고 `meta_ads` 테이블에 적재한다."""
    from linkmerce.core.meta.api.ads.extract import Ads
    from linkmerce.core.meta.api.ads.transform import Ads as T
    return Ads(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(access_token, app_id, app_secret),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(start_date, end_date, account_ids, fields)


@with_duckdb_connection(tables={
    "campaigns": "meta_campaigns",
    "adsets": "meta_adsets",
    "ads": "meta_ads",
    "insights": "meta_insights",
})
def insights(
        access_token: str,
        ad_level: Literal["campaign", "adset", "ad"],
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["daily", "total"] = "daily",
        app_id: str = str(),
        app_secret: str = str(),
        account_ids: Sequence[str] = list(),
        fields: Sequence[str] = list(),
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """메타 광고 성과 보고서와 캠페인, 광고세트, 소재 목록 목록을 각각의 테이블에 적재한다.

    테이블 키 | 테이블명 | 설명
    - `campaigns` | `meta_campaigns` | 메타 광고 캠페인 목록
    - `adsets` | `meta_adsets` | 메타 광고세트 목록
    - `ads` | `meta_ads` | 메타 광고 목록
    - `insights` | `meta_insights` | 메타 광고 성과 보고서"""
    from linkmerce.core.meta.api.ads.extract import Insights
    from linkmerce.core.meta.api.ads.transform import Insights as T
    return Insights(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(access_token, app_id, app_secret),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(ad_level, start_date, end_date, date_type, account_ids, fields)
