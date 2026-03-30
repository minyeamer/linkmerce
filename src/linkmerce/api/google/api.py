from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    from pathlib import Path
    import datetime as dt


def _get_api_configs(
        customer_id: int | str,
        manager_id: int | str,
        developer_token: str,
        service_account: str | Path | dict[str, str],
    ) -> dict:
    """구글 Ads API 인증에 필요한 설정을 구성한다."""
    return {
        "customer_id": customer_id,
        "manager_id": manager_id,
        "developer_token": developer_token,
        "service_account": service_account,
    }


@with_duckdb_connection(table="google_campaign")
def campaign(
        customer_id: int | str,
        manager_id: int | str,
        developer_token: str,
        service_account: str | Path | dict[str, str],
        start_date: dt.date | str | None = None,
        end_date: dt.date | str | None = None,
        date_range: Literal[
            "TODAY", "YESTERDAY", "LAST_7_DAYS", "LAST_14_DAYS", "LAST_30_DAYS", "LAST_BUSINESS_WEEK",
            "THIS_MONTH", "LAST_MONTH", "THIS_WEEK_SUN_TODAY", "THIS_WEEK_MON_TODAY",
            "LAST_WEEK_SUN_SAT", "LAST_WEEK_MON_SUN"] | None = "LAST_30_DAYS",
        fields: Sequence[str] = list(),
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """구글 광고 캠페인 목록을 조회하고 `google_campaign` 테이블에 적재한다."""
    from linkmerce.core.google.api.ads.extract import Campaign
    from linkmerce.core.google.api.ads.transform import Campaign as T
    return Campaign(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(customer_id, manager_id, developer_token, service_account),
    )).extract(start_date, end_date, date_range, fields)


@with_duckdb_connection(table="google_adgroup")
def adgroup(
        customer_id: int | str,
        manager_id: int | str,
        developer_token: str,
        service_account: str | Path | dict[str, str],
        start_date: dt.date | str | None = None,
        end_date: dt.date | str | None = None,
        date_range: Literal[
            "TODAY", "YESTERDAY", "LAST_7_DAYS", "LAST_14_DAYS", "LAST_30_DAYS", "LAST_BUSINESS_WEEK",
            "THIS_MONTH", "LAST_MONTH", "THIS_WEEK_SUN_TODAY", "THIS_WEEK_MON_TODAY",
            "LAST_WEEK_SUN_SAT", "LAST_WEEK_MON_SUN"] | None = "LAST_30_DAYS",
        fields: Sequence[str] = list(),
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """구글 광고그룹 목록을 조회하고 `google_adgroup` 테이블에 적재한다."""
    from linkmerce.core.google.api.ads.extract import AdGroup
    from linkmerce.core.google.api.ads.transform import AdGroup as T
    return AdGroup(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(customer_id, manager_id, developer_token, service_account),
    )).extract(start_date, end_date, date_range, fields)


@with_duckdb_connection(table="google_ad")
def ad(
        customer_id: int | str,
        manager_id: int | str,
        developer_token: str,
        service_account: str | Path | dict[str, str],
        start_date: dt.date | str | None = None,
        end_date: dt.date | str | None = None,
        date_range: Literal[
            "TODAY", "YESTERDAY", "LAST_7_DAYS", "LAST_14_DAYS", "LAST_30_DAYS", "LAST_BUSINESS_WEEK",
            "THIS_MONTH", "LAST_MONTH", "THIS_WEEK_SUN_TODAY", "THIS_WEEK_MON_TODAY",
            "LAST_WEEK_SUN_SAT", "LAST_WEEK_MON_SUN"] | None = "LAST_30_DAYS",
        fields: Sequence[str] = list(),
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """구글 광고 소재 목록을 조회하고 `google_ad` 테이블에 적재한다."""
    from linkmerce.core.google.api.ads.extract import Ad
    from linkmerce.core.google.api.ads.transform import Ad as T
    return Ad(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(customer_id, manager_id, developer_token, service_account),
    )).extract(start_date, end_date, date_range, fields)


@with_duckdb_connection(table="google_insight")
def insight(
        customer_id: int | str,
        manager_id: int | str,
        developer_token: str,
        service_account: str | Path | dict[str, str],
        start_date: dt.date | str | None = None,
        end_date: dt.date | str | None = None,
        date_freq: Literal['D', 'W', 'M'] = 'D',
        date_range: Literal[
            "TODAY", "YESTERDAY", "LAST_7_DAYS", "LAST_14_DAYS", "LAST_30_DAYS", "LAST_BUSINESS_WEEK",
            "THIS_MONTH", "LAST_MONTH", "THIS_WEEK_SUN_TODAY", "THIS_WEEK_MON_TODAY",
            "LAST_WEEK_SUN_SAT", "LAST_WEEK_MON_SUN"] | None = "YESTERDAY",
        fields: Sequence[str] = list(),
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """구글 광고 소재의 성과 데이터를 날짜/기기별로 조회하고 `google_insight` 테이블에 적재한다."""
    from linkmerce.core.google.api.ads.extract import Insight
    from linkmerce.core.google.api.ads.transform import Insight as T
    return Insight(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(customer_id, manager_id, developer_token, service_account),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(start_date, end_date, date_freq, date_range, fields)


@with_duckdb_connection(table="google_asset")
def asset(
        customer_id: int | str,
        manager_id: int | str,
        developer_token: str,
        service_account: str | Path | dict[str, str],
        fields: Sequence[str] = list(),
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """구글 광고 애셋 목록을 조회하고 `google_asset` 테이블에 적재한다."""
    from linkmerce.core.google.api.ads.extract import Asset
    from linkmerce.core.google.api.ads.transform import Asset as T
    return Asset(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(customer_id, manager_id, developer_token, service_account),
    )).extract(fields)


@with_duckdb_connection(table="google_asset_view")
def asset_view(
        customer_id: int | str,
        manager_id: int | str,
        developer_token: str,
        service_account: str | Path | dict[str, str],
        start_date: dt.date | str | None = None,
        end_date: dt.date | str | None = None,
        date_freq: Literal['D', 'W', 'M'] = 'D',
        date_range: Literal[
            "TODAY", "YESTERDAY", "LAST_7_DAYS", "LAST_14_DAYS", "LAST_30_DAYS", "LAST_BUSINESS_WEEK",
            "THIS_MONTH", "LAST_MONTH", "THIS_WEEK_SUN_TODAY", "THIS_WEEK_MON_TODAY",
            "LAST_WEEK_SUN_SAT", "LAST_WEEK_MON_SUN"] | None = "YESTERDAY",
        fields: Sequence[str] = list(),
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """구글 광고 소재-애셋 관계를 조회하고 `google_asset_view` 테이블에 적재한다."""
    from linkmerce.core.google.api.ads.extract import AssetView
    from linkmerce.core.google.api.ads.transform import AssetView as T
    return AssetView(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(customer_id, manager_id, developer_token, service_account),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(start_date, end_date, date_freq, date_range, fields)
