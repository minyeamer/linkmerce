from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


@with_duckdb_connection(table="searchad_campaign_gfa")
def campaign(
        account_no: int | str,
        cookies: str,
        status: Sequence[Literal["RUNNABLE", "DELETED"]] = ["RUNNABLE", "DELETED"],
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 성과형 디스플레이 광고 캠페인 목록을 조회해 `searchad_campaign_gfa` 테이블에 적재한다."""
    from linkmerce.core.searchad.gfa.adreport.extract import Campaign
    from linkmerce.core.searchad.gfa.adreport.transform import Campaign as T
    return Campaign(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"account_no": account_no},
        cookies = cookies,
        options = {
            "PaginateAll": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}},
            "RequestEachPages": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}},
        },
    )).extract(status)


@with_duckdb_connection(table="searchad_adgroup_gfa")
def adset(
        account_no: int | str,
        cookies: str,
        status: Sequence[Literal["ALL", "RUNNABLE", "BEFORE_STARTING", "TERMINATED", "DELETED"]] = ["ALL", "DELETED"],
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 성과형 디스플레이 광고그룹 목록을 조회해 `searchad_adgroup_gfa` 테이블에 적재한다."""
    from linkmerce.core.searchad.gfa.adreport.extract import AdSet
    from linkmerce.core.searchad.gfa.adreport.transform import AdSet as T
    return AdSet(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"account_no": account_no},
        cookies = cookies,
        options = {
            "PaginateAll": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}},
            "RequestEachPages": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}},
        },
    )).extract(status)


@with_duckdb_connection(table="searchad_creative_gfa")
def creative(
        account_no: int | str,
        cookies: str,
        status: Sequence[Literal["ALL", "PENDING", "REJECT", "ACCEPT", "PENDING_IN_OPERATION", "REJECT_IN_OPERATION", "DELETED"]] = ["ALL", "DELETED"],
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 성과형 디스플레이 광고 소재 목록을 조회해 `searchad_creative_gfa` 테이블에 적재한다."""
    from linkmerce.core.searchad.gfa.adreport.extract import Creative
    from linkmerce.core.searchad.gfa.adreport.transform import Creative as T
    return Creative(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"account_no": account_no},
        cookies = cookies,
        options = {
            "PaginateAll": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}},
            "RequestEachPages": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}},
        },
    )).extract(status)


@with_duckdb_connection(table="searchad_campaign_report")
def campaign_report(
        account_no: int | str,
        cookies: str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["TOTAL", "DAY", "WEEK", "MONTH", "HOUR"] = "DAY",
        columns: list[str] | Literal[":default:"] = ":default:",
        wait_seconds: int = 60,
        wait_interval: int = 1,
        progress: bool = True,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 성과형 디스플레이 광고 캠페인 성과 리포트를 다운로드하여 `searchad_campaign_report` 테이블에 적재한다."""
    from linkmerce.core.searchad.gfa.adreport.extract import CampaignReport
    from linkmerce.core.searchad.gfa.adreport.transform import CampaignReport as T
    return CampaignReport(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"account_no": account_no},
        cookies = cookies,
    )).extract(start_date, end_date, date_type, columns, wait_seconds, wait_interval, progress)


@with_duckdb_connection(table="searchad_creative_report")
def creative_report(
        account_no: int | str,
        cookies: str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["TOTAL", "DAY", "WEEK", "MONTH", "HOUR"] = "DAY",
        columns: list[str] | Literal[":default:"] = ":default:",
        wait_seconds: int = 60,
        wait_interval: int = 1,
        progress: bool = True,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 성과형 디스플레이 광고 소재 성과 리포트를 다운로드하여 `searchad_creative_report` 테이블에 적재한다."""
    from linkmerce.core.searchad.gfa.adreport.extract import CreativeReport
    from linkmerce.core.searchad.gfa.adreport.transform import CreativeReport as T
    return CreativeReport(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"account_no": account_no},
        cookies = cookies,
    )).extract(start_date, end_date, date_type, columns, wait_seconds, wait_interval, progress)
