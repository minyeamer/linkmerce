from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    from linkmerce.core.searchad.api.adreport.extract import _ReportsDownload
    import datetime as dt


def _get_api_configs(api_key: str, secret_key: str, customer_id: int | str) -> dict:
    """네이버 검색광고 API 인증에 필요한 설정을 구성한다."""
    return {"api_key": api_key, "secret_key": secret_key, "customer_id": customer_id}


def download_report(
        report_cls: _ReportsDownload | str,
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        return_type: Literal["csv", "json", "raw"] = "csv",
        **kwargs
    ) -> list[tuple] | str:
    """네이버 검색광고 API로 대용량 다운로드 보고서를 다운로드하여 `csv` 또는 `json` 형식으로 반환하다."""
    if isinstance(report_cls, str):
        from importlib import import_module
        report_cls = getattr(import_module("linkmerce.core.searchad.api.adreport.extract"), report_cls)
    extractor: _ReportsDownload = report_cls(configs=_get_api_configs(api_key, secret_key, customer_id))
    tsv_data = extractor.extract(**kwargs)

    if return_type == "json":
        if tsv_data:
            return [dict(zip(extractor.columns, row.split('\t'))) for row in tsv_data.split('\n')]
        return list()
    elif return_type == "csv":
        if tsv_data:
            return [extractor.columns] + [row.split('\t') for row in tsv_data.split('\n')]
        return [extractor.columns]
    return tsv_data


@with_duckdb_connection(table="searchad_campaign")
def campaign(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 검색광고 캠페인 마스터 데이터를 다운로드하여 `searchad_campaign` 테이블에 적재한다."""
    from linkmerce.core.searchad.api.adreport.extract import Campaign
    from linkmerce.core.searchad.api.adreport.transform import Campaign as T
    return Campaign(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(api_key, secret_key, customer_id),
    )).extract(from_date)


@with_duckdb_connection(table="searchad_adgroup")
def adgroup(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 검색광고 광고그룹 마스터 데이터를 다운로드하여 `searchad_adgroup` 테이블에 적재한다."""
    from linkmerce.core.searchad.api.adreport.extract import Adgroup
    from linkmerce.core.searchad.api.adreport.transform import Adgroup as T
    return Adgroup(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(api_key, secret_key, customer_id),
    )).extract(from_date)


@with_duckdb_connection(table="searchad_ad")
def master_ad(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """모든 소재 유형의 네이버 검색광고 마스터 데이터를 일괄 다운로드하여 `searchad_ad` 테이블에 적재한다."""
    from linkmerce.core.searchad.api.adreport.extract import MasterAd
    from linkmerce.core.searchad.api.adreport.transform import MasterAd as T
    return MasterAd(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(api_key, secret_key, customer_id),
    )).extract(from_date)


@with_duckdb_connection(table="searchad_media")
def media(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 검색광고 광고매체 마스터 데이터를 다운로드하여 `searchad_media` 테이블에 적재한다."""
    from linkmerce.core.searchad.api.adreport.extract import Media
    from linkmerce.core.searchad.api.adreport.transform import Media as T
    return Media(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(api_key, secret_key, customer_id),
    )).extract(from_date)


@with_duckdb_connection(table="searchad_report")
def advanced_report(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str, JsonObject]:
    """광고성과 및 전환 보고서를 다운로드하고 다차원 보고서를 생성해 `searchad_report` 테이블에 적재한다."""
    from linkmerce.core.searchad.api.adreport.extract import AdvancedReport
    from linkmerce.core.searchad.api.adreport.transform import AdvancedReport as T
    return AdvancedReport(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(api_key, secret_key, customer_id),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(start_date, end_date)


@with_duckdb_connection(table="searchad_contract")
def time_contract(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 검색광고 API로 브랜드검색 광고 계약기간 데이터를 조회해 `searchad_contract` 테이블에 적재한다."""
    from linkmerce.core.searchad.api.contract.extract import TimeContract
    from linkmerce.core.searchad.api.contract.transform import TimeContract as T
    return TimeContract(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(api_key, secret_key, customer_id),
    )).extract()


@with_duckdb_connection(table="searchad_contract_new")
def brand_new_contract(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 검색광고 API로 신제품검색 광고 계약기간 데이터를 조회해 `searchad_contract_new` 테이블에 적재한다."""
    from linkmerce.core.searchad.api.contract.extract import BrandNewContract
    from linkmerce.core.searchad.api.contract.transform import BrandNewContract as T
    return BrandNewContract(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(api_key, secret_key, customer_id),
    )).extract()


@with_duckdb_connection(table="searchad_keyword")
def keyword(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        keywords: str | Iterable[str],
        max_rank: int | None = None,
        show_detail: bool = True,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 검색광고 키워드 도구의 연관키워드 조회 결과를 `searchad_keyword` 테이블에 적재한다."""
    from linkmerce.core.searchad.api.keyword.extract import Keyword
    from linkmerce.core.searchad.api.keyword.transform import Keyword as T
    return Keyword(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(api_key, secret_key, customer_id),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(keywords, max_rank, (show_detail or (return_type != "raw")))
