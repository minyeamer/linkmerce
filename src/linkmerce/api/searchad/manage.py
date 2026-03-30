from __future__ import annotations

from linkmerce.api.common import prepare_extract, prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal, Sequence
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


def has_cookies(cookies: str, **kwargs) -> bool:
    """네이버 로그인 쿠키가 유효한지 검증한다."""
    from linkmerce.core.searchad.manage.common import has_cookies
    import requests
    with requests.Session() as session:
        return has_cookies(session, cookies)


def has_permission(customer_id: int | str, cookies: str, **kwargs) -> bool:
    """네이버 로그인 쿠키가 계정ID에 대한 접근 권한이 있는지 확인한다."""
    from linkmerce.core.searchad.manage.common import has_permission
    import requests
    with requests.Session() as session:
        return has_permission(session, customer_id, cookies)


def whoami(customer_id: int | str, cookies: str, **kwargs) -> dict | None:
    """네이버에서 현재 로그인된 사용자의 검색광고 계정ID를 조회한다."""
    from linkmerce.core.searchad.manage.common import whoami
    import requests
    with requests.Session() as session:
        return whoami(session, customer_id, cookies)


def adreport(
        customer_id: int | str,
        cookies: str,
        report_id: str,
        report_name: str,
        userid: str,
        attributes: list[str],
        fields: list[str],
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        *,
        return_type: Literal["json", "raw"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> Sequence:
    """네이버 검색광고 시스템에서 다차원 보고서를 다운로드한다."""
    from linkmerce.core.searchad.manage.adreport.extract import AdvancedReport
    from linkmerce.core.searchad.manage.adreport.transform import AdvancedReport as T
    return AdvancedReport(**prepare_extract(
        T, extract_options, transform_options, return_type,
        configs = {"customer_id": customer_id},
        headers = {"cookies": cookies},
    )).extract(report_id, report_name, userid, attributes, fields, start_date, end_date)


@with_duckdb_connection(table="searchad_report")
def daily_report(
        customer_id: int | str,
        cookies: str,
        report_id: str,
        report_name: str,
        userid: str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 검색광고 시스템에서 다차원 보고서를 일별로 다운로드하여 `searchad_report` 테이블에 적재한다."""
    from linkmerce.core.searchad.manage.adreport.extract import DailyReport
    from linkmerce.core.searchad.manage.adreport.transform import DailyReport as T
    return DailyReport(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"customer_id": customer_id},
        headers = {"cookies": cookies},
    )).extract(report_id, report_name, userid, start_date, end_date)


@with_duckdb_connection(table="searchad_exposure")
def diagnose_exposure(
        customer_id: int | str,
        cookies: str,
        keyword: str | Iterable[str],
        domain: Literal["search", "shopping"] = "search",
        mobile: bool = True,
        is_own: bool | None = None,
        *,
        connection: DuckDBConnection | None = None,
        max_retries: int = 5,
        request_delay: float | int = 1.01,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 검색광고 키워드 노출 진단 데이터를 조회하고 `searchad_exposure` 테이블에 적재한다."""
    from linkmerce.core.searchad.manage.exposure.extract import ExposureDiagnosis
    from linkmerce.core.searchad.manage.exposure.transform import ExposureDiagnosis as T
    return ExposureDiagnosis(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"customer_id": customer_id},
        headers = {"cookies": cookies},
        options = {
            "RequestLoop": {
                "max_retries": max_retries,
                "raise_errors": RuntimeError, 
                "ignored_errors": Exception
            },
            "RequestEachLoop": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(keyword, domain, mobile, is_own)


@with_duckdb_connection(tables={"rank": "searchad_rank", "product": "searchad_product"})
def rank_exposure(
        customer_id: int | str,
        cookies: str,
        keyword: str | Iterable[str],
        domain: Literal["search", "shopping"] = "search",
        mobile: bool = True,
        is_own: bool | None = None,
        *,
        connection: DuckDBConnection | None = None,
        max_retries: int = 5,
        request_delay: float | int = 1.01,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str, JsonObject]:
    """네이버 검색광고 키워드 노출 진단 데이터로부터 순위 및 상품 목록을 각각의 테이블에 변환 및 적재한다.

    테이블 키 | 테이블명 | 설명
    - `rank` | `searchad_rank` | 광고 노출 순위
    - `product` | `searchad_product` | 광고 노출 상품 목록"""
    from linkmerce.core.searchad.manage.exposure.extract import ExposureDiagnosis
    from linkmerce.core.searchad.manage.exposure.transform import ExposureRank as T
    return ExposureDiagnosis(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"customer_id": customer_id},
        headers = {"cookies": cookies},
        options = {
            "RequestLoop": {
                "max_retries": max_retries,
                "raise_errors": RuntimeError, 
                "ignored_errors": Exception
            },
            "RequestEachLoop": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(keyword, domain, mobile, is_own)
