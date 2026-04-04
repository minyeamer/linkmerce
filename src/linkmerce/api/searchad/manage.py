from __future__ import annotations

from linkmerce.api.common import prepare_extract, prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal, Sequence
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    from pathlib import Path
    import datetime as dt


def has_accounts(cookies: str) -> bool:
    """네이버 로그인 쿠키가 광고 계정을 가지고 있는지 검증한다."""
    from linkmerce.core.searchad.manage.common import has_accounts
    import requests
    with requests.Session() as session:
        cookies_map = dict([kv.split('=', maxsplit=1) for kv in cookies.split("; ") if '=' in kv])
        session.cookies.update(cookies_map)
        return has_accounts(session)


def get_accounts(cookies: str, page: int = 0, size: int = 10) -> list[dict]:
    """네이버 로그인 쿠키로 사용 가능한 광고 계정 목록을 조회한다."""
    from linkmerce.core.searchad.manage.common import get_accounts
    import requests
    with requests.Session() as session:
        cookies_map = dict([kv.split('=', maxsplit=1) for kv in cookies.split("; ") if '=' in kv])
        session.cookies.update(cookies_map)
        return get_accounts(session, page, size)


def login(account_no: int | str, cookies: str, save_to: str | Path | None = None) -> str:
    """네이버 쿠키를 가지고 네이버 광고주센터에 로그인해 `XSRF-TOKEN`을 발급받는다."""
    from linkmerce.core.searchad.manage.common import NaverAdLogin
    handler = NaverAdLogin()
    handler.login(account_no, cookies)
    cookies = handler.get_cookies(to="str")
    if cookies and save_to:
        with open(save_to, 'w', encoding="utf-8") as file:
            file.write(cookies)
    return cookies


def advanced_report(
        account_no: int | str,
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
        configs = {"account_no": account_no, "customer_id": customer_id},
        cookies = cookies,
    )).extract(report_id, report_name, userid, attributes, fields, start_date, end_date)


@with_duckdb_connection(table="searchad_report")
def daily_report(
        account_no: int | str,
        customer_id: int | str,
        cookies: str,
        report_id: str,
        report_name: str,
        userid: str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """네이버 검색광고 시스템에서 다차원 보고서를 일별로 다운로드하여 `searchad_report` 테이블에 적재한다."""
    from linkmerce.core.searchad.manage.adreport.extract import DailyReport
    from linkmerce.core.searchad.manage.adreport.transform import DailyReport as T
    return DailyReport(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"account_no": account_no, "customer_id": customer_id},
        cookies = cookies,
    )).extract(report_id, report_name, userid, start_date, end_date)


@with_duckdb_connection(table="searchad_exposure")
def diagnose_exposure(
        account_no: int | str,
        customer_id: int | str,
        cookies: str,
        keyword: str | Iterable[str],
        domain: Literal["search", "shopping"] = "search",
        mobile: bool = True,
        is_own: bool | None = None,
        *,
        connection: DuckDBConnection | None = None,
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
        configs = {"account_no": account_no, "customer_id": customer_id},
        cookies = cookies,
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(keyword, domain, mobile, is_own)


@with_duckdb_connection(tables={"rank": "searchad_rank", "product": "searchad_product"})
def rank_exposure(
        account_no: int | str,
        customer_id: int | str,
        cookies: str,
        keyword: str | Iterable[str],
        domain: Literal["search", "shopping"] = "search",
        mobile: bool = True,
        is_own: bool | None = None,
        *,
        connection: DuckDBConnection | None = None,
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
        configs = {"account_no": account_no, "customer_id": customer_id},
        cookies = cookies,
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(keyword, domain, mobile, is_own)
