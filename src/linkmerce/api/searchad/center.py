from __future__ import annotations

from linkmerce.api.common import prepare_extract, prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.api.common import DuckDBResult
    from linkmerce.common.load import DuckDBConnection
    from pathlib import Path
    import datetime as dt


def has_accounts(cookies: str) -> bool:
    """네이버 로그인 쿠키가 검색광고 계정을 가지고 있는지 확인한다.

    Parameters
    ----------
    cookies: str
        네이버 로그인 쿠키 문자열

    Returns
    -------
    bool
        권한이 있는 검색광고 계정이 확인되면 `True`, 없으면 `False`를 반환한다.
    """
    from linkmerce.core.searchad.center.common import has_accounts
    import requests
    with requests.Session() as session:
        cookies_map = dict([kv.split('=', maxsplit=1) for kv in cookies.split("; ") if '=' in kv])
        session.cookies.update(cookies_map)
        return has_accounts(session)


def get_accounts(cookies: str, page: int = 0, size: int = 10) -> list[dict]:
    """네이버 로그인 쿠키로 권한이 있는 검색광고 계정 목록을 조회한다.

    Parameters
    ----------
    cookies: str
        네이버 로그인 쿠키 문자열
    page: int
        조회할 페이지 번호. 기본값은 `0`
    size: int
        페이지당 계정 수. 기본값은 `10`

    Returns
    -------
    list[dict]
        사용 가능한 검색광고 계정 목록을 반환한다.
    """
    from linkmerce.core.searchad.center.common import get_accounts
    import requests
    with requests.Session() as session:
        cookies_map = dict([kv.split('=', maxsplit=1) for kv in cookies.split("; ") if '=' in kv])
        session.cookies.update(cookies_map)
        return get_accounts(session, page, size)


def login(account_no: int | str, cookies: str, save_to: str | Path | None = None) -> str:
    """네이버 광고주센터에 로그인하고 세션 쿠키를 반환한다.

    Parameters
    ----------
    account_no: int | str
        검색광고 계정 번호
    cookies: str
        네이버 로그인 쿠키 문자열
    save_to: str | Path | None
        로그인 후 쿠키를 저장할 파일 경로. 상위 경로가 없으면 자동 생성한다.

    Returns
    -------
    str
        `XSRF-TOKEN`이 포함된 네이버 광고주센터 로그인 쿠키 문자열을 반환한다.
    """
    from linkmerce.core.searchad.center.common import NaverAdLogin
    from linkmerce.api.common import handle_cookies
    handler = NaverAdLogin()
    handler.login(account_no, cookies)
    return handle_cookies(handler, save_to)


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
    ) -> list[dict] | str:
    """네이버 광고주센터 다차원 보고서를 다운로드해 지정한 형식으로 반환한다.

    Parameters
    ----------
    account_no: int | str
        검색광고 계정 번호
    customer_id: int | str
        검색광고 고객 ID
    cookies: str
        네이버 광고주센터 로그인 쿠키 문자열
    report_id: str
        다차원 보고서 ID. 다차원 보고서 화면의 상세 URL 경로에 포함된 값이다.
    report_name: str
        보고서 이름
    userid: str
        네이버 아이디
    attributes: list[str]
        보고서 항목 목록의 구분 목록
        - 예: 캠페인, 광고그룹, 소재, 매체이름, PC/모바일 매체, 검색/콘텐츠 매체, 일별 등
    fields: list[str]
        보고서 항목 목록의 성과 목록
        - 예: 노출수, 클릭수, 총비용, 총 전환수, 평균노출순위 등
    start_date: dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
    end_date: dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    list[dict] | str
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"json"`: CSV 형식의 보고서를 JSON 형식의 `list[dict]`로 파싱해 반환한다. (기본값)
            - `"raw"`: CSV 형식의 보고서를 원본 텍스트로 반환한다.
    """
    from linkmerce.core.searchad.center.adreport.extract import AdvancedReport
    from linkmerce.core.searchad.center.adreport.transform import AdvancedReport as T
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
    ) -> DuckDBResult | str | None:
    """네이버 광고주센터 다차원 보고서를 다운로드해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: searchad_report`

    Parameters
    ----------
    account_no: int | str
        검색광고 계정 번호
    customer_id: int | str
        검색광고 고객 ID
    cookies: str
        네이버 광고주센터 로그인 쿠키 문자열
    report_id: str
        다차원 보고서 ID. 다차원 보고서 화면의 상세 URL 경로에 포함된 값이다.
    report_name: str
        보고서 이름
    userid: str
        네이버 아이디
    start_date: dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
    end_date: dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    DuckDBResult | str | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 다운로드 후 CSV 텍스트 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.searchad.center.adreport.extract import DailyReport
    from linkmerce.core.searchad.center.adreport.transform import DailyReport as T
    return DailyReport(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"account_no": account_no, "customer_id": customer_id},
        cookies = cookies,
    )).extract(report_id, report_name, userid, start_date, end_date)


@with_duckdb_connection(table="searchad_exposure")
def exposure_status(
        account_no: int | str,
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
    ) -> DuckDBResult | dict | list[dict] | None:
    """네이버 검색광고 키워드 노출 진단 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: searchad_exposure`

    Parameters
    ----------
    account_no: int | str
        검색광고 계정 번호
    customer_id: int | str
        검색광고 고객 ID
    cookies: str
        네이버 광고주센터 로그인 쿠키 문자열
    keyword: str | Iterable[str]
        키워드. 문자열 또는 문자열의 배열을 입력한다.
    domain: str
        매체 구분. `mobile` 값과 조합해 "네이버 통합검색 모바일/PC" 또는 "네이버 쇼핑검색 모바일/PC" 탭을 선택한다.
            - `"search"`: 네이버 통합검색 (기본값)
            - `"shopping"`: 네이버 쇼핑검색
    mobile: bool
        기기 구분
            - `True`: 모바일 (기본값)
            - `False`: PC
    is_own: bool | None
        소유 여부 필터
            - `True`: 내 광고 상품만 필터
            - `False`: 내 광고 상품을 제외하고 필터
            - `None`: 필터하지 않음 (기본값)
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    max_retries: int
        최대 반복 실행 횟수. 기본값은 `5`
    request_delay: float | int | tuple[int, int]
        키워드별 요청 간 대기 시간(초). 기본값은 `1.01`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    DuckDBResult | dict | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 또는 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.searchad.center.exposure.extract import ExposureDiagnosis
    from linkmerce.core.searchad.center.exposure.transform import ExposureDiagnosis as T
    return ExposureDiagnosis(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"account_no": account_no, "customer_id": customer_id},
        cookies = cookies,
        options = {
            "RequestLoop": {
                "max_retries": max_retries
            },
            "RequestEachLoop": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            },
        },
    )).extract(keyword, domain, mobile, is_own)


@with_duckdb_connection(tables={"rank": "searchad_rank", "product": "searchad_product"})
def exposure_rank(
        account_no: int | str,
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
    ) -> dict[str, DuckDBResult] | dict | list[dict] | None:
    """네이버 검색광고 키워드별 노출 순위 및 상품 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Tables** ( *table_key: table_name (description)* ):
        1. `rank: searchad_rank` (노출 상품 순위)
        2. `product: searchad_product` (노출 상품 정보)

    Parameters
    ----------
    account_no: int | str
        검색광고 계정 번호
    customer_id: int | str
        검색광고 고객 ID
    cookies: str
        네이버 광고주센터 로그인 쿠키 문자열
    keyword: str | Iterable[str]
        키워드. 문자열 또는 문자열의 배열을 입력한다.
    domain: str
        매체 구분. `mobile` 값과 조합해 "네이버 통합검색 모바일/PC" 또는 "네이버 쇼핑검색 모바일/PC" 탭을 선택한다.
            - `"search"`: 네이버 통합검색 (기본값)
            - `"shopping"`: 네이버 쇼핑검색
    mobile: bool
        기기 구분
            - `True`: 모바일 (기본값)
            - `False`: PC
    is_own: bool | None
        소유 여부 필터
            - `True`: 내 광고 상품만 필터
            - `False`: 내 광고 상품을 제외하고 필터
            - `None`: 필터하지 않음 (기본값)
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    max_retries: int
        최대 반복 실행 횟수. 기본값은 `5`
    request_delay: float | int | tuple[int, int]
        키워드별 요청 간 대기 시간(초). 기본값은 `1.01`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    dict[str, DuckDBResult] | dict | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 모든 테이블 조회 결과를 `{table_key: list[tuple]}` 구조로 반환한다.
            - `"json"`: 모든 테이블 조회 결과를 `{table_key: list[dict]}` 구조로 반환한다. (기본값)
            - `"parquet"`: 모든 테이블 조회 결과를 `{table_key: Parquet 바이너리}` 구조로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 또는 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.searchad.center.exposure.extract import ExposureDiagnosis
    from linkmerce.core.searchad.center.exposure.transform import ExposureRank as T
    return ExposureDiagnosis(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"account_no": account_no, "customer_id": customer_id},
        cookies = cookies,
        options = {
            "RequestLoop": {
                "max_retries": max_retries
            },
            "RequestEachLoop": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            },
        },
    )).extract(keyword, domain, mobile, is_own)
