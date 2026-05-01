from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.api.common import DuckDBResult
    from linkmerce.common.load import DuckDBConnection
    from pathlib import Path
    import datetime as dt


def login(
        userid: str,
        passwd: str,
        domain: Literal["wing", "supplier"] = "wing",
        save_to: str | Path | None = None,
    ) -> str:
    """쿠팡 광고센터 로그인을 수행하여 쿠키를 발급한다.

    Parameters
    ----------
    userid: str
        쿠팡 Wing 또는 서플라이어 허브 로그인 아이디
    passwd: str
        쿠팡 Wing 또는 서플라이어 허브 로그인 비밀번호
    domain: str
        로그인할 판매자 계정의 도메인
            - `"wing"`: 쿠팡 Wing (기본값)
            - `"supplier"`: 쿠팡 서플라이어 허브

    Returns
    -------
    str
        쿠팡 광고센터 로그인 쿠키 문자열
    """
    from linkmerce.core.coupang.advertising.common import CoupangLogin
    from linkmerce.api.common import handle_cookies
    handler = CoupangLogin()
    handler.login(userid, passwd, domain)
    return handle_cookies(handler, save_to)


@with_duckdb_connection(tables={"campaign": "coupang_campaign", "adgroup": "coupang_adgroup"})
def campaign(
        cookies: str,
        vendor_id: str,
        goal_type: Literal["SALES", "NCA", "REACH"] = "SALES",
        is_deleted: bool = False,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str, DuckDBResult] | list[dict] | None:
    """쿠팡 광고센터 캠페인 및 광고그룹 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Tables** ( *table_key: table_name (description)* ):
        1. `campaign: coupang_campaign` (캠페인 목록)
        2. `adgroup: coupang_adgroup` (광고그룹 목록)

    Parameters
    ----------
    cookies: str
        쿠팡 광고센터 로그인 쿠키 문자열
    vendor_id: str
        업체 코드
    goal_type: str
        조회할 광고 목표
            - `"SALES"`: 매출 성장 (기본값)
            - `"NCA"`: 신규 구매 고객 확보
            - `"REACH"`: 인지도 상승
    is_deleted: bool
        삭제된 캠페인 조회 여부
            - `True`: 삭제된 캠페인만 조회
            - `False`: 삭제되지 않은 전체 캠페인 조회 (기본값)
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        페이지 요청 간 대기 시간. 기본값은 `1`
    progress: bool
        페이지 순회 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    dict[str, DuckDBResult] | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 모든 테이블 조회 결과를 `{table_key: list[tuple]}` 구조로 반환한다.
            - `"json"`: 모든 테이블 조회 결과를 `{table_key: list[dict]}` 구조로 반환한다. (기본값)
            - `"parquet"`: 모든 테이블 조회 결과를 `{table_key: Parquet 바이너리}` 구조로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 또는 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.coupang.advertising.adreport.extract import Campaign
    from linkmerce.core.coupang.advertising.adreport.transform import Campaign as T
    return Campaign(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
        options = {
            "PaginateAll": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(goal_type, is_deleted, vendor_id)


@with_duckdb_connection(table="coupang_creative")
def creative(
        cookies: str,
        vendor_id: str,
        campaign_id: int | str | Sequence[int | str],
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """쿠팡 광고센터 신규 구매 고객 확보 캠페인의 소재 정보를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: coupang_creative`

    Parameters
    ----------
    cookies: str
        쿠팡 광고센터 로그인 쿠키 문자열
    vendor_id: str
        업체 코드
    campaign_id: int | str | Sequence[int | str]
        조회할 캠페인 ID. 단일 값 또는 배열을 입력한다.
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        캠페인별 요청 간 대기 시간. 기본값은 `0.3`
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
    from linkmerce.core.coupang.advertising.adreport.extract import Creative
    from linkmerce.core.coupang.advertising.adreport.transform import Creative as T
    return Creative(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(campaign_id, vendor_id)


@with_duckdb_connection(table="coupang_adreport_pa")
def adreport_pa(
        cookies: str,
        vendor_id: str,
        start_date: dt.date | str, 
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["total", "daily"] = "daily",
        report_level: Literal["campaign", "adGroup", "vendorItem", "keyword"] = "vendorItem",
        campaign_ids: Sequence[int | str] = list(),
        wait_seconds: int = 60,
        wait_interval: int = 1,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict[str, bytes] | None:
    """쿠팡 매출 성장 광고 성과 보고서를 생성 및 다운로드해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: coupang_adreport_pa`

    Parameters
    ----------
    cookies: str
        쿠팡 광고센터 로그인 쿠키 문자열
    vendor_id: str
        업체 코드
    start_date: dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
    end_date: dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
    date_type: str
        보고서 기간 구분
            - `"total"`: 합계
            - `"daily"`: 일별 (기본값)
    report_level: str
        보고서 구조
            1. `campaign`: 캠페인
            2. `adGroup`: 캠페인 > 광고그룹
            3. `vendorItem`: 캠페인 > 광고그룹 > 상품 (기본값)
            4. `keyword`: 캠페인 > 광고그룹 > 상품 > 키워드
    campaign_ids: Sequence[int | str]
        조회할 캠페인 ID 목록. 생략 시 기간 내 전체 캠페인을 조회하여 선택한다.
    wait_seconds: int
        보고서 생성 완료를 기다리는 최대 시간(초). 기본값은 `60`   
        시간 내 보고서가 생성 완료되지 않으면 `ValueError`를 발생시킨다.
    wait_interval: int
        보고서 생성 완료 여부를 확인하는 조회 간격(초). 기본값은 `1`
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
    DuckDBResult | dict[str, bytes] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 다운로드 후 `{파일명: 엑셀 바이너리}` 구조의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.coupang.advertising.adreport.extract import ProductAdReport
    from linkmerce.core.coupang.advertising.adreport.transform import ProductAdReport as T
    return ProductAdReport(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
    )).extract(start_date, end_date, date_type, report_level, campaign_ids, vendor_id, wait_seconds, wait_interval)


@with_duckdb_connection(table="coupang_adreport_nca")
def adreport_nca(
        cookies: str,
        vendor_id: str,
        start_date: dt.date | str, 
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["total", "daily"] = "daily",
        report_level: Literal["campaign", "ad", "keyword", "creative"] = "creative",
        campaign_ids: Sequence[int | str] = list(),
        wait_seconds: int = 60,
        wait_interval: int = 1,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict[str, bytes] | None:
    """쿠팡 신규 구매 고객 확보 광고 성과 보고서를 생성 및 다운로드해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: coupang_adreport_nca`

    Parameters
    ----------
    cookies: str
        쿠팡 광고센터 로그인 쿠키 문자열
    vendor_id: str
        업체 코드
    start_date: dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
    end_date: dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
    date_type: str
        보고서 기간 구분
            - `"total"`: 합계
            - `"daily"`: 일별 (기본값)
    report_level: str
        보고서 구조
            1. `campaign`: 캠페인
            2. `ad`: 캠페인 > 광고
            3. `keyword`: 캠페인 > 광고 > 키워드
            4. `creative`: 캠페인 > 광고 > 키워드 > 소재 (기본값)
    campaign_ids: Sequence[int | str]
        조회할 캠페인 ID 목록. 생략 시 기간 내 전체 캠페인을 조회하여 선택한다.
    wait_seconds: int
        보고서 생성 완료를 기다리는 최대 시간(초). 기본값은 `60`   
        시간 내 보고서가 생성 완료되지 않으면 `ValueError`를 발생시킨다.
    wait_interval: int
        보고서 생성 완료 여부를 확인하는 조회 간격(초). 기본값은 `1`
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
    DuckDBResult | dict[str, bytes] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 다운로드 후 `{파일명: 엑셀 바이너리}` 구조의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.coupang.advertising.adreport.extract import NewCustomerAdReport
    from linkmerce.core.coupang.advertising.adreport.transform import NewCustomerAdReport as T
    return NewCustomerAdReport(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
    )).extract(start_date, end_date, date_type, report_level, campaign_ids, vendor_id, wait_seconds, wait_interval)
