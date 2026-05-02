from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.api.common import DuckDBResult
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
    ) -> DuckDBResult | list[dict] | None:
    """네이버 성과형 디스플레이 광고 캠페인 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: searchad_campaign_gfa`

    Parameters
    ----------
    account_no: int | str
        성과형 디스플레이 광고 계정 번호
    cookies: str
        네이버 광고주센터 로그인 쿠키 문자열
    status: Sequence[str]
        캠페인 상태 목록. 기본값은 `["RUNNABLE", "DELETED"]`
            - `"RUNNABLE"`: 운영가능
            - `"DELETED"`: 삭제
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        페이지 및 캠페인 상태별 요청 간 대기 시간(초). 기본값은 `0.3`
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
    DuckDBResult | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
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


@with_duckdb_connection(table="searchad_adset_gfa")
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
    ) -> DuckDBResult | list[dict] | None:
    """네이버 성과형 디스플레이 광고 그룹 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: searchad_adset_gfa`

    Parameters
    ----------
    account_no: int | str
        성과형 디스플레이 광고 계정 번호
    cookies: str
        네이버 광고주센터 로그인 쿠키 문자열
    status: Sequence[str]
        광고그룹 상태 목록. 기본값은 `["ALL", "DELETED"]`
            - `"ALL"`: 모든 상태
            - `"RUNNABLE"`: 운영가능
            - `"BEFORE_STARTING"`: 광고집행전
            - `"TERMINATED"`: 광고집행종료
            - `"DELETED"`: 삭제
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        페이지 및 광고그룹 상태별 요청 간 대기 시간(초). 기본값은 `0.3`
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
    DuckDBResult | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
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
    ) -> DuckDBResult | list[dict] | None:
    """네이버 성과형 디스플레이 광고 소재 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: searchad_creative_gfa`

    Parameters
    ----------
    account_no: int | str
        성과형 디스플레이 광고 계정 번호
    cookies: str
        네이버 광고주센터 로그인 쿠키 문자열
    status: Sequence[str]
        소재 검수 상태 목록. 기본값은 `["ALL", "DELETED"]`
            - `"ALL"`: 모든 상태
            - `"PENDING"`: 검수중
            - `"REJECT"`: 반려
            - `"ACCEPT"`: 승인
            - `"PENDING_IN_OPERATION"`: 승인 (수정사항 검수중)
            - `"REJECT_IN_OPERATION"`: 승인 (수정사항 반려)
            - `"DELETED"`: 삭제
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        페이지 및 소재 검수 상태별 요청 간 대기 시간(초). 기본값은 `0.3`
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
    DuckDBResult | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
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
    ) -> DuckDBResult | dict[str, bytes] | None:
    """네이버 성과형 디스플레이 광고 캠페인 성과 리포트를 다운로드해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: searchad_campaign_report`

    Parameters
    ----------
    account_no: int | str
        성과형 디스플레이 광고 계정 번호
    cookies: str
        네이버 광고주센터 로그인 쿠키 문자열
    start_date: dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
    end_date: dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
    date_type: str
        기간 단위
            - `"TOTAL"`: 전체
            - `"DAY"`: 일 (기본값)
            - `"WEEK"`: 주
            - `"MONTH"`: 월
            - `"HOUR"`: 시간
    columns: list[str]
        열 맞춤 설정
            - `":default:"`: 총비용, 노출수, 클릭수, 총 전환수, 총 전환매출액
    wait_seconds: int
        보고서 생성 완료를 기다리는 최대 시간(초). 기본값은 `60`   
        시간 내 보고서가 생성 완료되지 않으면 `RequestError`를 발생시킨다.
    wait_interval: int
        보고서 생성 완료 여부를 확인하는 조회 간격(초). 기본값은 `1`
    progress: bool
        - `True`: 리포트 생성 요청 및 다운로드 시 진행도를 출력한다. (기본값)
        - `False`: 진행도를 출력하지 않는다.
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
            - `"raw"`: 데이터 다운로드 후 `{파일명: ZIP 바이너리}` 구조의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.searchad.gfa.adreport.extract import PerformanceReport
    from linkmerce.core.searchad.gfa.adreport.transform import CampaignReport as T
    return PerformanceReport(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"account_no": account_no},
        cookies = cookies,
    )).extract("CAMPAIGN", start_date, end_date, date_type, columns, wait_seconds, wait_interval, progress)


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
    ) -> DuckDBResult | dict[str, bytes] | None:
    """네이버 성과형 디스플레이 광고 소재 성과 리포트를 다운로드해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: searchad_creative_report`

    Parameters
    ----------
    account_no: int | str
        성과형 디스플레이 광고 계정 번호
    cookies: str
        네이버 광고주센터 로그인 쿠키 문자열
    start_date: dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
    end_date: dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
    date_type: str
        기간 단위
            - `"TOTAL"`: 전체
            - `"DAY"`: 일 (기본값)
            - `"WEEK"`: 주
            - `"MONTH"`: 월
            - `"HOUR"`: 시간
    columns: list[str]
        열 맞춤 설정
            - `":default:"`: 총비용, 노출수, 클릭수, 총 전환수, 총 전환매출액
    wait_seconds: int
        보고서 생성 완료를 기다리는 최대 시간(초). 기본값은 `60`   
        시간 내 보고서가 생성 완료되지 않으면 `RequestError`를 발생시킨다.
    wait_interval: int
        보고서 생성 완료 여부를 확인하는 조회 간격(초). 기본값은 `1`
    progress: bool
        - `True`: 리포트 생성 요청 및 다운로드 시 진행도를 출력한다. (기본값)
        - `False`: 진행도를 출력하지 않는다.
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
            - `"raw"`: 데이터 다운로드 후 `{파일명: ZIP 바이너리}` 구조의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.searchad.gfa.adreport.extract import PerformanceReport
    from linkmerce.core.searchad.gfa.adreport.transform import CreativeReport as T
    return PerformanceReport(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"account_no": account_no},
        cookies = cookies,
    )).extract("CREATIVE", start_date, end_date, date_type, columns, wait_seconds, wait_interval, progress)
