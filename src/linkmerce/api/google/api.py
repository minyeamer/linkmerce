from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.api.common import DuckDBResult
    from linkmerce.common.load import DuckDBConnection
    from pathlib import Path
    import datetime as dt


def _get_api_configs(
        customer_id: int | str,
        manager_id: int | str,
        developer_token: str,
        service_account: str | Path | dict[str, str],
    ) -> dict:
    """구글 Ads API 인증에 필요한 공통 설정을 구성한다."""
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
    ) -> DuckDBResult | list[dict] | None:
    """구글 광고 캠페인 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: google_campaign`

    Parameters
    ----------
    customer_id: int | str
        구글 광고 고객 ID
    manager_id: int | str
        구글 광고 관리자 계정의 고객 ID
    developer_token: str
        구글 광고 API 개발자 토큰
    service_account: str | Path | dict[str, str]
        구글 서비스 계정 키 파일 경로 또는 파일 내용을 파싱한 딕셔너리
    start_date: dt.date | str | None
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
    end_date: dt.date | str | None
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
    date_range: str | None
        GAQL 사전 정의 조회 기간. `start_date` 또는 `end_date`가 있으면 무시된다.
    fields: Sequence[str]
        조회할 GAQL 필드 목록. 생략 시 클래스에 정의된 `fields` 속성을 사용한다.
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
    DuckDBResult | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
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
    ) -> DuckDBResult | list[dict] | None:
    """구글 광고 광고그룹 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: google_adgroup`

    Parameters
    ----------
    customer_id: int | str
        구글 광고 고객 ID
    manager_id: int | str
        구글 광고 관리자 계정의 고객 ID
    developer_token: str
        구글 광고 API 개발자 토큰
    service_account: str | Path | dict[str, str]
        구글 서비스 계정 키 파일 경로 또는 파일 내용을 파싱한 딕셔너리
    start_date: dt.date | str | None
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
    end_date: dt.date | str | None
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
    date_range: str | None
        GAQL 사전 정의 조회 기간. `start_date` 또는 `end_date`가 있으면 무시된다.
    fields: Sequence[str]
        조회할 GAQL 필드 목록. 생략 시 클래스에 정의된 `fields` 속성을 사용한다.
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
    DuckDBResult | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
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
    ) -> DuckDBResult | list[dict] | None:
    """구글 광고 소재 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: google_ad`

    Parameters
    ----------
    customer_id: int | str
        구글 광고 고객 ID
    manager_id: int | str
        구글 광고 관리자 계정의 고객 ID
    developer_token: str
        구글 광고 API 개발자 토큰
    service_account: str | Path | dict[str, str]
        구글 서비스 계정 키 파일 경로 또는 파일 내용을 파싱한 딕셔너리
    start_date: dt.date | str | None
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
    end_date: dt.date | str | None
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
    date_range: str | None
        GAQL 사전 정의 조회 기간. `start_date` 또는 `end_date`가 있으면 무시된다.
    fields: Sequence[str]
        조회할 GAQL 필드 목록. 생략 시 클래스에 정의된 `fields` 속성을 사용한다.
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
    DuckDBResult | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
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
    ) -> DuckDBResult | list[dict] | None:
    """구글 광고 소재 성과 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: google_insight`

    Parameters
    ----------
    customer_id: int | str
        구글 광고 고객 ID
    manager_id: int | str
        구글 광고 관리자 계정의 고객 ID
    developer_token: str
        구글 광고 API 개발자 토큰
    service_account: str | Path | dict[str, str]
        구글 서비스 계정 키 파일 경로 또는 파일 내용을 파싱한 딕셔너리
    start_date: dt.date | str | None
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
    end_date: dt.date | str | None
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
    date_freq: str
        조회 범위 및 데이터 집계 기간
            - `'D'`: 일 단위로 기간을 분할한다. (기본값)
            - `'W'`: 월요일 기준 주 단위로 기간을 분할한다.
            - `'M'`: 매월 1일 기준 월 단위로 기간을 분할한다.
    date_range: str | None
        GAQL 사전 정의 조회 기간. `start_date` 또는 `end_date`가 있으면 무시된다.
    fields: Sequence[str]
        조회할 GAQL 필드 목록. 생략 시 클래스에 정의된 `fields` 속성을 사용한다.
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        조회 기간별 요청 간 대기 시간(초). 기본값은 `1`
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
    ) -> DuckDBResult | list[dict] | None:
    """구글 광고 애셋 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: google_asset`

    Parameters
    ----------
    customer_id: int | str
        구글 광고 고객 ID
    manager_id: int | str
        구글 광고 관리자 계정의 고객 ID
    developer_token: str
        구글 광고 API 개발자 토큰
    service_account: str | Path | dict[str, str]
        구글 서비스 계정 키 파일 경로 또는 파일 내용을 파싱한 딕셔너리
    fields: Sequence[str]
        조회할 GAQL 필드 목록. 생략 시 클래스에 정의된 `fields` 속성을 사용한다.
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
    DuckDBResult | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
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
    ) -> DuckDBResult | list[dict] | None:
    """구글 광고 소재-애셋 관계 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: google_asset_view`

    Parameters
    ----------
    customer_id: int | str
        구글 광고 고객 ID
    manager_id: int | str
        구글 광고 관리자 계정의 고객 ID
    developer_token: str
        구글 광고 API 개발자 토큰
    service_account: str | Path | dict[str, str]
        구글 서비스 계정 키 파일 경로 또는 파일 내용을 파싱한 딕셔너리
    start_date: dt.date | str | None
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
    end_date: dt.date | str | None
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
    date_freq: str
        조회 범위 및 데이터 집계 기간
            - `'D'`: 일 단위로 기간을 분할한다. (기본값)
            - `'W'`: 월요일 기준 주 단위로 기간을 분할한다.
            - `'M'`: 매월 1일 기준 월 단위로 기간을 분할한다.
    date_range: str | None
        GAQL 사전 정의 조회 기간. `start_date` 또는 `end_date`가 있으면 무시된다.
    fields: Sequence[str]
        조회할 GAQL 필드 목록. 생략 시 클래스에 정의된 `fields` 속성을 사용한다.
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        조회 기간별 요청 간 대기 시간(초). 기본값은 `1`
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
