from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.api.common import DuckDBResult
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


def _get_api_configs(access_token: str, app_id: str = str(), app_secret: str = str()) -> dict:
    """메타 API 인증에 필요한 공통 설정을 구성한다."""
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
    ) -> DuckDBResult | dict | None:
    """메타 광고 캠페인 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: meta_campaigns`

    Parameters
    ----------
    access_token: str
        메타 액세스 토큰
    app_id: str
        메타 앱 ID (토큰 자동 갱신 시 필요)
    app_secret: str
        메타 앱 시크릿 (토큰 자동 갱신 시 필요)
    start_date: dt.date | str | None
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
    end_date: dt.date | str | None
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
    account_ids: Sequence[str]
        조회할 메타 광고 계정 ID 목록. 생략 시 사용 가능한 모든 계정을 조회한다.
    fields: Sequence[str]
        조회할 필드 목록. 생략 시 클래스에 정의된 `fields` 속성을 사용한다.
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        광고 계정별 요청 간 대기 시간(초). 기본값은 `1`
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
    DuckDBResult | dict | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
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
    ) -> DuckDBResult | dict | None:
    """메타 광고세트 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: meta_adsets`

    Parameters
    ----------
    access_token: str
        메타 액세스 토큰
    app_id: str
        메타 앱 ID (토큰 자동 갱신 시 필요)
    app_secret: str
        메타 앱 시크릿 (토큰 자동 갱신 시 필요)
    start_date: dt.date | str | None
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
    end_date: dt.date | str | None
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
    account_ids: Sequence[str]
        조회할 메타 광고 계정 ID 목록. 생략 시 사용 가능한 모든 계정을 조회한다.
    fields: Sequence[str]
        조회할 필드 목록. 생략 시 클래스에 정의된 `fields` 속성을 사용한다.
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        광고 계정별 요청 간 대기 시간(초). 기본값은 `1`
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
    DuckDBResult | dict | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
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
    ) -> DuckDBResult | dict | None:
    """메타 광고 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: meta_ads`

    Parameters
    ----------
    access_token: str
        메타 액세스 토큰
    app_id: str
        메타 앱 ID (토큰 자동 갱신 시 필요)
    app_secret: str
        메타 앱 시크릿 (토큰 자동 갱신 시 필요)
    start_date: dt.date | str | None
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
    end_date: dt.date | str | None
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
    account_ids: Sequence[str]
        조회할 메타 광고 계정 ID 목록. 생략 시 사용 가능한 모든 계정을 조회한다.
    fields: Sequence[str]
        조회할 필드 목록. 생략 시 클래스에 정의된 `fields` 속성을 사용한다.
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        광고 계정별 요청 간 대기 시간(초). 기본값은 `1`
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
    DuckDBResult | dict | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
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
    ) -> dict[str, DuckDBResult] | dict | None:
    """메타 광고 성과 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Tables** ( *table_key: table_name (description)* ):
        1. `campaigns: meta_campaigns` (캠페인 목록)
        2. `adsets: meta_adsets` (광고세트 목록)
        3. `ads: meta_ads` (광고 목록)
        4. `insights: meta_insights` (성과 보고서)

    Parameters
    ----------
    access_token: str
        메타 액세스 토큰
    ad_level: str
        보고서 집계 기준
            - `"campaign"`: 캠페인
            - `"adset"`: 광고세트
            - `"ad"`: 광고
    start_date: dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
    end_date: dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
    date_type: str
        보고서 기간 구분
            - `"total"`: 합계
            - `"daily"`: 일별 (기본값)
    app_id: str
        메타 앱 ID (토큰 자동 갱신 시 필요)
    app_secret: str
        메타 앱 시크릿 (토큰 자동 갱신 시 필요)
    account_ids: Sequence[str]
        조회할 메타 광고 계정 ID 목록. 생략 시 사용 가능한 모든 계정을 조회한다.
    fields: Sequence[str]
        조회할 필드 목록. 생략 시 클래스에 정의된 `fields` 속성을 사용한다.
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        광고 계정별 요청 간 대기 시간(초). 기본값은 `1`
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
    dict[str, DuckDBResult] | dict | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 모든 테이블 조회 결과를 `{table_key: list[tuple]}` 구조로 반환한다.
            - `"json"`: 모든 테이블 조회 결과를 `{table_key: list[dict]}` 구조로 반환한다. (기본값)
            - `"parquet"`: 모든 테이블 조회 결과를 `{table_key: Parquet 바이너리}` 구조로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
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
