from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.api.common import DuckDBResult
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


@with_duckdb_connection(tables={
    "report": "dable_report",
    "campaign": "dable_campaign",
})
def daily_report(
        api_key: str,
        client_name: str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        group_by_campaign: bool = True,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | None:
    """데이블 API로 광고 보고서를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Tables** ( *table_key: table_name (description)* ):
        1. `report: dable_report` (광고 보고서)
        2. `campaign: dable_campaign` (캠페인 목록)

    Parameters
    ----------
    api_key: str
        데이블 설정에서 발급 가능한 API KEY
    client_name: str
        데이블 URL 내 클라이언트 명칭
    start_date: dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
    end_date: dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
    group_by_campaign: bool
        캠페인 단위로 조회할지 여부
            - `True`: 캠페인 단위로 조회 (기본값)
            - `False`: 일별로 조회
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
    DuckDBResult | dict | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.dable.api.report.extract import DailyReport
    from linkmerce.core.dable.api.report.transform import DailyReport as T
    return DailyReport(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"api_key": api_key, "client_name": client_name},
    )).extract(start_date, end_date, group_by_campaign)
