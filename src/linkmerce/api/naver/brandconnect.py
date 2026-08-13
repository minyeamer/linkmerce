from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import datetime as dt
    from typing import Iterable, Literal
    from linkmerce.api.common import DuckDBResult
    from linkmerce.common.load import DuckDBConnection


@with_duckdb_connection(tables={
    "sales": "naver_connect_sales",
    "product": "naver_connect_product",
})
def sales_performances(
        cookies: str,
        space_id: int | str | Iterable[int | str],
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str, DuckDBResult] | dict | list[dict] | None:
    """네이버 쇼핑 커넥트 상품별 판매 실적을 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Tables** ( *table_key: table_name (description)* ):
        1. `sales: naver_connect_sales` (판매 실적)
        2. `product: naver_connect_product` (상품 목록)

    Parameters
    ----------
    cookies: str
        네이버 로그인 쿠키 문자열
    space_id: int | str | Iterable[int | str]
        브랜드 커넥트 스페이스 ID. 정수 또는 문자열, 또는 정수/문자열의 배열을 입력한다.
    start_date: dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
    end_date: dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        스페이스별 요청 간 대기 시간(초). 기본값은 `1`
    progress: bool
        반복 요청 작업의 진행률 출력 여부. 기본값은 `True`
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
            - `"none"`: 모든 과정을 수행하고 `None`을 반환한다.
    """
    from linkmerce.core.naver.brandconnect.sales.extract import SalesPerformances
    from linkmerce.core.naver.brandconnect.sales.transform import SalesPerformances as T
    return SalesPerformances(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies=cookies,
        options={
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)},
            },
        },
    )).extract(space_id, start_date, end_date)
