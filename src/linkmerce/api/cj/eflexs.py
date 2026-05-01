from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.api.common import DuckDBResult
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


@with_duckdb_connection(table="eflexs_stock")
def stock(
        userid: str,
        passwd: str,
        mail_info: dict,
        customer_id: int | str | Iterable[int | str],
        start_date: dt.date | str | Literal[":last_week:"] = ":last_week:",
        end_date: dt.date | str | Literal[":start_date:", ":today:"] = ":today:",
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """CJ eFLEXs 상세 재고 현황 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: eflexs_stock`

    Parameters
    ----------
    userid: str
        CJ eFLEXs 로그인을 위한 User ID
    passwd: str
        CJ eFLEXs 로그인을 위한 Password
    mail_info: dict[str, str]
        2단계 인증을 위한 이메일 정보. 다음 키값을 포함해야 한다.
            - `origin`: 메일 서비스 도메인
            - `email`: 메일 계정 아이디
            - `passwd`: 메일 계정 비밀번호
    customer_id: int | str | Iterable[int | str]
        조회할 고객 ID. 단일 값 또는 배열을 입력한다.
    start_date: dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":last_week:"`: 오늘 기준 7일 전 날짜 (기본값)
    end_date: dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜
            - `":today:"`: 오늘 날짜 (기본값)
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        고객별 요청 간 대기 시간(초). 기본값은 `1`
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
    from linkmerce.core.cj.eflexs.stock.extract import Stock
    from linkmerce.core.cj.eflexs.stock.transform import Stock as T
    return Stock(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {
            "userid": userid,
            "passwd": passwd,
            "mail_info": mail_info
        },
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(customer_id, start_date, end_date)
