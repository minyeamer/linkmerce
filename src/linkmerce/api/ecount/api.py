from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.api.common import DuckDBResult
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


def request(
        com_code: int | str,
        userid: str,
        api_key: str,
        path: str,
        body: dict | None = None,
        extract_options: dict = dict(),
        **kwargs
    ) -> dict:
    """이카운트 오픈 API 경로를 직접 지정하여 요청을 보내 JSON 형식의 응답을 반환한다.

    Parameters
    ----------
    com_code: int | str
        이카운트 회사코드
    userid: str
        이카운트 아이디
    api_key: str
        오픈 API 인증키
    path: str
        요청할 API 경로
    body: dict | None
        오픈 API 요청 본문
    extract_options: dict
        `Extractor` 초기화 옵션

    Returns
    -------
    JsonObject
        오픈 API 응답 결과
    """
    from linkmerce.core.ecount.api import EcountRequestApi
    from linkmerce.utils.nested import merge
    extractor = EcountRequestApi(**merge(
        extract_options,
        configs = {"com_code": com_code, "userid": userid, "api_key": api_key},
    ))
    return extractor.extract(path, body)


def test(
        com_code: int | str,
        userid: str,
        api_key: str,
        path: str,
        body: dict | None = None,
        extract_options: dict = dict(),
        **kwargs
    ) -> dict:
    """이카운트 테스트 API 경로를 직접 지정하여 요청을 보내 JSON 형식의 응답을 반환한다.

    Parameters
    ----------
    com_code: int | str
        이카운트 회사코드
    userid: str
        이카운트 아이디
    api_key: str
        테스트 API 인증키
    path: str
        테스트 API 경로
    body: dict | None
        테스트 API 요청 본문
    extract_options: dict
        `Extractor` 초기화 옵션

    Returns
    -------
    JsonObject
        테스트 API 응답 결과
    """
    from linkmerce.core.ecount.api import EcountTestApi
    from linkmerce.utils.nested import merge
    extractor = EcountTestApi(**merge(
        extract_options,
        configs = {"com_code": com_code, "userid": userid, "api_key": api_key},
    ))
    return extractor.extract(path, body)


@with_duckdb_connection(table="ecount_product")
def product(
        com_code: int | str,
        userid: str,
        api_key: str,
        product_code: str | None = None,
        comma_yn: bool = True,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | None:
    """이카운트 품목등록 리스트에서 상품 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: ecount_product`

    Parameters
    ----------
    com_code: int | str
        이카운트 회사코드
    userid: str
        이카운트 아이디
    api_key: str
        오픈 API 인증키
    product_code: str | None
        검색할 품목코드. 생략 시 모든 품목을 조회한다.
    comma_yn: bool
        쉼표 사용 여부. 기본값은 `True`
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
    from linkmerce.core.ecount.api.product.extract import Product
    from linkmerce.core.ecount.api.product.transform import Product as T
    return Product(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"com_code": com_code, "userid": userid, "api_key": api_key},
    )).extract(product_code, comma_yn)


@with_duckdb_connection(table="ecount_inventory")
def inventory(
        com_code: int | str,
        userid: str,
        api_key: str,
        base_date: dt.date | str | Literal[":today:"] = ":today:",
        warehouse_code: str | None = None,
        product_code: str | None = None,
        zero_yn: bool = True,
        balanced_yn: bool = False,
        deleted_yn: bool = False,
        safe_yn: bool = False,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | None:
    """이카운트 재고현황 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: ecount_inventory`

    Parameters
    ----------
    com_code: int | str
        이카운트 회사코드
    userid: str
        이카운트 아이디
    api_key: str
        오픈 API 인증키
    base_date: dt.date | str
        조회 기준일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":today:"`: 오늘 날짜 (기본값)
    warehouse_code: str | None
        조회할 창고 코드. 생략 시 전체 창고를 조회한다.
    product_code: str | None
        조회할 품목 코드. 생략 시 모든 품목을 조회한다.
    zero_yn: bool
        재고 수량이 0인 품목 포함 여부. 기본값은 `True`
    balanced_yn: bool
        수량 관리 제외 품목 포함 여부. 기본값은 `False`
    deleted_yn: bool
        사용 중단 품목 포함 여부. 기본값은 `False`
    safe_yn: bool
        안전 재고 설정 미만 표시 여부. 기본값은 `False`
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
    from linkmerce.core.ecount.api.inventory.extract import Inventory
    from linkmerce.core.ecount.api.inventory.transform import Inventory as T
    return Inventory(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"com_code": com_code, "userid": userid, "api_key": api_key},
    )).extract(base_date, warehouse_code, product_code, zero_yn, balanced_yn, deleted_yn, safe_yn)
