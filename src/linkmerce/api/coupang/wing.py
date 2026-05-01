from __future__ import annotations

from linkmerce.api.common import DuckDBResult, prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.load import DuckDBConnection
    from pathlib import Path
    import datetime as dt


def login(
        userid: str,
        passwd: str,
        domain: Literal["wing", "supplier"] = "wing",
        with_token: bool = True,
        save_to: str | Path | None = None,
    ) -> str:
    """쿠팡 Wing 또는 서플라이어 허브에 로그인하고 쿠키 문자열을 반환한다.

    Parameters
    ----------
    userid: str
        쿠팡 Wing 또는 서플라이어 허브 로그인 아이디
    passwd: str
        쿠팡 Wing 또는 서플라이어 허브 로그인 비밀번호
    domain: str
        로그인할 판매자 계정의 도메인
            - `"wing"`: 쿠팡 Wing (기본값)
            - `"supplier"`: 서플라이어 허브
    with_token: bool
        `XSRF-TOKEN` 포함 여부
            - `True`: 추가 요청을 보내 `XSRF-TOKEN` 값을 취득한다. (기본값)
            - `False`: 추가 요청 없이 `XSRF-TOKEN` 값이 누락된 쿠키를 반환한다.
    save_to: str | Path | None
        로그인 후 쿠키를 저장할 파일 경로. 상위 경로가 없으면 자동 생성한다.

    Returns
    -------
    str
        쿠팡 Wing 또는 서플라이어 로그인 쿠키 문자열
    """
    from linkmerce.core.coupang.wing.common import CoupangLogin
    from linkmerce.api.common import handle_cookies
    handler = CoupangLogin()
    handler.login(userid, passwd, domain, with_token)
    return handle_cookies(handler, save_to)


@with_duckdb_connection(table="coupang_product")
def product_option(
        cookies: str,
        is_deleted: bool = False,
        see_more: bool = False,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: tuple[dict | None, dict | None] = (None, None),
        transform_options: tuple[dict | None, dict | None] = (None, None),
    ) -> DuckDBResult | list[dict] | dict[str, list[dict]] | None:
    """쿠팡 Wing 상품 목록에서 옵션 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: coupang_product`

    Parameters
    ----------
    cookies: str
        쿠팡 Wing 로그인 쿠키 문자열
    is_deleted: bool
        삭제된 상품 조회 여부
            - `True`: 삭제된 상품만 조회
            - `False`: 삭제되지 않은 전체 상품 조회 (기본값)
    see_more: bool
        - `True`: 상품별로 상세 정보(`ProductDetail`)를 추가 요청해 결과를 보강한다.
        - `False`: 상품 목록에서 조회되는 항목만 수집한다. (기본값)
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        페이지 및 상품별 요청 간 대기 시간(초). 기본값은 `0.3`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: tuple[dict | None, dict | None]
        `Extractor` 초기화 옵션. `(ProductOption, ProductDetail)` 순서로 튜플을 구성한다.
    transform_options: tuple[dict | None, dict | None]
        `Transformer` 초기화 옵션. `(ProductOption, ProductDetail)` 순서로 튜플을 구성한다.

    Returns
    -------
    DuckDBResult | list[dict] | dict[str, list[dict]] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.

        **NOTE** `see_more=True` 조건으로 실행하면 결과가 다르다.
            - 변환 및 적재 과정을 거치면 상품 목록 테이블에 누락된 `[ 노출상품ID, 옵션ID, 판매가 ]` 값이 추가된다.
            - 원본 응답을 반환할 때는 `{"products": [ProductOption], "details": [ProductDetail]}` 구조로 반환된다.
    """
    from linkmerce.core.coupang.wing.product.extract import ProductOption
    from linkmerce.core.coupang.wing.product.transform import ProductOption as T
    OPTION, DETAIL = 0, 1

    if isinstance((opt := transform_options[OPTION]), dict) and ("tables" in opt):
        common = opt["tables"]
    else:
        common = dict(tables={"table": "coupang_product"})

    products = ProductOption(**prepare_duckdb_extract(
        T, connection, extract_options[OPTION], transform_options[OPTION], return_type,
        cookies = cookies,
        options = {
            "PaginateAll": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(is_deleted)

    if see_more:
        from linkmerce.core.coupang.wing.product.extract import ProductDetail
        from linkmerce.core.coupang.wing.product.transform import ProductDetail as T

        if return_type == "raw":
            ids = set()
            for result in products:
                ids = ids.union({product["vendor_inventory_id"] for product in result})
            vendor_inventory_id = list(ids)
        else:
            query = "SELECT DISTINCT vendor_inventory_id FROM {}".format(common["tables"]["table"])
            vendor_inventory_id = [row[0] for row in connection.execute(query)[0].fetchall()]

        details = ProductDetail(**prepare_duckdb_extract(
            T, connection, extract_options[DETAIL],
            transform_options = ((transform_options[DETAIL] or dict()) | common),
            return_type = return_type,
            cookies = cookies,
            options = {
                "RequestEach": {
                    "request_delay": request_delay,
                    "tqdm_options": {"disable": (not progress)}
                }
            },
        )).extract(vendor_inventory_id, referer="vendor")
        return {"products": products, "details": details}
    return products


@with_duckdb_connection(table="coupang_product_detail")
def product_detail(
        cookies: str,
        vendor_inventory_id: int | str | Sequence[int | str],
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """쿠팡 Wing 상품별 상세 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: coupang_product_detail`

    Parameters
    ----------
    cookies: str
        쿠팡 Wing 로그인 쿠키 문자열
    vendor_inventory_id: int | str | Sequence[int | str]
        조회할 등록상품 ID. 단일 값 또는 배열을 입력한다.
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        상품별 요청 간 대기 시간(초). 기본값은 `0.3`
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
    from linkmerce.core.coupang.wing.product.extract import ProductDetail
    from linkmerce.core.coupang.wing.product.transform import ProductDetail as T
    return ProductDetail(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(vendor_inventory_id)


@with_duckdb_connection(table="coupang_product_download")
def product_download(
        cookies: str,
        vendor_id: str,
        request_type: str = "VENDOR_INVENTORY_ITEM",
        fields: list[str] = list(),
        is_deleted: bool = False,
        wait_seconds: int = 60,
        wait_interval: int = 1,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict[str, bytes] | None:
    """쿠팡 Wing 상품 목록을 엑셀로 다운로드해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: coupang_product_download`

    Parameters
    ----------
    cookies: str
        쿠팡 Wing 로그인 쿠키 문자열
    vendor_id: str
        업체 코드
    request_type: str
        상품 조회조건. `"VENDOR_INVENTORY_ITEM"` 조건만 데이터 변환을 지원한다.
            - `"VENDOR_INVENTORY_ITEM"`: 가격/재고/판매상태 (기본값)
            - `"EDITABLE_CATALOGUE"`: 쿠팡상품정보
            - `"BULK_DELETE_INVENTORY"`: 상품삭제
    fields: list[str]
        엑셀 항목 코드 목록. 생략 시 기본 항목으로 조회한다.
    is_deleted: bool
        삭제된 상품 조회 여부
            - `True`: 삭제된 상품만 조회
            - `False`: 삭제되지 않은 전체 상품 조회 (기본값)
    wait_seconds: int
        파일 생성 완료를 기다리는 최대 시간(초). 기본값은 `60`
    wait_interval: int
        파일 완료 여부를 확인하는 조회 간격(초). 기본값은 `1`
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
    from linkmerce.core.coupang.wing.product.extract import ProductDownload
    from linkmerce.core.coupang.wing.product.transform import ProductDownload as T
    return ProductDownload(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
    )).extract(request_type, fields, is_deleted, vendor_id, wait_seconds, wait_interval)


@with_duckdb_connection(table="coupang_rocket_inventory")
def rocket_inventory(
        cookies: str,
        vendor_id: str,
        hidden_status: Literal["VISIBLE", "HIDDEN"] | None = None,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | list[dict] | None:
    """쿠팡 로켓그로스 재고현황에서 재고 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: coupang_rocket_inventory`

    Parameters
    ----------
    cookies: str
        쿠팡 Wing 로그인 쿠키 문자열 (`XSRF-TOKEN` 포함)
    vendor_id: str
        업체 코드
    hidden_status: str | None
        상품 상태 필터
            - `"VISIBLE"`: 노출 상품
            - `"HIDDEN"`: 숨겨진 상품
            - `None`: 전체 상품 (기본값)
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        커서 요청 간 대기 시간(초). 기본값은 `1`
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
    from linkmerce.core.coupang.wing.product.extract import RocketInventory
    from linkmerce.core.coupang.wing.product.transform import RocketInventory as T
    return RocketInventory(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
        options = {"CursorAll": {"request_delay": request_delay}},
    )).run(hidden_status, vendor_id, how_to_run="sync")


@with_duckdb_connection(table="coupang_rocket_option")
def rocket_option(
        cookies: str,
        vendor_id: str,
        hidden_status: Literal["VISIBLE", "HIDDEN"] | None = None, 
        see_more: bool = False,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: tuple[dict | None, dict | None] = (None, None),
        transform_options: tuple[dict | None, dict | None] = (None, None),
    ) -> DuckDBResult | list[dict] | None:
    """쿠팡 로켓그로스 재고현황에서 옵션 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: coupang_rocket_option`

    Parameters
    ----------
    cookies: str
        쿠팡 Wing 로그인 쿠키 문자열 (`XSRF-TOKEN` 포함)
    vendor_id: str
        업체 코드
    hidden_status: str | None
        상품 상태 필터
            - `"VISIBLE"`: 노출 상품
            - `"HIDDEN"`: 숨겨진 상품
            - `None`: 전체 상품 (기본값)
    see_more: bool
        - `True`: 상품별로 상세 정보(`ProductDetail`)를 추가 요청해 결과를 보강한다.
        - `False`: 재고현황에서 조회되는 항목만 수집한다. (기본값)
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        커서 및 상품별 요청 간 대기 시간(초). 기본값은 `0.3`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: tuple[dict | None, dict | None]
        `Extractor` 초기화 옵션. `(RocketInventory, ProductDetail)` 순서로 튜플을 구성한다.
    transform_options: tuple[dict | None, dict | None]
        `Transformer` 초기화 옵션. `(RocketOption, ProductDetail)` 순서로 튜플을 구성한다.

    Returns
    -------
    DuckDBResult | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.

        **NOTE** `see_more=True` 조건으로 실행하면 결과가 다르다.
            - 변환 및 적재 과정을 거치면 로켓 상품 테이블에 누락된 `[ 등록옵션ID, 옵션ID, 바코드 ]` 값이 추가된다.
            - 원본 응답을 반환할 때는 `{"products": [RocketOption], "details": [ProductDetail]}` 구조로 반환된다.
    """
    from linkmerce.core.coupang.wing.product.extract import RocketInventory
    from linkmerce.core.coupang.wing.product.transform import RocketOption as T
    OPTION, DETAIL = 0, 1

    if isinstance((opt := transform_options[OPTION]), dict) and ("tables" in opt):
        common = opt["tables"]
    else:
        common = dict(tables={"table": "coupang_rocket_option"})

    products = RocketInventory(**prepare_duckdb_extract(
        T, connection, extract_options[OPTION], transform_options[OPTION], return_type,
        cookies = cookies,
        options = {"CursorAll": {"request_delay": request_delay}},
    )).extract(hidden_status, vendor_id)

    if see_more:
        from linkmerce.core.coupang.wing.product.extract import ProductDetail
        from linkmerce.core.coupang.wing.product.transform import ProductDetail as T

        if return_type == "raw":
            ids = set()
            for result in products:
                ids = ids.union({product["vendor_inventory_id"] for product in result})
            vendor_inventory_id = list(ids)
        else:
            query = "SELECT DISTINCT vendor_inventory_id FROM {}".format(common["tables"]["table"])
            vendor_inventory_id = [row[0] for row in connection.execute(query)[0].fetchall()]

        details = ProductDetail(**prepare_duckdb_extract(
            T, connection, extract_options[DETAIL],
            transform_options = ((transform_options[DETAIL] or dict()) | common),
            return_type = return_type,
            cookies = cookies,
            options = {
                "RequestEach": {
                    "request_delay": request_delay,
                    "tqdm_options": {"disable": (not progress)}
                }
            },
        )).extract(vendor_inventory_id, referer="rfm")
        return {"products": products, "details": details}
    return products


def summary(
        cookies: str,
        start_from: str,
        end_to: str,
        extract_options: dict = dict(),
    ) -> dict:
    """쿠팡 로켓그로스 수익 현황 데이터를 조회한다.

    Parameters
    ----------
    cookies: str
        쿠팡 Wing 로그인 쿠키 문자열 (`XSRF-TOKEN` 포함)
    start_from: str
        기준일 시작. UTC 시간대의 ISO 8601 문자열을 입력한다.
    end_to: str
        기준일 종료. UTC 시간대의 ISO 8601 문자열을 입력한다.
    extract_options: dict
        `Extractor` 초기화 옵션

    Returns
    -------
    dict
        기간 내 수익 현황
    """
    from linkmerce.core.coupang.wing.settlement.extract import Summary
    from linkmerce.utils.nested import merge
    extractor = Summary(**merge(extract_options or dict(), headers={"cookies": cookies}))
    return extractor.extract(start_from, end_to)


@with_duckdb_connection(table="coupang_rocket_settlement")
def rocket_settlement(
        cookies: str,
        vendor_id: str,
        start_date: dt.date | str, 
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["PAYMENT", "SALES"] = "SALES",
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | None:
    """쿠팡 로켓그로스 정산현황의 정산 리포트 목록을 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: coupang_rocket_settlement`

    Parameters
    ----------
    cookies: str
        쿠팡 Wing 로그인 쿠키 문자열 (`XSRF-TOKEN` 포함)
    vendor_id: str
        업체 코드
    start_date: dt.date | str
        기준일 시작. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
    end_date: dt.date | str
        기준일 종료. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
    date_type: str
        기준일 유형
            - `"PAYMENT"`: 정산일
            - `"SALES"`: 매출 인식일 (기본값)
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
    from linkmerce.core.coupang.wing.settlement.extract import RocketSettlement
    from linkmerce.core.coupang.wing.settlement.transform import RocketSettlement as T
    return RocketSettlement(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
    )).extract(start_date, end_date, date_type, vendor_id)


@with_duckdb_connection(tables={"sales": "coupang_rocket_sales", "shipping": "coupang_rocket_shipping"})
def rocket_settlement_download(
        cookies: str,
        vendor_id: str,
        start_date: dt.date | str, 
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["PAYMENT", "SALES"] = "SALES",
        wait_seconds: int = 60,
        wait_interval: int = 1,
        progress: bool = True,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str, DuckDBResult] | dict[str, bytes] | None:
    """쿠팡 로켓그로스 정산 리포트를 요청 및 다운로드해 DuckDB 테이블에 변환 및 적재한다.

    **Tables** ( *table_key: table_name (description)* ):
        1. `sales: coupang_rocket_sales` (판매 수수료 리포트)
        2. `shipping: coupang_rocket_shipping` (입출고비/배송비 리포트)

    Parameters
    ----------
    cookies: str
        쿠팡 Wing 로그인 쿠키 문자열 (`XSRF-TOKEN` 포함)
    vendor_id: str
        업체 코드
    start_date: dt.date | str
        기준일 시작. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
    end_date: dt.date | str
        기준일 종료. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
    date_type: str
        기준일 유형
            - `"PAYMENT"`: 정산일
            - `"SALES"`: 매출 인식일 (기본값)
    wait_seconds: int
        파일 생성 완료를 기다리는 최대 시간(초). 기본값은 `60`.   
        시간 내 엑셀 파일이 생성 완료되지 않으면 `ValueError`를 발생시킨다.
    wait_interval: int
        파일 생성 완료 여부를 확인하는 조회 간격(초). 기본값은 `1`
    progress: bool
        정산 리포트 다운로드 진행도 출력 여부. 기본값은 `True`
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
    dict[str, DuckDBResult] | dict[str, bytes] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 모든 테이블 조회 결과를 `{table_key: list[tuple]}` 구조로 반환한다.
            - `"json"`: 모든 테이블 조회 결과를 `{table_key: list[dict]}` 구조로 반환한다. (기본값)
            - `"parquet"`: 모든 테이블 조회 결과를 `{table_key: Parquet 바이너리}` 구조로 반환한다.
            - `"raw"`: 데이터 다운로드 후 `{파일명: 엑셀 바이너리}` 구조의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.coupang.wing.settlement.extract import RocketSettlementDownload
    from linkmerce.core.coupang.wing.settlement.transform import RocketSettlementDownload as T
    return RocketSettlementDownload(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
    )).extract(start_date, end_date, date_type, vendor_id, wait_seconds, wait_interval, progress)
