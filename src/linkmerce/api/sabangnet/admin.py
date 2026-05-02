from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.api.common import DuckDBResult
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


def _get_login_configs(userid: str, passwd: str, domain: int) -> dict:
    """사방넷 로그인에 필요한 공통 설정을 구성한다."""
    return {"userid": userid, "passwd": passwd, "domain": domain}


def login(userid: str, passwd: str) -> dict[str, str]:
    """사방넷에 로그인하고 쿠키와 토큰을 반환한다.

    Parameters
    ----------
    userid: str
        사방넷 아이디
    passwd: str
        사방넷 비밀번호

    Returns
    -------
    dict[str, str]
        - `"cookies"`: 사방넷 로그인 쿠키 문자열
        - `"access_token"`: 사방넷 액세스 토큰
        - `"cookies"`: 액세스 토큰 만료시 필요한 갱신 토큰
    """
    from linkmerce.core.sabangnet.admin.common import SabangnetLogin
    auth = SabangnetLogin()
    return auth.login(userid, passwd)


@with_duckdb_connection(table="sabangnet_order_detail")
def order(
        userid: str,
        passwd: str,
        domain: int,
        start_date: dt.datetime | dt.date | str | Literal[":today:"] = ":today:",
        end_date: dt.datetime | dt.date | str | Literal[":start_date:", ":now:"] = ":start_date:",
        date_type: str = "reg_dm",
        order_status_div: str = str(),
        order_status: list[str] = list(),
        shop_id: str = str(),
        sort_type: str = "ord_no_asc",
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | list[dict] | None:
    """사방넷 주문서확인처리 메뉴의 주문 내역 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: sabangnet_order_detail`

    Parameters
    ----------
    userid: str
        사방넷 아이디
    passwd: str
        사방넷 비밀번호
    domain: int
        사방넷 시스템 도메인 번호
    start_date: dt.datetime | dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":today:"`: 오늘 날짜 (기본값)
    end_date: dt.datetime | dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
            - `":now:"`: 현재 시각
    date_type: str
        일자 유형. 기본값은 수집일(`"reg_dm"`)
    order_status_div: str
        주문구분 코드
    order_status: list[str]
        주문상태 코드 목록
    shop_id: str
        쇼핑몰 ID 검색 조건
    sort_type: str
        정렬순서 코드. `"<정렬기준>_<asc|desc>"` 형식이며, 기본값은 `"ord_no_asc"`
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        페이지 요청 간 대기 시간(초). 기본값은 `1`
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
    DuckDBResult | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.sabangnet.admin.order.extract import Order
    from linkmerce.core.sabangnet.admin.order.transform import Order as T
    return Order(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_login_configs(userid, passwd, domain),
        options = {
            "PaginateAll": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(start_date, end_date, date_type, order_status_div, order_status, shop_id, sort_type)


def order_download(
        userid: str,
        passwd: str,
        domain: int,
        download_no: int,
        download_type: Literal["order", "option", "invoice", "dispatch"],
        start_date: dt.datetime | dt.date | str | Literal[":today:"] = ":today:",
        end_date: dt.datetime | dt.date | str | Literal[":start_date:", ":now:"] = ":start_date:",
        date_type: str = "reg_dm",
        order_seq: list[int] = list(),
        order_status_div: str = str(),
        order_status: list[str] = list(),
        shop_id: str = str(),
        sort_type: str = "ord_no_asc",
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict[str, bytes] | None:
    """사방넷 주문서확인처리 메뉴의 주문 내역 데이터를 다운로드해 DuckDB 테이블에 변환 및 적재한다.

    **Tables** ( *table_key: table_name (description)* ):
        1. `order: sabangnet_order` (주문 내역)
        2. `option: sabangnet_option` (주문 옵션 목록)
        3. `invoice: sabangnet_invoice` (발주 내역)
        4. `dispatch: sabangnet_dispatch` (발송 내역)

    Parameters
    ----------
    userid: str
        사방넷 아이디
    passwd: str
        사방넷 비밀번호
    domain: int
        사방넷 시스템 도메인 번호
    download_no: int
        주문서 출력 양식 번호
    download_type: str
        다운로드 유형. 주문 내역을 스키마에 맞춰 변환하고 적재할 테이블의 `table_key` 하나를 입력한다.
    start_date: dt.datetime | dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":today:"`: 오늘 날짜 (기본값)
    end_date: dt.datetime | dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
            - `":now:"`: 현재 시각
    date_type: str
        일자 유형. 기본값은 수집일(`"reg_dm"`)
    order_seq: list[int]
        사방넷 주문 번호 목록
    order_status_div: str
        주문구분 코드
    order_status: list[str]
        주문상태 코드 목록
    shop_id: str
        쇼핑몰 ID 검색 조건
    sort_type: str
        정렬순서 코드. `"<정렬기준>_<asc|desc>"` 형식이며, 기본값은 `"ord_no_asc"`
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션. `download_type` 값이 옵션에 덮어쓰기로 추가된다.

    Returns
    -------
    DuckDBResult | dict[str, bytes] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 다운로드 후 `{파일명: 엑셀 바이너리}` 구조의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.

        **NOTE** `download_type`에 해당하는 DuckDB 테이블에 데이터를 적재하고 반환한다.
    """
    @with_duckdb_connection(tables={download_type: f"sabangnet_{download_type}"})
    def _download(*, connection: DuckDBConnection | None = None, return_type: str = "json"):
        from linkmerce.core.sabangnet.admin.order.extract import OrderDownload
        from linkmerce.core.sabangnet.admin.order.transform import OrderDownload as T
        from linkmerce.utils.nested import merge
        transform_options_ = merge(transform_options or dict(), {"download_type": download_type})
        return OrderDownload(**prepare_duckdb_extract(
            T, connection, extract_options, transform_options_, return_type,
            configs = _get_login_configs(userid, passwd, domain),
        )).extract(download_no, start_date, end_date, date_type, order_seq, order_status_div, order_status, shop_id, sort_type)
    return _download(connection=connection, return_type=return_type)


@with_duckdb_connection(table="sabangnet_order_status")
def order_status(
        userid: str,
        passwd: str,
        domain: int,
        download_no: int,
        start_date: dt.datetime | dt.date | str | Literal[":today:"] = ":today:",
        end_date: dt.datetime | dt.date | str | Literal[":start_date:", ":now:"] = ":start_date:",
        date_type: list[str] = ["delivery_confirm_date", "cancel_dt", "rtn_dt", "chng_dt"],
        order_seq: list[int] = list(),
        order_status_div: str = str(),
        order_status: list[str] = list(),
        shop_id: str = str(),
        sort_type: str = "ord_no_asc",
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict[str, bytes] | None:
    """사방넷 주문서확인처리 메뉴의 주문 상태 데이터를 일자 유형별로 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: sabangnet_order_status`

    Parameters
    ----------
    userid: str
        사방넷 아이디
    passwd: str
        사방넷 비밀번호
    domain: int
        사방넷 시스템 도메인 번호
    download_no: int
        주문서 출력 양식 번호
    start_date: dt.datetime | dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":today:"`: 오늘 날짜 (기본값)
    end_date: dt.datetime | dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
            - `":now:"`: 현재 시각
    date_type: list[str]
        일자 유형 목록. 기본값은 ["출고완료일", "취소완료일", "반품완료일", "교환완료일"]에 해당하는 코드 목록
    order_seq: list[int]
        사방넷 주문 번호 목록
    order_status_div: str
        주문구분 코드
    order_status: list[str]
        주문상태 코드 목록
    shop_id: str
        쇼핑몰 ID 검색 조건
    sort_type: str
        정렬순서 코드. `"<정렬기준>_<asc|desc>"` 형식이며, 기본값은 `"ord_no_asc"`
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        일자 유형별 요청 간 대기 시간(초). 기본값은 `1`
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
    DuckDBResult | dict[str, bytes] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 다운로드 후 `{일자 유형: 엑셀 바이너리}` 구조의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.sabangnet.admin.order.extract import OrderStatus
    from linkmerce.core.sabangnet.admin.order.transform import OrderStatus as T
    return OrderStatus(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_login_configs(userid, passwd, domain),
        options = {
            "PaginateAll": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(download_no, start_date, end_date, date_type, order_seq, order_status_div, order_status, shop_id, sort_type)


@with_duckdb_connection(table="sabangnet_product_mapping")
def product_mapping(
        userid: str,
        passwd: str,
        domain: int,
        start_date: dt.date | str | Literal[":base_date:", ":today:"] = ":base_date:",
        end_date: dt.date | str | Literal[":start_date:", ":today:"] = ":today:",
        shop_id: str = str(),
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | list[dict] | None:
    """사방넷 품번코드매핑관리 메뉴의 품번코드 매핑 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: sabangnet_product_mapping`

    Parameters
    ----------
    userid: str
        사방넷 아이디
    passwd: str
        사방넷 비밀번호
    domain: int
        사방넷 시스템 도메인 번호
    start_date: dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":base_date:"`: 사방넷 설립일 `1986-01-09` (기본값)
            - `":today:"`: 오늘 날짜
    end_date: dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜
            - `":today:"`: 오늘 날짜 (기본값)
    shop_id: str
        쇼핑몰 ID 검색 조건
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        페이지 요청 간 대기 시간(초). 기본값은 `1`
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
    DuckDBResult | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.sabangnet.admin.order.extract import ProductMapping
    from linkmerce.core.sabangnet.admin.order.transform import ProductMapping as T
    return ProductMapping(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_login_configs(userid, passwd, domain),
        options = {
            "PaginateAll": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(start_date, end_date, shop_id)


@with_duckdb_connection(table="sabangnet_sku_mapping")
def sku_mapping(
        userid: str,
        passwd: str,
        domain: int,
        query: dict | Iterable[dict],
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """사방넷 단품코드매핑관리 메뉴에서 단품코드 매핑 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: sabangnet_sku_mapping`

    Parameters
    ----------
    userid: str
        사방넷 아이디
    passwd: str
        사방넷 비밀번호
    domain: int
        사방넷 시스템 도메인 번호
    query: dict | Iterable[dict]
        조회할 단품상품 식별 정보 목록. 각 항목은 아래 키를 포함해야 한다.
            - `product_id_shop`: 쇼핑몰상품코드
            - `shop_id`: 쇼핑몰ID
            - `product_id`: 품번코드
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
    from linkmerce.core.sabangnet.admin.order.extract import SkuMapping
    from linkmerce.core.sabangnet.admin.order.transform import SkuMapping as T
    return SkuMapping(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_login_configs(userid, passwd, domain),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(query)


@with_duckdb_connection(tables={
    "product": "sabangnet_product_mapping",
    "sku": "sabangnet_sku_mapping",
})
def option_mapping(
        userid: str,
        passwd: str,
        domain: int,
        start_date: dt.date | str | Literal[":base_date:", ":today:"] = ":base_date:",
        end_date: dt.date | str | Literal[":start_date:", ":today:"] = ":today:",
        shop_id: str = str(),
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: tuple[dict | None, dict | None] = (None, None),
        transform_options: tuple[dict | None, dict | None] = (None, None),
    ) -> dict[str, DuckDBResult] | dict[str, object] | None:
    """사방넷에서 품번코드 매핑을 수집하고, 각각의 매핑에 대한 단품코드 매핑까지 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Tables** ( *table_key: table_name (description)* ):
        1. `product: sabangnet_product_mapping` (품번코드 매핑 내역)
        2. `sku: sabangnet_sku_mapping` (단품코드 매핑 내역)

    Parameters
    ----------
    userid: str
        사방넷 아이디
    passwd: str
        사방넷 비밀번호
    domain: int
        사방넷 시스템 도메인 번호
    start_date: dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":base_date:"`: 사방넷 설립일 `1986-01-09` (기본값)
            - `":today:"`: 오늘 날짜
    end_date: dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜
            - `":today:"`: 오늘 날짜 (기본값)
    shop_id: str
        쇼핑몰 ID 검색 조건
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        상품별 요청 간 대기 시간(초). 기본값은 `0.3`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: tuple[dict | None, dict | None]
        `Extractor` 초기화 옵션. `(ProductMapping, SkuMapping)` 순서로 튜플을 구성한다.
    transform_options: tuple[dict | None, dict | None]
        `Transformer` 초기화 옵션. `(ProductMapping, SkuMapping)` 순서로 튜플을 구성한다.

    Returns
    -------
    dict[str, DuckDBResult] | dict[str, object] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 모든 테이블 조회 결과를 `{table_key: list[tuple]}` 구조로 반환한다.
            - `"json"`: 모든 테이블 조회 결과를 `{table_key: list[dict]}` 구조로 반환한다. (기본값)
            - `"parquet"`: 모든 테이블 조회 결과를 `{table_key: Parquet 바이너리}` 구조로 반환한다.
            - `"raw"`: 중간 수집 결과를 `{table_key: 원본 응답}` 구조로 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    PRODUCT, SKU = 0, 1
    results = dict()

    results["product"] = product_mapping(
        userid, passwd, domain, start_date, end_date, shop_id,
        connection=connection, request_delay=request_delay, progress=progress, return_type=return_type,
        extract_options=extract_options[PRODUCT], transform_options=transform_options[PRODUCT])

    if return_type == "raw":
        from linkmerce.utils.nested import hier_get
        query = list()
        for result in results["product"]:
            for mapping in (hier_get(result, "data.list") or list()):
                if (mapping["mpngCnt"] or 0) == 0: continue
                query.append({
                    "product_id_shop": mapping["shmaPrdNo"],
                    "shop_id": mapping["shmaId"],
                    "product_id": mapping["prdNo"],
                })
    else:
        q = "SELECT DISTINCT product_id_shop, shop_id, product_id FROM sabangnet_product_mapping WHERE mapping_count > 0"
        query = connection.fetch_all_to_json(q)

    results["sku"] = sku_mapping(
        userid, passwd, domain, query,
        connection=connection, request_delay=request_delay, progress=progress, return_type=return_type,
        extract_options=extract_options[SKU], transform_options=transform_options[SKU])

    return results


@with_duckdb_connection(table="sabangnet_product")
def product(
        userid: str,
        passwd: str,
        domain: int,
        start_date: dt.date | str | Literal[":base_date:", ":today:"] = ":base_date:",
        end_date: dt.date | str | Literal[":start_date:", ":today:"] = ":today:",
        date_type: str = "001",
        sort_type: str = "001",
        sort_asc: bool = True,
        is_deleted: bool = False,
        product_status: str | None = None,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | list[dict] | None:
    """사방넷상품조회수정 메뉴의 상품 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: sabangnet_product`

    Parameters
    ----------
    userid: str
        사방넷 아이디
    passwd: str
        사방넷 비밀번호
    domain: int
        사방넷 시스템 도메인 번호
    start_date: dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":base_date:"`: 사방넷 설립일 `1986-01-09` (기본값)
            - `":today:"`: 오늘 날짜
    end_date: dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜
            - `":today:"`: 오늘 날짜 (기본값)
    date_type: str
        일자 유형 코드. 기본값은 상품등록일(`"001"`)
    sort_type: str
        정렬순서 코드. 기본값은 등록일(`"001"`)
    sort_asc: bool
        - `True`: 정렬순서 오름차순 (기본값)
        - `False`: 정렬순서 내림차순
    is_deleted: bool
        삭제된 상품 조회 여부
            - `True`: 삭제된 상품만 조회 (`product_status = "006"`)
            - `False`: `product_status` 조건에 맞는 상품 조회 (기본값)
    product_status: str | None
        상품상태 코드
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        페이지 요청 간 대기 시간(초). 기본값은 `1`
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
    DuckDBResult | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.sabangnet.admin.product.extract import Product
    from linkmerce.core.sabangnet.admin.product.transform import Product as T
    return Product(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_login_configs(userid, passwd, domain),
        options = {
            "PaginateAll": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(start_date, end_date, date_type, sort_type, sort_asc, is_deleted, product_status)


@with_duckdb_connection(table="sabangnet_option")
def option(
        userid: str,
        passwd: str,
        domain: int,
        product_id: str | Iterable[str],
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """사방넷상품조회수정 메뉴에서 상품별 옵션 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: sabangnet_option`

    Parameters
    ----------
    userid: str
        사방넷 아이디
    passwd: str
        사방넷 비밀번호
    domain: int
        사방넷 시스템 도메인 번호
    product_id: str | Iterable[str]
        조회할 사방넷 품번코드. 문자열 또는 문자열의 배열을 입력한다.
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
    from linkmerce.core.sabangnet.admin.product.extract import Option
    from linkmerce.core.sabangnet.admin.product.transform import Option as T
    return Option(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_login_configs(userid, passwd, domain),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(product_id)


@with_duckdb_connection(table="sabangnet_option_download")
def option_download(
        userid: str,
        passwd: str,
        domain: int,
        start_date: dt.date | str | Literal[":base_date:", ":today:"] = ":base_date:",
        end_date: dt.date | str | Literal[":start_date:", ":today:"] = ":today:",
        date_type: str = "prdFstRegsDt",
        sort_type: str = "prdNo",
        sort_asc: bool = True,
        is_deleted: bool = False,
        product_status: list[str] = list(),
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict[str, bytes] | None:
    """사방넷단품대량수정 메뉴에서 옵션 데이터를 다운로드해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: sabangnet_option_download`

    Parameters
    ----------
    userid: str
        사방넷 아이디
    passwd: str
        사방넷 비밀번호
    domain: int
        사방넷 시스템 도메인 번호
    start_date: dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":base_date:"`: 사방넷 설립일 `1986-01-09` (기본값)
            - `":today:"`: 오늘 날짜
    end_date: dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜
            - `":today:"`: 오늘 날짜 (기본값)
    date_type: str
        일자 유형 코드. 기본값은 상품등록일(`"prdFstRegsDt"`)
    sort_type: str
        정렬순서 코드. 기본값은 품번코드(`"prdNo"`)
    sort_asc: bool
        - `True`: 정렬순서 오름차순 (기본값)
        - `False`: 정렬순서 내림차순
    is_deleted: bool
        삭제된 상품 조회 여부
            - `True`: 삭제된 상품만 조회 (`product_status = "006"`)
            - `False`: `product_status` 조건에 맞는 상품 조회 (기본값)
    product_status: list[str]
        상품상태 코드 목록
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
    from linkmerce.core.sabangnet.admin.product.extract import OptionDownload
    from linkmerce.core.sabangnet.admin.product.transform import OptionDownload as T
    return OptionDownload(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_login_configs(userid, passwd, domain),
    )).extract(start_date, end_date, date_type, sort_type, sort_asc, is_deleted, product_status)


@with_duckdb_connection(table="sabangnet_add_product")
def add_product(
        userid: str,
        passwd: str,
        domain: int,
        group_id: str | Iterable[str] = list(),
        start_date: dt.date | str | Literal[":base_date:", ":today:"] = ":base_date:",
        end_date: dt.date | str | Literal[":start_date:", ":today:"] = ":today:",
        shop_id: str = str(),
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """사방넷추가상품관리 메뉴에서 추가상품 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: sabangnet_add_product`

    Parameters
    ----------
    userid: str
        사방넷 아이디
    passwd: str
        사방넷 비밀번호
    domain: int
        사방넷 시스템 도메인 번호
    group_id: str | Iterable[str]
        조회할 추가상품 그룹 코드. 생략하면 기간 내 모든 추가상품 그룹을 수집한다.   
        **NOTE** 생략하여 추가상품 그룹을 수집할 경우 `sabangnet_add_product_group` 테이블이 추가된다.
    start_date: dt.date | str
        추가상품 그룹 조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":base_date:"`: 사방넷 설립일 `1986-01-09` (기본값)
            - `":today:"`: 오늘 날짜
    end_date: dt.date | str
        추가상품 그룹 조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜
            - `":today:"`: 오늘 날짜 (기본값)
    shop_id: str
        쇼핑몰 ID 검색 조건. 추가상품 그룹을 조회할 때 조건으로 사용된다.
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        추가상품 그룹별 요청 간 대기 시간(초). 기본값은 `1`
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
    if not group_id:
        from linkmerce.core.sabangnet.admin.product.extract import AddProductGroup
        from linkmerce.core.sabangnet.admin.product.transform import AddProductGroup as T

        groups = AddProductGroup(**prepare_duckdb_extract(
            T, connection, return_type="json",
            configs = _get_login_configs(userid, passwd, domain),
            options = {
                "PaginateAll": {
                    "request_delay": request_delay,
                    "tqdm_options": {"disable": (not progress)}
                }
            },
        )).extract(start_date, end_date, shop_id)

        if return_type == "raw":
            from linkmerce.utils.nested import hier_get
            ids = [group["addPrdGrpId"] for result in groups for group in (hier_get(result, "data") or list())]
            group_id = list(dict.fromkeys(ids))
        else:
            query = "SELECT group_id FROM sabangnet_add_product_group"
            group_id = [row[0] for row in connection.fetch_all_to_csv(query, header=False)]

    from linkmerce.core.sabangnet.admin.product.extract import AddProduct
    from linkmerce.core.sabangnet.admin.product.transform import AddProduct as T

    return AddProduct(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_login_configs(userid, passwd, domain),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(group_id)
