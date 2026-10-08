from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import datetime as dt
    from typing import Iterable, Literal
    from linkmerce.api.common import DuckDBResult
    from linkmerce.common.load import DuckDBConnection


def _get_api_configs(access_key: str, secret_key: str, vendor_id: str) -> dict:
    """쿠팡 Open API 인증에 필요한 설정을 구성한다."""
    return {"access_key": access_key, "secret_key": secret_key, "vendor_id": vendor_id}


def request(
        access_key: str,
        secret_key: str,
        vendor_id: str,
        method: str,
        path: str,
        params: dict | list[tuple] | bytes | None = None,
        data: dict | list[tuple] | bytes | None = None,
        json: dict | None = None,
        headers: dict[str, str] = None,
        extract_options: dict = dict(),
    ) -> dict:
    """쿠팡 Open API에 임의의 HTTP 요청을 보내 JSON 응답을 반환한다.

    Parameters
    ----------
    access_key: str
        쿠팡 Open API 액세스 키
    secret_key: str
        쿠팡 Open API 시크릿 키
    vendor_id: str
        업체 코드
    method: str
        HTTP 메서드
    path: str
        쿠팡 Open API 경로
    params: dict | list[tuple] | bytes | None
        쿠팡 Open API 요청 파라미터
    data: dict | list[tuple] | bytes | None
        쿠팡 Open API 요청 본문
    json: dict | None
        쿠팡 Open API 요청 본문 (JSON)
    headers: dict[str, str]
        쿠팡 Open API 요청 헤더
    extract_options: dict
        `CoupangTestAPI` 초기화 옵션

    Returns
    -------
    dict
        쿠팡 Open API 응답 결과
    """
    from linkmerce.core.coupang.api.common import CoupangTestApi
    from linkmerce.utils.nested import merge
    extractor = CoupangTestApi(**merge(
        extract_options or dict(),
        configs = _get_api_configs(access_key, secret_key, vendor_id),
    ))
    return extractor.extract(method, path, params, data, json, headers)


@with_duckdb_connection(table="coupang_product")
def product(
        access_key: str,
        secret_key: str,
        vendor_id: str,
        business_type: Literal["rocketGrowth"] | None = None,
        seller_product_id: int | str | None = None,
        seller_product_name: str | None = None,
        status: Literal["IN_REVIEW", "SAVED", "APPROVING", "APPROVED", "PARTIAL_APPROVED", "DENIED", "DELETED"] | None = None,
        manufacture: str | None = None,
        created_at: dt.date | str | None = None,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int | tuple[int, int] = 0.3,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | list[dict] | None:
    """쿠팡 마켓플레이스 또는 로켓그로스 상품 목록을 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: coupang_product`

    Parameters
    ----------
    access_key: str
        쿠팡 Open API 액세스 키
    secret_key: str
        쿠팡 Open API 시크릿 키
    vendor_id: str
        업체 코드
    business_type: str | None
        - `"rocketGrowth"`: 로켓그로스 상품 또는 마켓플레이스 및 로켓그로스 아이템이 모두 존재하는 Hybrid 상품
        - `None`: 마켓플레이스 상품 또는 마켓플레이스 및 로켓그로스 아이템이 모두 존재하는 Hybrid 상품 (기본값)
    seller_product_id: int | str
        조회할 노출옵션ID를 선택할 수 있다.
    seller_product_name: str
        조회할 상품명을 입력할 수 있다.
    status: str
        업체상품상태. `status` 속성의 키를 전달할 수 있다.
    manufacture: str
        조회할 제조사를 입력할 수 있다.
    created_at: dt.date | str
        상품등록일시를 제한할 수 있다.   
        예) '2015-12-17'과 같이 입력하면, '2015-12-17T00:00:00' ~ '2015-12-17T23:59:59'와 같이 조회됨
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        커서 요청 간 대기 시간(초). 기본값은 `0.3`
    return_type: Literal["csv", "json", "parquet", "raw", "none"]
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
            - `"raw"`: 데이터 수집 후 `dict` 또는 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.coupang.api.product.extract import Product
    from linkmerce.core.coupang.api.product.transform import Product as T
    return Product(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(access_key, secret_key, vendor_id),
        options = {"CursorAll": {"request_delay": request_delay}},
    )).extract(business_type, seller_product_id, seller_product_name, status, manufacture, created_at)


@with_duckdb_connection(table="coupang_product_detail")
def product_detail(
        access_key: str,
        secret_key: str,
        vendor_id: str,
        seller_product_id: int | str | Iterable[int | str],
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int | tuple[int, int] = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """쿠팡 마켓플레이스 또는 로켓그로스 상품의 상세 정보를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: coupang_product_detail`

    Parameters
    ----------
    access_key: str
        쿠팡 Open API 액세스 키
    secret_key: str
        쿠팡 Open API 시크릿 키
    vendor_id: str
        업체 코드
    seller_product_id: int | str | Iterable[int | str]
        등록상품ID. 단일 값 또는 배열
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        상품별 요청 간 대기 시간(초). 기본값은 `0.3`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: Literal["csv", "json", "parquet", "raw", "none"]
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
    from linkmerce.core.coupang.api.product.extract import ProductDetail
    from linkmerce.core.coupang.api.product.transform import ProductDetail as T
    return ProductDetail(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(access_key, secret_key, vendor_id),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(seller_product_id)


@with_duckdb_connection(table="coupang_product_option")
def product_option(
        access_key: str,
        secret_key: str,
        vendor_id: str,
        business_types: Iterable[Literal["rocketGrowth"] | None] = [None, "rocketGrowth"],
        seller_product_id: int | str | None = None,
        seller_product_name: str | None = None,
        statuses: Iterable[Literal["IN_REVIEW", "SAVED", "APPROVING", "APPROVED", "PARTIAL_APPROVED", "DENIED", "DELETED"] | None] = [None, "DELETED"],
        manufacture: str | None = None,
        created_at: dt.date | str | None = None,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int | tuple[int, int] = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: tuple[dict | None, dict | None] = (None, None),
        transform_options: tuple[dict | None, dict | None] = (None, None),
        merged_table: str | None = None,
    ) -> DuckDBResult | list[dict] | dict[str, list[dict]] | None:
    """쿠팡 마켓플레이스 또는 로켓그로스 상품 목록에서 옵션 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: coupang_product_option`

    Parameters
    ----------
    access_key: str
        쿠팡 Open API 액세스 키
    secret_key: str
        쿠팡 Open API 시크릿 키
    vendor_id: str
        업체 코드
    business_types: Iterable[str | None]
        - `"rocketGrowth"`: 로켓그로스 상품 또는 마켓플레이스 및 로켓그로스 아이템이 모두 존재하는 Hybrid 상품
        - `None`: 마켓플레이스 상품 또는 마켓플레이스 및 로켓그로스 아이템이 모두 존재하는 Hybrid 상품 (기본값)
    seller_product_id: int | str
        조회할 노출옵션ID를 선택할 수 있다.
    seller_product_name: str
        조회할 상품명을 입력할 수 있다.
    statuses: Iterable[str | None]
        업체상품상태. `status` 속성의 키를 목록으로 전달할 수 있다.
    manufacture: str
        조회할 제조사를 입력할 수 있다.
    created_at: dt.date | str
        상품등록일시를 제한할 수 있다.   
        예) '2015-12-17'과 같이 입력하면, '2015-12-17T00:00:00' ~ '2015-12-17T23:59:59'와 같이 조회됨
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        커서 및 상품별 요청 간 대기 시간(초). 기본값은 `0.3`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: tuple[dict | None, dict | None]
        `Extractor` 초기화 옵션. `(Product, ProductDetail)` 순서로 튜플을 구성한다.
    transform_options: tuple[dict | None, dict | None]
        `Transformer` 초기화 옵션. `(ProductItem, ProductDetail)` 순서로 튜플을 구성한다.
    merged_table: str | None
        상품과 옵션 병합 결과를 적재할 테이블 명칭. 생략하면 `"coupang_product_option"` 테이블을 생성한다.

    Returns
    -------
    DuckDBResult | list[dict] | dict[str, list[dict]] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `{"product": ProductItem, "option": ProductDetail}` 구조의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.coupang.api.product.extract import Product
    from linkmerce.core.coupang.api.product.transform import ProductItem as T1
    from linkmerce.api.common import get_table
    PRODUCT, OPTION = 0, 1
    product_table = get_table(transform_options[PRODUCT], default="coupang_product_item")
    option_table = get_table(transform_options[OPTION], default="coupang_product_detail")

    products = list()
    for business_type in business_types:
        for status in statuses:
            products += Product(**prepare_duckdb_extract(
                T1, connection, extract_options[PRODUCT], transform_options[PRODUCT], return_type,
                configs = _get_api_configs(access_key, secret_key, vendor_id),
                options = {"CursorAll": {"request_delay": request_delay}},
            )).extract(business_type, seller_product_id, seller_product_name, status, manufacture, created_at)

    from linkmerce.core.coupang.api.product.extract import ProductDetail
    from linkmerce.core.coupang.api.product.transform import ProductDetail as T2

    options = ProductDetail(**prepare_duckdb_extract(
        T2, connection, extract_options[OPTION], transform_options[OPTION], return_type,
        configs = _get_api_configs(access_key, secret_key, vendor_id),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(connection.fetch_values(f"SELECT DISTINCT vendor_inventory_id FROM {product_table}", axis=1))

    if return_type == "raw":
        return {"product": products, "option": options}

    table = merged_table or "coupang_product_option"

    from textwrap import dedent
    connection.execute(
        dedent(f"""CREATE OR REPLACE TABLE {table} AS
        SELECT
            O.vendor_inventory_id,
            O.vendor_inventory_item_id,
            COALESCE(O.product_id, P.product_id) AS product_id,
            O.option_id,
            O.item_id,
            O.vendor_id,
            O.business_type,
            O.seller_product_name,
            O.display_product_name,
            O.option_name,
            O.display_category_id,
            COALESCE(O.category_id, P.category_id) AS category_id,
            O.barcode,
            COALESCE(O.brand_name, P.brand_name) AS brand_name,
            O.maker_name,
            O.image_url,
            COALESCE(NULLIF(O.product_status, 9), P.product_status) AS product_status,
            O.price,
            O.sales_price,
            O.delivery_fee,
            O.sales_started_at,
            O.sales_ended_at,
            P.created_at
        FROM {option_table} AS O
        LEFT JOIN (
            SELECT * FROM {product_table}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY vendor_inventory_id) = 1
        ) AS P
            ON O.vendor_inventory_id = P.vendor_inventory_id
        """))

    connection.execute(
        dedent(f"""INSERT INTO {table}
        SELECT
            P.vendor_inventory_id,
            P.vendor_inventory_item_id,
            COALESCE(P.product_id, O.product_id) AS product_id,
            P.option_id,
            CAST(NULL AS BIGINT) AS item_id,
            P.vendor_id,
            P.business_type,
            P.seller_product_name,
            CAST(NULL AS VARCHAR) AS display_product_name,
            P.option_name,
            P.display_category_id,
            COALESCE(P.category_id, O.category_id) AS category_id,
            CAST(NULL AS VARCHAR) AS barcode,
            COALESCE(P.brand_name, O.brand_name) AS brand_name,
            O.maker_name,
            CAST(NULL AS VARCHAR) AS image_url,
            COALESCE(NULLIF(P.product_status, 9), O.product_status) AS product_status,
            CAST(NULL AS INTEGER) AS price,
            CAST(NULL AS INTEGER) AS sales_price,
            O.delivery_fee,
            P.sales_started_at,
            P.sales_ended_at,
            P.created_at
        FROM (
            SELECT * FROM {product_table}
            WHERE vendor_inventory_item_id != 0
        ) AS P
        LEFT JOIN (
            SELECT * FROM {option_table}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY vendor_inventory_id) = 1
        ) AS O
            ON P.vendor_inventory_id = O.vendor_inventory_id
        WHERE NOT EXISTS (
            SELECT 1
            FROM {table} AS T
            WHERE T.vendor_inventory_id = P.vendor_inventory_id
                AND T.vendor_inventory_item_id = P.vendor_inventory_item_id
        )
        """))


@with_duckdb_connection(table="coupang_inventory")
def inventory(
        access_key: str,
        secret_key: str,
        vendor_id: str,
        vendor_item_id: int | str | Iterable[int | str],
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int | tuple[int, int] = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """쿠팡 옵션별 수량, 가격, 판매상태 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: coupang_inventory`

    Parameters
    ----------
    access_key: str
        쿠팡 Open API 액세스 키
    secret_key: str
        쿠팡 Open API 시크릿 키
    vendor_id: str
        업체 코드
    vendor_item_id: int | str | Iterable[int | str]
        노출옵션ID. 단일 값 또는 배열
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        옵션별 요청 간 대기 시간(초). 기본값은 `0.3`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: Literal["csv", "json", "parquet", "raw", "none"]
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
    from linkmerce.core.coupang.api.product.extract import Inventory
    from linkmerce.core.coupang.api.product.transform import Inventory as T
    return Inventory(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(access_key, secret_key, vendor_id),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(vendor_item_id)


@with_duckdb_connection(tables = {
    "order": "coupang_order",
    "delivery": "coupang_order_delivery",
    "detail": "coupang_order_detail",
    "item": "coupang_order_item",
})
def order(
        access_key: str,
        secret_key: str,
        vendor_id: str,
        start_datetime: dt.datetime | str,
        end_datetime: dt.datetime | str | Literal[":end_of_day:", ":max_window:"] = ":end_of_day:",
        status: Literal["ALL", "ACCEPT", "INSTRUCT", "DEPARTURE", "DELIVERING", "FINAL_DELIVERY", "NONE_TRACKING"] | Iterable[str] = "ALL",
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int | tuple[int, int] = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str, DuckDBResult] | dict | list[dict] | None:
    """쿠팡 마켓플레이스 발주서 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    - **Tables** ( *table_key: table_name (description)* ):
        1. `order: coupang_order` (발주서 정보)
        2. `delivery: coupang_order_delivery` (발주서 배송 정보)
        3. `detail: coupang_order_detail` (발주서 결제 정보)
        4. `item: coupang_order_item` (발주서 상품 정보)

    Parameters
    ----------
    access_key: str
        쿠팡 Open API 액세스 키
    secret_key: str
        쿠팡 Open API 시크릿 키
    vendor_id: str
        업체 코드
    start_datetime: dt.datetime | str
        검색 시작 일시. `dt.datetime` 객체 또는 "yyyy-mm-ddT00:00+09:00" 형태의 문자열을 입력한다.
    end_datetime: dt.datetime | str
        검색 종료 일시. `dt.datetime` 객체 또는 "yyyy-mm-ddT00:00+09:00" 형태의 문자열을 입력한다.
            - `":end_of_day:"`: `start_datetime`의 하루 중 마지막 시점 (기본값)
            - `":max_window:"`: `start_datetime`으로부터 23시간 59분이 지난 시점
    status: str | Iterable[str]
        발주서 상태. `"ALL"` 값으로 모든 상태를 조회하거나, 개별 상태 코드를 단일 값 또는 배열로 입력할 수 있다.
            - `"ACCEPT"`: 결제완료
            - `"INSTRUCT"`: 상품준비중
            - `"DEPARTURE"`: 배송지시
            - `"DELIVERING"`: 배송중
            - `"FINAL_DELIVERY"`: 배송완료
            - `"NONE_TRACKING"`: 업체 직접 배송
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        검색 기간 및 발주서 상태별 요청 간 대기 시간(초). 기본값은 `0.3`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: Literal["csv", "json", "parquet", "raw", "none"]
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    dict[str, DuckDBResult] | dict | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `dict[str, list[tuple]]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `dict[str, list[dict]]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 `dict[str, bytes]` Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 또는 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.coupang.api.order.extract import Order
    from linkmerce.core.coupang.api.order.transform import Order as T
    return Order(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(access_key, secret_key, vendor_id),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(start_datetime, end_datetime, status)


@with_duckdb_connection(tables = {
    "order": "coupang_order",
    "delivery": "coupang_order_delivery",
    "detail": "coupang_order_detail",
    "item": "coupang_order_item",
})
def order_detail(
        access_key: str,
        secret_key: str,
        vendor_id: str,
        query_id: int | str | Iterable[int | str],
        query_type: Literal["order_id", "shipment_box_id"] = "order_id",
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int | tuple[int, int] = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str, DuckDBResult] | dict | list[dict] | None:
    """쿠팡 주문번호별 마켓플레이스 발주서 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    - **Tables** ( *table_key: table_name (description)* ):
        1. `order: coupang_order` (발주서 정보)
        2. `delivery: coupang_order_delivery` (발주서 배송 정보)
        3. `detail: coupang_order_detail` (발주서 결제 정보)
        4. `item: coupang_order_item` (발주서 상품 정보)

    Parameters
    ----------
    access_key: str
        쿠팡 Open API 액세스 키
    secret_key: str
        쿠팡 Open API 시크릿 키
    vendor_id: str
        업체 코드
    query_id: int | str | Iterable[int | str]
        주문번호 또는 배송번호. 단일 값 또는 배열
    query_type: str
        번호 유형
            - `"order_id"`: 주문번호 (기본값)
            - `"shipment_box_id"`: 배송번호
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간(초). 기본값은 `0.3`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: Literal["csv", "json", "parquet", "raw", "none"]
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    dict[str, DuckDBResult] | dict | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `dict[str, list[tuple]]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `dict[str, list[dict]]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 `dict[str, bytes]` Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 또는 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.coupang.api.order.extract import OrderDetail
    from linkmerce.core.coupang.api.order.transform import OrderDetail as T
    return OrderDetail(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(access_key, secret_key, vendor_id),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(query_id, query_type)


@with_duckdb_connection(table="coupang_rocket_inventory")
def rocket_inventory(
        access_key: str,
        secret_key: str,
        vendor_id: str,
        vendor_item_id: int | str | None = None,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int | tuple[int, int] = 1.3,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | list[dict] | None:
    """쿠팡 로켓창고의 재고 요약 목록을 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: coupang_rocket_inventory`

    Parameters
    ----------
    access_key: str
        쿠팡 Open API 액세스 키
    secret_key: str
        쿠팡 Open API 시크릿 키
    vendor_id: str
        업체 코드
    vendor_item_id: int | str | None
        노출옵션ID. 생략하면 전체 옵션 조회
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간(초). 기본값은 `1.3`
    progress: bool
        반복 요청 작업의 진행률 출력 여부. 기본값은 `True`
    return_type: Literal["csv", "json", "parquet", "raw", "none"]
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
            - `"raw"`: 데이터 수집 후 `dict` 또는 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.coupang.api.rocket_growth.extract import Inventory
    from linkmerce.core.coupang.api.rocket_growth.transform import Inventory as T
    return Inventory(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(access_key, secret_key, vendor_id),
        options = {"CursorAll": {"request_delay": request_delay}},
    )).extract(vendor_item_id)


@with_duckdb_connection(table="coupang_rocket_order")
def rocket_order(
        access_key: str,
        secret_key: str,
        vendor_id: str,
        start_date: dt.date | str, 
        end_date: dt.date | str | Literal[":next_start_date:"] = ":next_start_date:",
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int | tuple[int, int] = 1.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | list[dict] | None:
    """쿠팡 로켓그로스 주문 목록을 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: coupang_rocket_order`

    Parameters
    ----------
    access_key: str
        쿠팡 Open API 액세스 키
    secret_key: str
        쿠팡 Open API 시크릿 키
    vendor_id: str
        업체 코드
    start_date: dt.date | str
        검색 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
    end_date: dt.date | str
        검색 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":next_start_date:"`: `start_date`의 1일 후 날짜 (기본값)
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간(초). 기본값은 `1.3`
    progress: bool
        반복 요청 작업의 진행률 출력 여부. 기본값은 `True`
    return_type: Literal["csv", "json", "parquet", "raw", "none"]
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
            - `"raw"`: 데이터 수집 후 `dict` 또는 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.coupang.api.rocket_growth.extract import Order
    from linkmerce.core.coupang.api.rocket_growth.transform import Order as T
    return Order(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(access_key, secret_key, vendor_id),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(start_date, end_date)
