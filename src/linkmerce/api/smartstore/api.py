from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal, Sequence
    from linkmerce.api.common import DuckDBResult
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


def _get_api_configs(client_id: str, client_secret: str) -> dict:
    """커머스 API 인증에 필요한 설정을 구성한다."""
    return {"client_id": client_id, "client_secret": client_secret}


def request(
        client_id: str,
        client_secret: str,
        method: str,
        path: str,
        version: str | None = None,
        params: dict | list[tuple] | bytes | None = None,
        data: dict | list[tuple] | bytes | None = None,
        json: JsonObject | None = None,
        headers: dict[str, str] = None,
        extract_options: dict = dict(),
    ) -> dict:
    """네이버 커머스 API에 임의의 HTTP 요청을 보내 JSON 응답을 반환한다.

    Parameters
    ----------
    client_id: str
        커머스 API 애플리케이션 ID
    client_secret: str
        커머스 API 애플리케이션 시크릿
    method: str
        HTTP 메서드
    path: str
        커머스 API 경로
    version: str | None
        커머스 API 버전
    params: dict | list[tuple] | bytes | None
        커머스 API 요청 파라미터
    data: dict | list[tuple] | bytes | None
        커머스 API 요청 본문
    json: JsonObject | None
        커머스 API 요청 본문 (JSON)
    headers: dict[str, str]
        커머스 API 요청 헤더

    Returns
    -------
    dict
        커머스 API 응답 결과
    """
    from linkmerce.core.smartstore.api.common import SmartstoreTestAPI
    from linkmerce.utils.nested import merge
    extractor = SmartstoreTestAPI(**merge(
        extract_options or dict(),
        configs = _get_api_configs(client_id, client_secret),
    ))
    return extractor.extract(method, path, version, params, data, json, headers)


@with_duckdb_connection(table="smartstore_product")
def product(
        client_id: str,
        client_secret: str,
        channel_seq: int | str,
        search_keyword: Sequence[int] = list(),
        keyword_type: Literal["CHANNEL_PRODUCT_NO", "PRODUCT_NO", "GROUP_PRODUCT_NO"] = "CHANNEL_PRODUCT_NO",
        status_type: Sequence[
            Literal["ALL", "WAIT", "SALE", "OUTOFSTOCK", "UNADMISSION", "REJECTION",
                    "SUSPENSION", "CLOSE", "PROHIBITION"]] = ["SALE"],
        period_type: Literal["PROD_REG_DAY", "SALE_START_DAY", "SALE_END_DAY", "PROD_MOD_DAY"] = "PROD_REG_DAY",
        from_date: dt.date | str | None = None,
        to_date: dt.date | str | None = None,
        max_retries: int = 5,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | list[dict] | None:
    """네이버 스마트스토어 상품 목록 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: smartstore_product`

    Parameters
    ----------
    client_id: str
        커머스 API 애플리케이션 ID
    client_secret: str
        커머스 API 애플리케이션 시크릿
    channel_seq: int | str
        채널 번호
    search_keyword: Sequence[int]
        검색 키워드
            - 채널 상품번호(`"CHANNEL_PRODUCT_NO"`) 선택 시 채널 상품번호를 입력한다.
            - 원상품번호(`"PRODUCT_NO"`) 선택 시 채널 원상품번호를 입력한다.
            - 그룹상품번호(`"GROUP_PRODUCT_NO"`) 선택 시 그룹상품번호를 입력한다.
            - 판매자 관리 코드(`"SELLER_CODE"`) 선택 시 판매자 관리 코드를 입력한다.
    keyword_type: str
        검색 키워드 타입. 기본값은 채널 상품번호(`"CHANNEL_PRODUCT_NO"`)
    status_type: Sequence[str]
        상품 판매 상태 목록
            - `"WAIT"`: 판매 대기
            - `"SALE"`: 판매 중
            - `"OUTOFSTOCK"`: 품절
            - `"UNADMISSION"`: 승인 대기
            - `"REJECTION"`: 승인 거부
            - `"SUSPENSION"`: 판매 중지
            - `"CLOSE"`: 판매 종료
            - `"PROHIBITION"`: 판매 금지
    period_type: str
        검색 기간 유형
            - `"PROD_REG_DAY"`: 상품 등록일
            - `"SALE_START_DAY"`: 판매 시작일
            - `"SALE_END_DAY"`: 판매 종료일
            - `"PROD_MOD_DAY"`: 최종 수정일
    from_date: dt.date | str | None
        검색 기간 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
    to_date: dt.date | str | None
        검색 기간 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
    max_retries: int
        동시 요청 제한이 발생할 경우 최대 재시도 횟수. 기본값은 `5`
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
    from linkmerce.core.smartstore.api.product.extract import Product
    from linkmerce.core.smartstore.api.product.transform import Product as T
    return Product(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(client_id, client_secret),
        options = {
            "PaginateAll": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(search_keyword, keyword_type, status_type, period_type, from_date, to_date, channel_seq, max_retries)


@with_duckdb_connection(table="smartstore_option")
def option(
        client_id: str,
        client_secret: str,
        channel_seq: int | str,
        product_id: int | str | Sequence[int | str],
        max_retries: int = 5,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """네이버 스마트스토어 상품 옵션 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: smartstore_option`

    Parameters
    ----------
    client_id: str
        커머스 API 애플리케이션 ID
    client_secret: str
        커머스 API 애플리케이션 시크릿
    channel_seq: int | str
        채널 번호
    product_id: int | str | Sequence[int | str]
        상품코드 목록
    max_retries: int
        동시 요청 제한이 발생할 경우 최대 재시도 횟수. 기본값은 `5`
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        상품별 요청 간 대기 시간(초). 기본값은 `1`
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
    from linkmerce.core.smartstore.api.product.extract import Option
    from linkmerce.core.smartstore.api.product.transform import Option as T
    return Option(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(client_id, client_secret),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(product_id, channel_seq, max_retries)


@with_duckdb_connection(tables={
    "product": "smartstore_product",
    "option": "smartstore_option",
    "merged": "smartstore_product_option",
})
def product_option(
        client_id: str,
        client_secret: str,
        channel_seq: int | str,
        search_keyword: Sequence[int] = list(),
        keyword_type: Literal["CHANNEL_PRODUCT_NO", "PRODUCT_NO", "GROUP_PRODUCT_NO"] = "CHANNEL_PRODUCT_NO",
        status_type: Sequence[Literal["ALL", "WAIT", "SALE", "OUTOFSTOCK", "UNADMISSION", "REJECTION", "SUSPENSION", "CLOSE", "PROHIBITION"]] = ["SALE"],
        period_type: Literal["PROD_REG_DAY", "SALE_START_DAY", "SALE_END_DAY", "PROD_MOD_DAY"] = "PROD_REG_DAY",
        from_date: dt.date | str | None = None,
        to_date: dt.date | str | None = None,
        max_retries: int = 5,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: tuple[dict | None, dict | None] = (None, None),
        transform_options: tuple[dict | None, dict | None] = (None, None),
        merged_table: str | None = None,
    ) -> dict[str, DuckDBResult] | dict[str, dict | list[dict]] | None:
    """네이버 스마트스토어 상품 및 옵션 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Tables** ( *table_key: table_name (description)* ):
        1. `product: smartstore_product` (상품 목록)
        2. `option: smartstore_option` (옵션 목록)
        3. `merged: smartstore_product_option` (상품-옵션 병합 결과)

    Parameters
    ----------
    client_id: str
        커머스 API 애플리케이션 ID
    client_secret: str
        커머스 API 애플리케이션 시크릿
    channel_seq: int | str
        채널 번호
    search_keyword: Sequence[int]
        검색 키워드
            - 채널 상품번호(`"CHANNEL_PRODUCT_NO"`) 선택 시 채널 상품번호를 입력한다.
            - 원상품번호(`"PRODUCT_NO"`) 선택 시 채널 원상품번호를 입력한다.
            - 그룹상품번호(`"GROUP_PRODUCT_NO"`) 선택 시 그룹상품번호를 입력한다.
            - 판매자 관리 코드(`"SELLER_CODE"`) 선택 시 판매자 관리 코드를 입력한다.
    keyword_type: str
        검색 키워드 타입. 기본값은 채널 상품번호(`"CHANNEL_PRODUCT_NO"`)
    status_type: Sequence[str]
        상품 판매 상태 목록
            - `"WAIT"`: 판매 대기
            - `"SALE"`: 판매 중
            - `"OUTOFSTOCK"`: 품절
            - `"UNADMISSION"`: 승인 대기
            - `"REJECTION"`: 승인 거부
            - `"SUSPENSION"`: 판매 중지
            - `"CLOSE"`: 판매 종료
            - `"PROHIBITION"`: 판매 금지
    period_type: str
        검색 기간 유형
            - `"PROD_REG_DAY"`: 상품 등록일
            - `"SALE_START_DAY"`: 판매 시작일
            - `"SALE_END_DAY"`: 판매 종료일
            - `"PROD_MOD_DAY"`: 최종 수정일
    from_date: dt.date | str | None
        검색 기간 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
    to_date: dt.date | str | None
        검색 기간 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력할 수 있다.
    max_retries: int
        동시 요청 제한이 발생할 경우 최대 재시도 횟수. 기본값은 `5`
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        요청 간 대기 시간(초). 기본값은 `1`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: tuple[dict | None, dict | None]
        `Extractor` 초기화 옵션. `(Product, Option)` 순서로 튜플을 구성한다.
    transform_options: tuple[dict | None, dict | None]
        `Transformer` 초기화 옵션. `(Product, Option)` 순서로 튜플을 구성한다.   
        실행 결과의 `table_key`에 해당하는 테이블은 아래와 같이 각 `Transformer.table_key`와 매칭된다.
            - `product` - `Product.table`
            - `option` - `Option.table`
    merged_table: str | None
        상품-옵션 병합 결과를 적재할 테이블 명칭. 생략하면 `"smartstore_product_option"` 테이블을 생성한다.

    Returns
    -------
    dict[str, DuckDBResult] | dict[str, dict | list[dict]] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 모든 테이블 조회 결과를 `{table_key: list[tuple]}` 구조로 반환한다.
            - `"json"`: 모든 테이블 조회 결과를 `{table_key: list[dict]}` 구조로 반환한다. (기본값)
            - `"parquet"`: 모든 테이블 조회 결과를 `{table_key: Parquet 바이너리}` 구조로 반환한다.
            - `"raw"`: 데이터 수집 후 `{"product": Product, "option": Option}` 구조의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.api.common import get_table
    PRODUCT, OPTION = 0, 1
    product_table = get_table(transform_options[PRODUCT], default="smartstore_product")
    option_table = get_table(transform_options[OPTION], default="smartstore_option")

    results = dict()
    return_type = return_type if return_type == "raw" else "none"

    args = (search_keyword, keyword_type, status_type, period_type, from_date, to_date, channel_seq, max_retries)
    results["product"] = product(
        client_id, client_secret, *args,
        connection=connection, request_delay=request_delay, progress=progress, return_type=return_type,
        extract_options=extract_options[PRODUCT], transform_options=transform_options[PRODUCT])

    if return_type == "raw":
        ids = [product["channelProductNo"] for result in results["product"] for product in result["contents"]]
        product_id = list(dict.fromkeys(ids))
    else:
        query = f"SELECT DISTINCT product_id FROM {product_table} WHERE channel_seq = {channel_seq}"
        product_id = [row[0] for row in connection.execute(query)[0].fetchall()]

    results["option"] = option(
        client_id, client_secret, product_id, channel_seq, max_retries,
        connection=connection, request_delay=request_delay, progress=progress, return_type=return_type,
        extract_options=extract_options[OPTION], transform_options=transform_options[OPTION])

    if return_type == "raw":
        return results

    table = merged_table or "smartstore_product_option"
    keyword = f"INSERT INTO {table}" if connection.table_exists(table) else f"CREATE TABLE {table} AS"

    from textwrap import dedent
    connection.execute(
        dedent(f"""{keyword}
        SELECT
            L.product_id,
            COALESCE(R.option_id, L.product_id) AS option_id,
            L.channel_seq,
            ((CASE WHEN R.product_type = 0 THEN '옵션상품(단독형)'
                WHEN R.product_type = 1 THEN '옵션상품(조합형)'
                WHEN R.product_type = 2 THEN '추가상품'
                ELSE '단품상품' END) AS product_type),
            L.product_name,
            R.register_order,
            R.option_group_1,
            R.option_group_2,
            R.option_group_3,
            R.option_name_1,
            R.option_name_2,
            R.option_name_3,
            L.management_code AS seller_product_code,
            R.management_code AS seller_option_code,
            L.model_name,
            L.brand_name,
            L.category_id,
            ((CASE WHEN L.status_type = 'WAIT' THEN '판매대기'
                WHEN L.status_type = 'SALE' THEN '판매중'
                WHEN L.status_type = 'OUTOFSTOCK' THEN '품절'
                WHEN L.status_type = 'UNADMISSION' THEN '승인대기'
                WHEN L.status_type = 'REJECTION' THEN '승인거부'
                WHEN L.status_type = 'SUSPENSION' THEN '판매중지'
                WHEN L.status_type = 'CLOSE' THEN '판매종료'
                WHEN L.status_type = 'PROHIBITION' THEN '판매금지'
                ELSE NULL END) AS product_status),
            ((CASE WHEN L.display_type = 'WAIT' THEN '전시대기',
                WHEN L.display_type = 'ON' THEN '전시중'
                WHEN L.display_type = 'SUSPENSION' THEN '전시중지'
                ELSE NULL END) AS display_type),
            IF(R.usable, '사용', '사용안함') AS option_status,
            L.price,
            L.sales_price,
            L.delivery_fee,
            R.option_price,
            R.stock_quantity,
            L.register_dt,
            L.modify_dt
        FROM {product_table} AS L
        LEFT JOIN {option_table} AS R
            ON L.product_id = R.product_id
        """))

    return results


@with_duckdb_connection(tables={
    "order": "smartstore_order",
    "product_order": "smartstore_product_order",
    "delivery": "smartstore_delivery",
    "option": "smartstore_option",
})
def order(
        client_id: str,
        client_secret: str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        range_type: str = "PAYED_DATETIME",
        product_order_status: Iterable[str] = list(),
        claim_status: Iterable[str] = list(),
        place_order_status: str | None = None,
        page_start: int = 1,
        max_retries: int = 5,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str, DuckDBResult] | dict | list[dict] | None:
    """네이버 스마트스토어 상품 주문 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Tables** ( *table_key: table_name (description)* ):
        1. `order: smartstore_order` (주문 정보)
        2. `product_order: smartstore_product_order` (상품 주문 정보)
        3. `delivery: smartstore_delivery` (주문 배송 정보)
        4. `option: smartstore_option` (주문 옵션 정보)

    Parameters
    ----------
    client_id: str
        커머스 API 애플리케이션 ID
    client_secret: str
        커머스 API 애플리케이션 시크릿
    start_date: dt.date | str
        조회 기준의 시작 일시. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
    end_date: dt.date | str
        조회 기준의 종료 일시. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
    range_type: str
        조회 기준 유형. 기본값은 결제일시(`"PAYED_DATETIME"`)
    product_order_status: Iterable[str]
        상품 주문 상태 목록
    claim_status: Iterable[str]
        클레임 상태 목록
    place_order_status: str | None
        발주 상태
    page_start: int
        페이지 번호. 기본값은 `1`
    max_retries: int
        동시 요청 제한이 발생할 경우 최대 재시도 횟수. 기본값은 `5`
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        조회 기간 내 커서 요청 간 대기 시간(초). 기본값은 `1`
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
    dict[str, DuckDBResult] | dict | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 모든 테이블 조회 결과를 `{table_key: list[tuple]}` 구조로 반환한다.
            - `"json"`: 모든 테이블 조회 결과를 `{table_key: list[dict]}` 구조로 반환한다. (기본값)
            - `"parquet"`: 모든 테이블 조회 결과를 `{table_key: Parquet 바이너리}` 구조로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 또는 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.smartstore.api.order.extract import Order
    from linkmerce.core.smartstore.api.order.transform import Order as T
    return Order(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(client_id, client_secret),
        options = {
            "CursorAll": {
                "request_delay": request_delay
            },
            "RequestEachCursor": {
                "tqdm_options": {"disable": (not progress)}
            },
        },
    )).extract(start_date, end_date, range_type, product_order_status, claim_status, place_order_status, page_start, max_retries)


@with_duckdb_connection(table="smartstore_order_time")
def order_status(
        client_id: str,
        client_secret: str,
        channel_seq: int | str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        last_changed_type: str | None = None,
        max_retries: int = 5,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """네이버 스마트스토어 변경 상품 주문 상태 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: smartstore_order_time`

    Parameters
    ----------
    client_id: str
        커머스 API 애플리케이션 ID
    client_secret: str
        커머스 API 애플리케이션 시크릿
    channel_seq: int | str
        채널 번호
    start_date: dt.date | str
        조회 기준의 시작 일시. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
    end_date: dt.date | str
        조회 기준의 종료 일시. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
    last_changed_type: str | None
        최종 변경 구분
    max_retries: int
        동시 요청 제한이 발생할 경우 최대 재시도 횟수. 기본값은 `5`
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        조회 기간 내 커서 요청 간 대기 시간(초). 기본값은 `1`
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
    from linkmerce.core.smartstore.api.order.extract import OrderStatus
    from linkmerce.core.smartstore.api.order.transform import OrderStatus as T
    return OrderStatus(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(client_id, client_secret),
        options = {
            "CursorAll": {
                "request_delay": request_delay
            },
            "RequestEachCursor": {
                "tqdm_options": {"disable": (not progress)}
            },
        },
    )).extract(start_date, end_date, last_changed_type, channel_seq, max_retries)


@with_duckdb_connection(table="smartstore_order_time")
def aggregated_order_status(
        client_id: str,
        client_secret: str,
        channel_seq: int | str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        max_retries: int = 5,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: tuple[dict | None, dict | None, dict | None] = (None, None, None),
        transform_options: tuple[dict | None, dict | None, dict | None] = (None, None, None),
    ) -> DuckDBResult | dict[str, dict | list[dict]] | None:
    """네이버 스마트스토어 주문 상태 변경 시점을 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: smartstore_order_time`

    Parameters
    ----------
    client_id: str
        커머스 API 애플리케이션 ID
    client_secret: str
        커머스 API 애플리케이션 시크릿
    channel_seq: int | str
        채널 번호
    start_date: dt.date | str
        조회 기준의 시작 일시. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
    end_date: dt.date | str
        조회 기준의 종료 일시. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
    max_retries: int
        동시 요청 제한이 발생할 경우 최대 재시도 횟수. 기본값은 `5`
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        조회 기간 내 커서 요청 간 대기 시간(초). 기본값은 `1`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: tuple[dict | None, dict | None, dict | None]
        `Extractor` 초기화 옵션. `(OrderStatus, Order, Order)` 순서로 튜플을 구성한다.
    transform_options: tuple[dict | None, dict | None, dict | None]
        `Transformer` 초기화 옵션. `(OrderStatus, Order, Order)` 순서로 튜플을 구성한다.   
        테이블 명칭은 `OrderStatus` 옵션에만 지정한다. 나머지 `Order` 옵션에는 동일한 값이 적용된다.

    Returns
    -------
    DuckDBResult | dict[str, dict | list[dict]] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `{table_key: dict | list[dict]}` 구조의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.

        **NOTE** 원본 응답을 반환할 경우 `table_key`는 아래와 같이 각 `Extractor` 응답 결과와 대응된다.
            1. `order_status` - `OrderStatus`
            2. `purchase_decided` - `Order`
            3. `claim_completed` - `Order`
    """
    from linkmerce.core.smartstore.api.order.extract import OrderStatus
    from linkmerce.core.smartstore.api.order.transform import OrderStatus as T
    STATUS, PURCHASE, CLAIM = 0, 1, 2
    common = {
        "configs": _get_api_configs(client_id, client_secret),
        "options": {
            "CursorAll": {
                "request_delay": request_delay
            },
            "RequestEachCursor": {
                "tqdm_options": {"disable": (not progress)}
            },
        },
    }
    kwargs = {"channel_seq": channel_seq, "max_retries": max_retries}

    results = dict()
    return_type = return_type if return_type == "raw" else "none"

    results["order_status"] = OrderStatus(**prepare_duckdb_extract(
            T, connection, extract_options[STATUS], transform_options[STATUS], return_type, **common,
        )).extract(start_date, end_date, **kwargs)

    from linkmerce.core.smartstore.api.order.extract import Order
    from linkmerce.core.smartstore.api.order.transform import OrderTime as T
    from linkmerce.api.common import get_table
    table = get_table(transform_options[STATUS], default="smartstore_order_time")

    transform_options_ = (transform_options[PURCHASE] or dict()) | {"tables": {"table": table}}
    results["purchase_decided"] = Order(**prepare_duckdb_extract(
            T, connection, extract_options[PURCHASE], transform_options_, return_type, **common,
        )).extract(start_date, end_date, "PURCHASE_DECIDED_DATETIME", **kwargs)

    transform_options_ = (transform_options[CLAIM] or dict()) | {"tables": {"table": table}}
    results["claim_completed"] = Order(**prepare_duckdb_extract(
            T, connection, extract_options[CLAIM], transform_options_, return_type, **common,
        )).extract(start_date, end_date, "CLAIM_COMPLETED_DATETIME", **kwargs)

    return results


@with_duckdb_connection(table="marketing_channel")
def marketing_channel(
        client_id: str,
        client_secret: str,
        channel_seq: int | str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        max_retries: int = 5,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """네이버 스마트스토어 사용자 정의 채널 상세 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: marketing_channel`

    Parameters
    ----------
    client_id: str
        커머스 API 애플리케이션 ID
    client_secret: str
        커머스 API 애플리케이션 시크릿
    channel_seq: int | str
        조회 채널 번호
    start_date: dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
    end_date: dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
    max_retries: int
        동시 요청 제한이 발생할 경우 최대 재시도 횟수. 기본값은 `5`
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
    DuckDBResult | dict | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 또는 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.smartstore.api.bizdata.extract import MarketingChannel
    from linkmerce.core.smartstore.api.bizdata.transform import MarketingChannel as T
    return MarketingChannel(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(client_id, client_secret),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(channel_seq, start_date, end_date, max_retries)
