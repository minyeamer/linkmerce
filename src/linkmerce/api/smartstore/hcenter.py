from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.api.common import DuckDBResult
    from linkmerce.common.load import DuckDBConnection
    from pathlib import Path
    import datetime as dt


def login(
        userid: str | None = None,
        passwd: str | None = None,
        channel_seq: int | str | None = None,
        cookies: str | None = None,
        save_to: str | Path | None = None,
    ) -> str:
    """네이버 스마트스토어센터를 통해서 쇼핑파트너센터에 로그인하고 쿠키 문자열을 반환한다.

    Parameters
    ----------
    userid: str | None
        스마트스토어센터 판매자 아이디/이메일
    passwd: str | None
        스마트스토어센터 판매자 비밀번호
    channel_seq: int | str | None
        채널 번호. 로그인 후 접속된 채널이 다르면 채널 번호에 맞는 채널로 전환한다.
    cookies: str | None
        네이버 로그인 쿠키 문자열. 판매자 아이디 로그인을 대체하여 네이버 아이디로 로그인한다.
    save_to: str | Path | None
        로그인 후 쿠키를 저장할 파일 경로. 상위 경로가 없으면 자동 생성한다.

    Returns
    -------
    str
        네이버 쇼핑파트너센터 쿠키 문자열
    """
    from linkmerce.core.smartstore.hcenter.common import PartnerCenterLogin
    from linkmerce.api.common import handle_cookies
    handler = PartnerCenterLogin()
    handler.login(userid, passwd, channel_seq, cookies)
    return handle_cookies(handler, save_to)


@with_duckdb_connection(table="naver_brand_catalog")
def brand_catalog(
        cookies: str,
        brand_ids: str | Iterable[str],
        sort_type: Literal["popular", "recent", "count", "price"] = "recent",
        is_brand_catalog: bool | None = None,
        page: int | list[int] | None = 0,
        page_size: int = 100,
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """네이버 브랜드 카탈로그 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: naver_brand_catalog`

    Parameters
    ----------
    cookies: str
        네이버 쇼핑파트너센터 로그인 쿠키 문자열
    brand_ids: str | Iterable[str]
        브랜드 ID. 문자열 또는 문자열의 배열을 입력한다.
    sort_type: str
        정렬 기준
            - `"popular"`: 카탈로그 인기순
            - `"recent"`: 등록일 최신순 (기본값)
            - `"count"`: 판매처 많은순
            - `"price"`: 최저가 낮은순
    is_brand_catalog: bool | None
        - `True`: 브랜드 카탈로그만 보기
        - `False`: 일반 카탈로그만 보기
        - `None`: 전체 카탈로그 보기 (기본값)
    page: int | list[int] | None
        조회할 페이지 번호. 정수 또는 정수의 배열을 입력할 수 있다. (0부터 시작)
    page_size: int
        한 번에 표시할 목록 수. 가본값은 `100`
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    how_to_run: str
        `Extractor` 실행 방식
            - `"sync"`: 동기 실행. `extract` 메서드를 실행한다. (기본값)
            - `"async"`: 비동기 실행. `asyncio.run` 함수로 감싸서 `extract_async` 코루틴을 실행한다.
            - `"async_loop"`: 비동기 루프 내 실행. 주피터 노트북 등 이미 비동기 이벤트 루프 내에 있을 경우에 비동기 실행을 처리한다.
    max_concurrent: int
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    request_delay: float | int | tuple[int, int]
        페이지 및 브랜드별 요청 간 대기 시간(초). 기본값은 `1`
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
    from linkmerce.core.smartstore.hcenter.catalog.extract import BrandCatalog
    from linkmerce.core.smartstore.hcenter.catalog.transform import BrandCatalog as T
    return BrandCatalog(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
        options = {
            "PaginateAll": {
                "max_concurrent": max_concurrent,
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)},
            },
            "RequestEachPages": {
                "max_concurrent": max_concurrent,
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)},
            },
        },
    )).run(brand_ids, sort_type, is_brand_catalog, page, page_size, how_to_run=how_to_run)


@with_duckdb_connection(table="naver_brand_product")
def brand_product(
        cookies: str,
        brand_ids: str | Iterable[str],
        mall_seq: int | str | Iterable[int | str] | None = None,
        sort_type: Literal["popular", "recent", "price"] = "recent",
        is_brand_store: bool | None = None,
        page: int | list[int] | None = 0,
        page_size: int = 100,
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """네이버 브랜드 상품 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: naver_brand_product`

    Parameters
    ----------
    cookies: str
        네이버 쇼핑파트너센터 로그인 쿠키 문자열
    brand_ids: str | Iterable[str]
        브랜드 ID. 문자열 또는 문자열의 배열을 입력한다.   
        브랜드 ID를 쉼표(,)로 묶어서 OR 조건으로 동시에 조회할 수 있다.
    mall_seq: int | str | Iterable[int | str] | None
        쇼핑몰 순번. 정수 또는 문자열, 또는 정수/문자열의 배열을 입력할 수 있다.   
        목록으로 입력할 시 브랜드 ID 목록에서 인덱스가 동일한 항목과 AND 조건으로 조회된다.
    sort_type: str
        정렬 기준
            - `"popular"`: 상품 인기순
            - `"recent"`: 등록일 최신순 (기본값)
            - `"price"`: 가격낮은순
    is_brand_store: bool | None
        - `True`: 브랜드 공식스토어 상품만 보기
        - `False`: 다른 쇼핑몰 상품만 보기
        - `None`: 전체 상품 보기 (기본값)
    page: int | list[int] | None
        조회할 페이지 번호. 정수 또는 정수의 배열을 입력할 수 있다. (0부터 시작)
    page_size: int
        한 번에 표시할 목록 수. 가본값은 `100`
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    how_to_run: str
        `Extractor` 실행 방식
            - `"sync"`: 동기 실행. `extract` 메서드를 실행한다. (기본값)
            - `"async"`: 비동기 실행. `asyncio.run` 함수로 감싸서 `extract_async` 코루틴을 실행한다.
            - `"async_loop"`: 비동기 루프 내 실행. 주피터 노트북 등 이미 비동기 이벤트 루프 내에 있을 경우에 비동기 실행을 처리한다.
    max_concurrent: int
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    request_delay: float | int | tuple[int, int]
        페이지 및 브랜드+쇼핑몰별 요청 간 대기 시간(초). 기본값은 `1`
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
    from linkmerce.core.smartstore.hcenter.catalog.extract import BrandProduct
    from linkmerce.core.smartstore.hcenter.catalog.transform import BrandProduct as T
    return BrandProduct(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
        options = {
            "PaginateAll": {
                "max_concurrent": max_concurrent,
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)},
            },
            "RequestEachPages": {
                "max_concurrent": max_concurrent,
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)},
            },
        },
    )).run(brand_ids, mall_seq, sort_type, is_brand_store, page, page_size, how_to_run=how_to_run)


@with_duckdb_connection(tables={"price": "naver_price_history", "product": "naver_product"})
def brand_price(
        cookies: str,
        brand_ids: str | Iterable[str],
        mall_seq: int | str | Iterable[int | str],
        sort_type: Literal["popular", "recent", "price"] = "recent",
        is_brand_catalog: bool | None = None,
        page: int | list[int] | None = 0,
        page_size: int = 100,
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str, DuckDBResult] | dict | list[dict] | None:
    """네이버 브랜드 상품 데이터로부터 가격 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Tables** ( *table_key: table_name (description)* ):
        1. `price: naver_price_history` (브랜드 상품 가격)
        2. `product: naver_product` (브랜드 상품 정보)

    Parameters
    ----------
    cookies: str
        네이버 쇼핑파트너센터 로그인 쿠키 문자열
    brand_ids: str | Iterable[str]
        브랜드 ID. 문자열 또는 문자열의 배열을 입력한다.   
        브랜드 ID를 쉼표(,)로 묶어서 OR 조건으로 동시에 조회할 수 있다.
    mall_seq: int | str | Iterable[int | str] | None
        쇼핑몰 순번. 정수 또는 문자열, 또는 정수/문자열의 배열을 입력할 수 있다.   
        목록으로 입력할 시 브랜드 ID 목록에서 인덱스가 동일한 항목과 AND 조건으로 조회된다.
    sort_type: str
        정렬 기준
            - `"popular"`: 상품 인기순
            - `"recent"`: 등록일 최신순 (기본값)
            - `"price"`: 가격낮은순
    is_brand_store: bool | None
        - `True`: 브랜드 공식스토어 상품만 보기
        - `False`: 다른 쇼핑몰 상품만 보기
        - `None`: 전체 상품 보기 (기본값)
    page: int | list[int] | None
        조회할 페이지 번호. 정수 또는 정수의 배열을 입력할 수 있다. (0부터 시작)
    page_size: int
        한 번에 표시할 목록 수. 가본값은 `100`
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    how_to_run: str
        `Extractor` 실행 방식
            - `"sync"`: 동기 실행. `extract` 메서드를 실행한다. (기본값)
            - `"async"`: 비동기 실행. `asyncio.run` 함수로 감싸서 `extract_async` 코루틴을 실행한다.
            - `"async_loop"`: 비동기 루프 내 실행. 주피터 노트북 등 이미 비동기 이벤트 루프 내에 있을 경우에 비동기 실행을 처리한다.
    max_concurrent: int
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    request_delay: float | int | tuple[int, int]
        페이지 및 브랜드+쇼핑몰별 요청 간 대기 시간(초). 기본값은 `1`
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
    from linkmerce.core.smartstore.hcenter.catalog.extract import BrandProduct
    from linkmerce.core.smartstore.hcenter.catalog.transform import BrandPrice as T
    return BrandProduct(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
        options = {
            "PaginateAll": {
                "max_concurrent": max_concurrent,
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)},
            },
            "RequestEachPages": {
                "max_concurrent": max_concurrent,
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)},
            },
        },
    )).run(brand_ids, mall_seq, sort_type, is_brand_catalog, page, page_size, how_to_run=how_to_run)


@with_duckdb_connection(table="naver_catalog_product")
def product_catalog(
        cookies: str,
        brand_ids: str | Iterable[str],
        mall_seq: int | str | Iterable[int | str],
        sort_type: Literal["popular", "recent", "price"] = "recent",
        is_brand_catalog: bool | None = None,
        page: int | list[int] | None = 0,
        page_size: int = 100,
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """네이버 브랜드 상품 데이터로부터 카탈로그-상품 매핑 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: naver_catalog_product`

    Parameters
    ----------
    cookies: str
        네이버 쇼핑파트너센터 로그인 쿠키 문자열
    brand_ids: str | Iterable[str]
        브랜드 ID. 문자열 또는 문자열의 배열을 입력한다.   
        브랜드 ID를 쉼표(,)로 묶어서 OR 조건으로 동시에 조회할 수 있다.
    mall_seq: int | str | Iterable[int | str] | None
        쇼핑몰 순번. 정수 또는 문자열, 또는 정수/문자열의 배열을 입력할 수 있다.   
        목록으로 입력할 시 브랜드 ID 목록에서 인덱스가 동일한 항목과 AND 조건으로 조회된다.
    sort_type: str
        정렬 기준
            - `"popular"`: 상품 인기순
            - `"recent"`: 등록일 최신순 (기본값)
            - `"price"`: 가격낮은순
    is_brand_store: bool | None
        - `True`: 브랜드 공식스토어 상품만 보기
        - `False`: 다른 쇼핑몰 상품만 보기
        - `None`: 전체 상품 보기 (기본값)
    page: int | list[int] | None
        조회할 페이지 번호. 정수 또는 정수의 배열을 입력할 수 있다. (0부터 시작)
    page_size: int
        한 번에 표시할 목록 수. 가본값은 `100`
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    how_to_run: str
        `Extractor` 실행 방식
            - `"sync"`: 동기 실행. `extract` 메서드를 실행한다. (기본값)
            - `"async"`: 비동기 실행. `asyncio.run` 함수로 감싸서 `extract_async` 코루틴을 실행한다.
            - `"async_loop"`: 비동기 루프 내 실행. 주피터 노트북 등 이미 비동기 이벤트 루프 내에 있을 경우에 비동기 실행을 처리한다.
    max_concurrent: int
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    request_delay: float | int | tuple[int, int]
        페이지 및 브랜드+쇼핑몰별 요청 간 대기 시간(초). 기본값은 `1`
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
    from linkmerce.core.smartstore.hcenter.catalog.extract import BrandProduct
    from linkmerce.core.smartstore.hcenter.catalog.transform import ProductCatalog as T
    return BrandProduct(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
        options = {
            "PaginateAll": {
                "max_concurrent": max_concurrent,
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)},
            },
            "RequestEachPages": {
                "max_concurrent": max_concurrent,
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)},
            },
        },
    )).run(brand_ids, mall_seq, sort_type, is_brand_catalog, page, page_size, how_to_run=how_to_run)


def page_view(
        cookies: str,
        aggregate_by: Literal["device", "product", "url"],
        mall_seq: int | str | Iterable[int | str],
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_retries: int = 5,
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """네이버 브랜드 스토어의 방문 통계 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Tables** ( *aggregate_by: table_name (description)* ):
        1. `device: naver_pv_by_device` (일별/기기별 방문 통계)
        2. `product: naver_pv_by_product` (일별/상품별 방문 통계)
        3. `url: naver_pv_by_url` (일별/URL별 방문 통계)

    **NOTE** `aggregate_by`에 해당하는 테이블 하나만 생성된다.

    Parameters
    ----------
    cookies: str
        네이버 쇼핑파트너센터 로그인 쿠키 문자열
    aggregate_by: str
        집계 기준
            - `"device"`: 일별/기기별 방문 통계
            - `"product"`: 일별/상품별 방문 통계
            - `"url"`: 일별/URL별 방문 통계
    mall_seq: int | str | Iterable[int | str]
        브랜드 스토어의 쇼핑몰 순번. 정수 또는 문자열, 또는 정수/문자열의 배열을 입력한다.
    start_date: dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
    end_date: dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    how_to_run: str
        `Extractor` 실행 방식
            - `"sync"`: 동기 실행. `extract` 메서드를 실행한다. (기본값)
            - `"async"`: 비동기 실행. `asyncio.run` 함수로 감싸서 `extract_async` 코루틴을 실행한다.
            - `"async_loop"`: 비동기 루프 내 실행. 주피터 노트북 등 이미 비동기 이벤트 루프 내에 있을 경우에 비동기 실행을 처리한다.
    max_retries: int
        최대 반복 실행 횟수. 기본값은 `5`
    max_concurrent: int
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    request_delay: float | int | tuple[int, int]
        쇼핑몰별 요청 간 대기 시간(초). 기본값은 `1`
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
            - `"csv"`: 선택된 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 선택된 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 선택된 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 또는 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    if aggregate_by == "device":
        from linkmerce.core.smartstore.hcenter.pageview.extract import PageViewByDevice as E
        from linkmerce.core.smartstore.hcenter.pageview.transform import PageViewByDevice as T
        table = "naver_pv_by_device"
    elif aggregate_by == "product":
        from linkmerce.core.smartstore.hcenter.pageview.extract import PageViewByUrl as E
        from linkmerce.core.smartstore.hcenter.pageview.transform import PageViewByProduct as T
        table = "naver_pv_by_product"
    else:
        from linkmerce.core.smartstore.hcenter.pageview.extract import PageViewByUrl as E
        from linkmerce.core.smartstore.hcenter.pageview.transform import PageViewByUrl as T
        table = "naver_pv_by_url"

    @with_duckdb_connection(table=table)
    def _main(*args, connection: DuckDBConnection | None = None, return_type: str = "json", **kwargs):
        return E(**prepare_duckdb_extract(
            T, connection, extract_options, transform_options, return_type,
            cookies = cookies,
            options = {
                "RequestLoop": {
                    "max_retries": max_retries
                },
                "RequestEachLoop": {
                    "request_delay": request_delay,
                    "max_concurrent": max_concurrent,
                    "tqdm_options": {"disable": (not progress)}
                },
            },
        )).run(mall_seq, start_date, end_date, how_to_run=how_to_run)
    return _main(connection=connection, return_type=return_type)


def store_sales(
        cookies: str,
        sales_type: Literal["store", "category", "product"],
        mall_seq: int | str | Iterable[int | str],
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["daily", "weekly", "monthly"] = "daily",
        page: int | Iterable[int] = 1,
        page_size: int = 1000,
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_retries: int = 5,
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """네이버 스토어의 매출 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Tables** ( *sales_type: table_name (description)* ):
        1. `store: naver_store_sales` (스토어 매출)
        2. `category: naver_category_sales` (카테고리별 매출)
        3. `product: naver_product_sales` (상품별 매출)

    **NOTE** `sales_type`에 해당하는 테이블 하나만 생성된다.

    Parameters
    ----------
    cookies: str
        네이버 쇼핑파트너센터 로그인 쿠키 문자열
    sales_type: str
        매출 유형
            - `"store"`: 스토어 매출
            - `"category"`: 카테고리별 매출
            - `"product"`: 상품별 매출
    mall_seq: int | str | Iterable[int | str]
        쇼핑몰 순번. 정수 또는 문자열, 또는 정수/문자열의 배열을 입력한다.
    start_date: dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
    end_date: dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
    date_type: str
        기간
            - `"daily"`: 일간 (기본값)
            - `"weekly"`: 주간
            - `"monthly"`: 월간
    page: int | Iterable[int]
        페이지 번호. 정수 또는 정수의 배열을 입력한다.
    page_size: int
        한 번에 표시할 목록 수. 가본값은 `1000`
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    how_to_run: str
        `Extractor` 실행 방식
            - `"sync"`: 동기 실행. `extract` 메서드를 실행한다. (기본값)
            - `"async"`: 비동기 실행. `asyncio.run` 함수로 감싸서 `extract_async` 코루틴을 실행한다.
            - `"async_loop"`: 비동기 루프 내 실행. 주피터 노트북 등 이미 비동기 이벤트 루프 내에 있을 경우에 비동기 실행을 처리한다.
    max_retries: int
        최대 반복 실행 횟수. 기본값은 `5`
    max_concurrent: int
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    request_delay: float | int | tuple[int, int]
        쇼핑몰 및 조회 기간별 요청 간 대기 시간(초). 기본값은 `1`
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
            - `"csv"`: 선택된 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 선택된 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 선택된 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 또는 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    if sales_type == "store":
        from linkmerce.core.smartstore.hcenter.sales.extract import StoreSales as E
        from linkmerce.core.smartstore.hcenter.sales.transform import StoreSales as T
        table = "naver_store_sales"
    elif sales_type == "category":
        from linkmerce.core.smartstore.hcenter.sales.extract import CategorySales as E
        from linkmerce.core.smartstore.hcenter.sales.transform import CategorySales as T
        table = "naver_category_sales"
    else:
        from linkmerce.core.smartstore.hcenter.sales.extract import ProductSales as E
        from linkmerce.core.smartstore.hcenter.sales.transform import ProductSales as T
        table = "naver_product_sales"

    @with_duckdb_connection(table=table)
    def _main(*args, connection: DuckDBConnection | None = None, return_type: str = "json", **kwargs):
        return E(**prepare_duckdb_extract(
            T, connection, extract_options, transform_options, return_type,
            cookies = cookies,
            options = {
                "RequestLoop": {
                    "max_retries": max_retries
                },
                "RequestEachLoop": {
                    "request_delay": request_delay,
                    "max_concurrent": max_concurrent,
                    "tqdm_options": {"disable": (not progress)}
                },
            },
        )).run(mall_seq, start_date, end_date, date_type, page, page_size, how_to_run=how_to_run)
    return _main(connection=connection, return_type=return_type)


@with_duckdb_connection(tables={"sales": "naver_sales", "product": "naver_product"})
def aggregated_sales(
        cookies: str,
        mall_seq: int | str | Iterable[int | str],
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["daily", "weekly", "monthly"] = "daily",
        page: int | Iterable[int] = 1,
        page_size: int = 1000,
        *,
        connection: DuckDBConnection | None = None,
        how_to_run: Literal["sync", "async", "async_loop"] = "sync",
        max_retries: int = 5,
        max_concurrent: int = 3,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str, DuckDBResult] | dict | list[dict] | None:
    """네이버 스토어의 일간/상품별 매출 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Tables** ( *table_key: table_name (description)* ):
        1. `sales: naver_sales` (상품 매출)
        2. `product: naver_product` (상품 정보)

    Parameters
    ----------
    cookies: str
        네이버 쇼핑파트너센터 로그인 쿠키 문자열
    mall_seq: int | str | Iterable[int | str]
        쇼핑몰 순번. 정수 또는 문자열, 또는 정수/문자열의 배열을 입력한다.
    start_date: dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
    end_date: dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
    date_type: str
        기간
            - `"daily"`: 일간 (기본값)
            - `"weekly"`: 주간
            - `"monthly"`: 월간
    page: int | Iterable[int]
        페이지 번호. 정수 또는 정수의 배열을 입력한다.
    page_size: int
        한 번에 표시할 목록 수. 가본값은 `1000`
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    how_to_run: str
        `Extractor` 실행 방식
            - `"sync"`: 동기 실행. `extract` 메서드를 실행한다. (기본값)
            - `"async"`: 비동기 실행. `asyncio.run` 함수로 감싸서 `extract_async` 코루틴을 실행한다.
            - `"async_loop"`: 비동기 루프 내 실행. 주피터 노트북 등 이미 비동기 이벤트 루프 내에 있을 경우에 비동기 실행을 처리한다.
    max_retries: int
        최대 반복 실행 횟수. 기본값은 `5`
    max_concurrent: int
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    request_delay: float | int | tuple[int, int]
        쇼핑몰 및 조회 기간별 요청 간 대기 시간(초). 기본값은 `1`
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
    from linkmerce.core.smartstore.hcenter.sales.extract import ProductSales
    from linkmerce.core.smartstore.hcenter.sales.transform import AggregatedSales as T
    return ProductSales(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
        options = {
            "RequestLoop": {
                "max_retries": max_retries
            },
            "RequestEachLoop": {
                "request_delay": request_delay,
                "max_concurrent": max_concurrent,
                "tqdm_options": {"disable": (not progress)}
            },
        },
    )).run(mall_seq, start_date, end_date, date_type, page, page_size, how_to_run=how_to_run)
