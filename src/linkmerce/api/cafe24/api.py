from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.api.common import DuckDBResult
    from linkmerce.common.load import DuckDBConnection
    from linkmerce.core.cafe24.api.common import Cafe24ApiTokenManager
    from pathlib import Path
    import datetime as dt


def _get_api_configs(
        mall_id: str,
        client_id: str,
        client_secret: str,
        token_info: dict[str, str] | str | Path,
    ) -> dict:
    """카페24 API 인증에 필요한 설정을 구성한다."""
    return {
        "mall_id": mall_id,
        "client_id": client_id,
        "client_secret": client_secret,
        "token_info": token_info,
    }


def _get_token_manager(
        mall_id: str,
        extract_options: dict | None = None,
    ) -> Cafe24ApiTokenManager:
    """카페24 토큰 관리자를 생성한다."""
    from linkmerce.core.cafe24.api.common import Cafe24ApiTokenManager
    from linkmerce.utils.nested import merge
    return Cafe24ApiTokenManager(**merge(
        extract_options or dict(),
        configs = {"mall_id": mall_id},
    ))


def request(
        mall_id: str,
        client_id: str,
        client_secret: str,
        token_info: dict[str, str],
        method: str,
        path: str,
        version: str | None = None,
        params: dict | list[tuple] | bytes | None = None,
        data: dict | list[tuple] | bytes | None = None,
        json: dict | None = None,
        headers: dict[str, str] | None = None,
        extract_options: dict | None = None,
    ) -> dict:
    """카페24 API에 임의의 HTTP 요청을 보내 JSON 응답을 반환한다.

    토큰 갱신 결과는 반환하지 않는다. 반복 호출 시 `validate_token`으로 토큰을 갱신하고
    반환된 토큰 정보를 보관해 전달한다.

    Parameters
    ----------
    mall_id: str
        카페24 쇼핑몰 아이디
    client_id: str
        카페24 앱 클라이언트 아이디
    client_secret: str
        카페24 앱 클라이언트 시크릿
    token_info: dict[str, str] | str | Path
        카페24 액세스 토큰을 포함하는 OAuth 응답 결과가 기록된 파일 경로 또는 객체.
        `access_token`, `refresh_token` 키를 반드시 포함해야 하며,
        `expires_at` 값에 따라 액세스 토큰 만료 여부를 판단해 갱신한다.
    method: str
        HTTP 메서드
    path: str
        카페24 API 경로. `admin/orders`처럼 API 버전 뒤의 상대 경로를 입력한다.
    version: str | None
        카페24 API 버전 경로. 생략하면 `v2`를 사용한다.
    params: dict | list[tuple] | bytes | None
        카페24 API 요청 파라미터
    data: dict | list[tuple] | bytes | None
        카페24 API 요청 본문
    json: dict | None
        카페24 API 요청 본문 (JSON)
    headers: dict[str, str] | None
        카페24 API 요청 헤더
    extract_options: dict | None
        `Cafe24TestApi` 초기화 옵션

    Returns
    -------
    dict
        카페24 API 응답 결과
    """
    from linkmerce.core.cafe24.api.common import Cafe24TestApi
    from linkmerce.utils.nested import merge
    extractor = Cafe24TestApi(**merge(
        extract_options or dict(),
        configs = _get_api_configs(mall_id, client_id, client_secret, token_info),
    ))
    return extractor.extract(method, path, version, params, data, json, headers)


def make_oauth_url(
        mall_id: str,
        client_id: str,
        redirect_uri: str,
        scope: str | Iterable[str],
    ) -> str:
    """브라우저에서 카페24 권한 동의를 진행할 인증 요청 URL을 생성한다.

    Parameters
    ----------
    mall_id: str
        카페24 쇼핑몰 아이디
    client_id: str
        카페24 앱 클라이언트 아이디
    redirect_uri: str
        인증 코드가 발급될 Redirect URI
    scope: str | Iterable[str]
        개발자센터에 등록한 API 권한 범위
    extract_options: dict | None
        `Cafe24ApiTokenManager` 초기화 옵션

    Returns
    -------
    str
        카페24 OAuth 인증 요청 URL
    """
    return (_get_token_manager(mall_id)
            .make_oauth_url(client_id, redirect_uri, scope))


def create_token(
        mall_id: str,
        client_id: str,
        client_secret: str,
        code: str,
        redirect_uri: str,
        save_to: str | Path | None = None,
        extract_options: dict | None = None,
    ) -> dict:
    """인증 코드로 카페24 액세스 토큰을 발급받는다.

    Parameters
    ----------
    mall_id: str
        카페24 쇼핑몰 아이디
    client_id: str
        카페24 앱 클라이언트 아이디
    client_secret: str
        카페24 앱 클라이언트 시크릿
    code: str
        브라우저에서 권한 동의를 완료한 후 발급받은 인증 코드
    redirect_uri: str
        인증 코드를 발급받은 Redirect URI
    save_to: str | Path | None
        OAuth 응답 결과를 저장할 JSON 파일 경로. 상위 경로가 없으면 자동 생성한다.
    extract_options: dict | None
        `Cafe24ApiTokenManager` 초기화 옵션

    Returns
    -------
    dict
        액세스 토큰, 리프레시 토큰 및 만료 시각을 포함한 카페24 OAuth 응답
    """
    return (_get_token_manager(mall_id, extract_options)
            .create_token(client_id, client_secret, code, redirect_uri, save_to))


def refresh_token(
        mall_id: str,
        client_id: str,
        client_secret: str,
        token: str,
        save_to: str | Path | None = None,
        extract_options: dict | None = None,
    ) -> dict:
    """리프레시 토큰으로 카페24 액세스 토큰을 갱신한다.

    Parameters
    ----------
    mall_id: str
        카페24 쇼핑몰 아이디
    client_id: str
        카페24 앱 클라이언트 아이디
    client_secret: str
        카페24 앱 클라이언트 시크릿
    token: str
        리프레시 토큰 (발급 후 2주가 지나지 않은 토큰만 유효하다.)
    save_to: str | Path | None
        OAuth 응답 결과를 저장할 JSON 파일 경로. 상위 경로가 없으면 자동 생성한다.
    extract_options: dict | None
        `Cafe24ApiTokenManager` 초기화 옵션

    Returns
    -------
    dict
        액세스 토큰, 리프레시 토큰 및 만료 시각을 포함한 카페24 OAuth 응답
    """
    return (_get_token_manager(mall_id, extract_options)
            .refresh_token(client_id, client_secret, token, save_to))


def validate_token(
        mall_id: str,
        client_id: str,
        client_secret: str,
        token_info: dict,
        from_now: dict | None = None,
        save_to: str | Path | None = None,
        extract_options: dict | None = None,
    ) -> dict:
    """카페24 액세스 토큰이 만료되었다면 갱신하고 아니면 그대로 반환한다.

    Parameters
    ----------
    mall_id: str
        카페24 쇼핑몰 아이디
    client_id: str
        카페24 앱 클라이언트 아이디
    client_secret: str
        카페24 앱 클라이언트 시크릿
    token_info: dict[str, str]
        카페24 액세스 토큰을 포함하는 OAuth 응답 결과.
        `access_token`, `refresh_token` 키를 반드시 포함해야 하며,
        `expires_at` 값에 따라 액세스 토큰 만료 여부를 판단해 갱신한다.
    from_now: dict | None
        만료 시각 판단 기준. 현재로부터 `timedelta` 이후의 시각을 기준으로 사용한다.
    save_to: str | Path | None
        OAuth 응답 결과를 저장할 JSON 파일 경로. 상위 경로가 없으면 자동 생성한다.
    extract_options: dict | None
        `Cafe24ApiTokenManager` 초기화 옵션

    Returns
    -------
    dict
        액세스 토큰, 리프레시 토큰 및 만료 시각을 포함한 카페24 OAuth 응답
    """
    return (_get_token_manager(mall_id, extract_options)
            .validate_token(client_id, client_secret, token_info, from_now, save_to))


@with_duckdb_connection(table="cafe24_product")
def product(
        mall_id: str,
        client_id: str,
        client_secret: str,
        token_info: dict[str, str] | str | Path,
        product_name: str | None = None,
        display: Literal["T", "F"] | None = None,
        selling: Literal["T", "F"] | None = None,
        shop_no: int = 1,
        embed: str | None = None,
        page_size: int = 100,
        since_product_no: int = 0,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int | tuple[int, int] = 0.5,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | list[dict] | None:
    """카페24 상품 목록을 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: cafe24_product`

    Parameters
    ----------
    mall_id: str
        카페24 쇼핑몰 아이디
    client_id: str
        카페24 앱 클라이언트 아이디
    client_secret: str
        카페24 앱 클라이언트 시크릿
    token_info: dict[str, str] | str | Path
        카페24 액세스 토큰을 포함하는 OAuth 응답 결과가 기록된 파일 경로 또는 객체.
        `access_token`, `refresh_token` 키를 반드시 포함해야 하며,
        `expires_at` 값에 따라 액세스 토큰 만료 여부를 판단해 갱신한다.
    product_name: str | None
        상품명 검색어. 생략하면 전체 상품을 조회한다.
    display: str | None
        진열상태. `"T"`: 진열함, `"F"`: 진열안함. 생략하면 전체 상태를 조회한다.
    selling: str | None
        판매상태. `"T"`: 판매함, `"F"`: 판매안함. 생략하면 전체 상태를 조회한다.
    shop_no: int
        멀티쇼핑몰 번호. 기본값은 `1`
    embed: str | None
        함께 조회할 하위 리소스. 쉼표(,)로 구분하여 여러 개를 지정할 수 있다.
        - 사용 가능한 값: channeldiscountprices, discountprice, decorationimages, benefits, options, variants, additionalimages, hits
    page_size: int
        조회결과 최대건수. 기본값은 `100`, 최대 `100`
    since_product_no: int
        해당 상품번호 이후 검색. 기본값은 `0`
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        커서별 요청 간 대기 시간(초). 기본값은 `0.5`
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
    from linkmerce.core.cafe24.api.product.extract import Product
    from linkmerce.core.cafe24.api.product.transform import Product as T
    return Product(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(mall_id, client_id, client_secret, token_info),
        options = {"CursorAll": {"request_delay": request_delay}},
    )).extract(product_name, display, selling, shop_no, embed, page_size, since_product_no)


@with_duckdb_connection(table="cafe24_product_variant")
def product_variant(
        mall_id: str,
        client_id: str,
        client_secret: str,
        token_info: dict[str, str] | str | Path,
        product_no: int | str | Iterable[int | str],
        shop_no: int = 1,
        embed: str | None = None,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int | tuple[int, int] = 0.5,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """카페24 상품 품목 목록을 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: cafe24_product_variant`

    Parameters
    ----------
    mall_id: str
        카페24 쇼핑몰 아이디
    client_id: str
        카페24 앱 클라이언트 아이디
    client_secret: str
        카페24 앱 클라이언트 시크릿
    token_info: dict[str, str] | str | Path
        카페24 액세스 토큰을 포함하는 OAuth 응답 결과가 기록된 파일 경로 또는 객체.
        `access_token`, `refresh_token` 키를 반드시 포함해야 하며,
        `expires_at` 값에 따라 액세스 토큰 만료 여부를 판단해 갱신한다.
    product_no: int | str | Iterable[int | str]
        상품번호. 단일 값 또는 배열
    shop_no: int
        멀티쇼핑몰 번호. 기본값은 `1`
    embed: str | None
        함께 조회할 하위 리소스. 사용 가능한 값: inventories
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        상품별 요청 간 대기 시간(초). 기본값은 `0.5`
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
            - `"raw"`: 데이터 수집 후 `dict | list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.cafe24.api.product.extract import Variant
    from linkmerce.core.cafe24.api.product.transform import Variant as T
    return Variant(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(mall_id, client_id, client_secret, token_info),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(product_no, shop_no, embed)


@with_duckdb_connection(table="cafe24_product_option")
def product_option(
        mall_id: str,
        client_id: str,
        client_secret: str,
        token_info: dict[str, str] | str | Path,
        product_name: str | None = None,
        display: Literal["T", "F"] | None = None,
        selling: Literal["T", "F"] | None = None,
        shop_no: int = 1,
        page_size: int = 100,
        since_product_no: int = 0,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int | tuple[int, int] = 0.5,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: tuple[dict | None, dict | None] = (None, None),
        transform_options: tuple[dict | None, dict | None] = (None, None),
        merged_table: str | None = None,
    ) -> DuckDBResult | dict[str, list[dict]] | None:
    """카페24 상품 목록과 품목 목록을 수집해 하나의 DuckDB 테이블로 결합 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: cafe24_product_option`

    Parameters
    ----------
    mall_id: str
        카페24 쇼핑몰 아이디
    client_id: str
        카페24 앱 클라이언트 아이디
    client_secret: str
        카페24 앱 클라이언트 시크릿
    token_info: dict[str, str] | str | Path
        카페24 액세스 토큰을 포함하는 OAuth 응답 결과가 기록된 파일 경로 또는 객체.
        `access_token`, `refresh_token` 키를 반드시 포함해야 하며,
        `expires_at` 값에 따라 액세스 토큰 만료 여부를 판단해 갱신한다.
    product_name: str | None
        상품명 검색어. 생략하면 전체 상품을 조회한다.
    display: str | None
        진열상태. `"T"`: 진열함, `"F"`: 진열안함. 생략하면 전체 상태를 조회한다.
    selling: str | None
        판매상태. `"T"`: 판매함, `"F"`: 판매안함. 생략하면 전체 상태를 조회한다.
    shop_no: int
        멀티쇼핑몰 번호. 기본값은 `1`
    page_size: int
        조회결과 최대건수. 기본값은 `100`, 최대 `100`
    since_product_no: int
        해당 상품번호 이후 검색. 기본값은 `0`
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        커서 및 추가 품목 요청 간 대기 시간(초). 기본값은 `0.5`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: tuple[dict | None, dict | None]
        `Extractor` 초기화 옵션. `(Product, Variant)` 순서로 튜플을 구성한다.
        `Variant` 옵션은 전체 품목을 별도로 조회할 때 사용한다.
    transform_options: tuple[dict | None, dict | None]
        `Transformer` 초기화 옵션. `(Product, Variant)` 순서로 튜플을 구성한다.
    merged_table: str | None
        상품과 품목 병합 결과를 적재할 테이블 명칭. 생략하면 `"cafe24_product_option"` 테이블을 생성한다.

    Returns
    -------
    DuckDBResult | dict[str, list[dict]] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `{"product": Product, "product_variant": Variant}` 구조의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.cafe24.api.product.extract import Product
    from linkmerce.core.cafe24.api.product.transform import Product as T1, Variant as T2
    from linkmerce.api.common import get_table
    from linkmerce.utils.nested import merge
    PRODUCT, VARIANT = 0, 1
    product_table = get_table(transform_options[PRODUCT], default="cafe24_product")
    variant_table = get_table(transform_options[VARIANT], default="cafe24_product_variant")
    configs = _get_api_configs(mall_id, client_id, client_secret, token_info)

    embed = "variants"
    pages = Product(
        **merge(extract_options[PRODUCT] or dict(),
            configs = configs,
            options = {"CursorAll": {"request_delay": request_delay}},
        )).extract(product_name, display, selling, shop_no, embed, page_size, since_product_no)

    t1 = T1(**merge(transform_options[PRODUCT] or dict(), db_info={"conn": connection}))
    t2 = T2(**merge(transform_options[VARIANT] or dict(), db_info={"conn": connection}))

    variants, extra_product_no = list(), list()
    for page in pages:
        if return_type != "raw":
            t1.transform(page, mall_id=mall_id)

        for product in page["products"]:
            product_no = product["product_no"]
            if len(product["variants"]) >= 100:
                extra_product_no.append(product_no)
            elif return_type == "raw":
                variants.append({"variants": product["variants"]})

            if return_type != "raw":
                t2.transform({"variants": product["variants"]}, mall_id=mall_id, product_no=product_no)

    # 품목이 100개 이상 반환된 상품은 전체 품목을 별도로 조회해 보완한다.
    results = list()
    if extra_product_no:
        from linkmerce.core.cafe24.api.product.extract import Variant
        results = Variant(**prepare_duckdb_extract(
            T2, connection, extract_options[VARIANT], transform_options[VARIANT], return_type,
            configs = configs,
            options = {
                "RequestEach": {
                    "request_delay": request_delay,
                    "tqdm_options": {"disable": (not progress)}
                }
            },
        )).extract(extra_product_no, shop_no)

    if return_type == "raw":
        return {"product": pages, "product_variant": (variants + results)}

    table = merged_table or "cafe24_product_option"
    keyword = f"INSERT INTO {table}" if connection.table_exists(table) else f"CREATE TABLE {table} AS"

    from textwrap import dedent
    connection.execute(
        dedent(f"""{keyword}
        SELECT
            P.mall_id,
            P.product_no,
            V.variant_code,
            P.custom_product_code,
            P.model_name,
            P.product_name,
            concat_ws(' / ', V.option_name1, V.option_name2, V.option_name3) AS option_name,
            COALESCE(V.image_url, P.image_url) AS image_url,
            P.display AND V.display AS display,
            P.selling AND V.selling AS selling,
            P.sold_out,
            P.price,
            V.option_price,
            P.retail_price,
            COALESCE(V.supply_price, P.supply_price) AS supply_price,
            P.margin_rate,
            P.tags,
            V.display_order,
            P.register_dt,
            P.modify_dt
        FROM {product_table} AS P
        LEFT JOIN {variant_table} AS V
            ON (P.mall_id = V.mall_id) AND (P.product_no = V.product_no)
        WHERE P.mall_id = '{mall_id}'
        """))


@with_duckdb_connection(table="cafe24_category")
def category(
        mall_id: str,
        client_id: str,
        client_secret: str,
        token_info: dict[str, str] | str | Path,
        category_depth: int | None = None,
        parent_category_no: int | None = None,
        shop_no: int = 1,
        page_size: int = 100,
        offset: int = 0,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int | tuple[int, int] = 0.5,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | list[dict] | None:
    """카페24 상품분류 목록을 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: cafe24_category`

    Parameters
    ----------
    mall_id: str
        카페24 쇼핑몰 아이디
    client_id: str
        카페24 앱 클라이언트 아이디
    client_secret: str
        카페24 앱 클라이언트 시크릿
    token_info: dict[str, str] | str | Path
        카페24 액세스 토큰을 포함하는 OAuth 응답 결과가 기록된 파일 경로 또는 객체.
        `access_token`, `refresh_token` 키를 반드시 포함해야 하며,
        `expires_at` 값에 따라 액세스 토큰 만료 여부를 판단해 갱신한다.
    category_depth: int | None
        분류 깊이. `1`부터 `4`까지 입력할 수 있다. 생략하면 전체 깊이를 조회한다.
    parent_category_no: int | None
        상위 분류번호. 대분류만 검색하고자 할 경우 `1`을 입력한다.
    shop_no: int
        멀티쇼핑몰 번호. 기본값은 `1`
    page_size: int
        조회결과 최대건수. 기본값은 `100`, 최대 `100`
    offset: int
        조회결과 시작위치. 기본값은 `0`, 최대 `8000`
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        커서별 요청 간 대기 시간(초). 기본값은 `0.5`
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
    from linkmerce.core.cafe24.api.product.extract import Category
    from linkmerce.core.cafe24.api.product.transform import Category as T
    return Category(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(mall_id, client_id, client_secret, token_info),
        options = {"CursorAll": {"request_delay": request_delay}},
    )).extract(category_depth, parent_category_no, shop_no, page_size, offset)


@with_duckdb_connection(tables={
    "order": "cafe24_order",
    "delivery": "cafe24_order_delivery",
    "detail": "cafe24_order_detail",
    "option": "cafe24_order_option",
})
def order(
        mall_id: str,
        client_id: str,
        client_secret: str,
        token_info: dict[str, str] | str | Path,
        start_datetime: dt.datetime | str,
        end_datetime: dt.datetime | str | Literal[":end_of_day:"] = ":end_of_day:",
        date_type: str = "order_date",
        order_status: str | Iterable[str] | None = None,
        shop_no: int = 1,
        embed: str | None = "items,receivers",
        page_size: int = 100,
        offset: int = 0,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int | tuple[int, int] = 0.5,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str, DuckDBResult] | list[dict] | None:
    """카페24 주문 목록을 수집해 DuckDB 테이블에 변환 및 적재한다.

    - **Tables** ( *table_key: table_name (description)* ):
        1. `order: cafe24_order` (주문 정보)
        2. `delivery: cafe24_order_delivery` (주문 배송 정보)
        3. `detail: cafe24_order_detail` (주문 결제 정보)
        4. `option: cafe24_order_option` (주문 품목 정보)

    Parameters
    ----------
    mall_id: str
        카페24 쇼핑몰 아이디
    client_id: str
        카페24 앱 클라이언트 아이디
    client_secret: str
        카페24 앱 클라이언트 시크릿
    token_info: dict[str, str] | str | Path
        카페24 액세스 토큰을 포함하는 OAuth 응답 결과가 기록된 파일 경로 또는 객체.
        `access_token`, `refresh_token` 키를 반드시 포함해야 하며,
        `expires_at` 값에 따라 액세스 토큰 만료 여부를 판단해 갱신한다.
    start_datetime: dt.datetime | str
        검색 시작 일시. `dt.datetime` 객체 또는 ISO 8601 형식의 문자열을 입력한다.
    end_datetime: dt.datetime | str
        검색 종료 일시. `dt.datetime` 객체 또는 ISO 8601 형식의 문자열을 입력한다.
            - `":end_of_day:"`: `start_datetime`의 하루 중 마지막 시점 (기본값)
    date_type: str
        검색날짜 유형. `date_type` 속성의 키를 전달할 수 있다. 기본값은 주문일(`"order_date"`)
    order_status: str | Iterable[str] | None
        주문상태. `order_status` 속성의 키를 단일 값 또는 배열로 전달할 수 있다. 생략하면 전체 상태를 조회한다.
    shop_no: int
        멀티쇼핑몰 번호. 기본값은 `1`
    embed: str | None
        함께 조회할 하위 리소스. 쉼표(,)로 구분하여 여러 개를 지정할 수 있다.
        - 사용 가능한 값: items, receivers, buyer, benefits, coupons, return, cancellation, exchange, refunds
    page_size: int
        조회결과 최대건수. 기본값은 `100`, 최대 `1000`
    offset: int
        조회결과 시작위치. 기본값은 `0`, 최대 `15000`
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        조회 기간 및 커서별 요청 간 대기 시간(초). 기본값은 `0.5`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    dict[str, DuckDBResult] | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 모든 테이블 조회 결과를 `{table_key: list[tuple]}` 구조로 반환한다.
            - `"json"`: 모든 테이블 조회 결과를 `{table_key: list[dict]}` 구조로 반환한다. (기본값)
            - `"parquet"`: 모든 테이블 조회 결과를 `{table_key: Parquet 바이너리}` 구조로 반환한다.
            - `"raw"`: 데이터 수집 후 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.cafe24.api.order.extract import Order
    from linkmerce.core.cafe24.api.order.transform import Order as T
    return Order(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(mall_id, client_id, client_secret, token_info),
        options = {"CursorAll": {"request_delay": request_delay}},
    )).extract(start_datetime, end_datetime, date_type, order_status, shop_no, embed, page_size, offset)


@with_duckdb_connection(table="cafe24_order_time")
def order_time(
        mall_id: str,
        client_id: str,
        client_secret: str,
        token_info: dict[str, str] | str | Path,
        start_datetime: dt.datetime | str,
        end_datetime: dt.datetime | str | Literal[":end_of_day:"] = ":end_of_day:",
        date_type: str = "order_date",
        order_status: str | Iterable[str] | None = None,
        shop_no: int = 1,
        embed: str | None = "items",
        page_size: int = 100,
        offset: int = 0,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int | tuple[int, int] = 0.5,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | list[dict] | None:
    """카페24 주문 품목별 처리 일시를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: cafe24_order_time`

    Parameters
    ----------
    mall_id: str
        카페24 쇼핑몰 아이디
    client_id: str
        카페24 앱 클라이언트 아이디
    client_secret: str
        카페24 앱 클라이언트 시크릿
    token_info: dict[str, str] | str | Path
        카페24 액세스 토큰을 포함하는 OAuth 응답 결과가 기록된 파일 경로 또는 객체.
        `access_token`, `refresh_token` 키를 반드시 포함해야 하며,
        `expires_at` 값에 따라 액세스 토큰 만료 여부를 판단해 갱신한다.
    start_datetime: dt.datetime | str
        검색 시작 일시. `dt.datetime` 객체 또는 ISO 8601 형식의 문자열을 입력한다.
    end_datetime: dt.datetime | str
        검색 종료 일시. `dt.datetime` 객체 또는 ISO 8601 형식의 문자열을 입력한다.
            - `":end_of_day:"`: `start_datetime`의 하루 중 마지막 시점 (기본값)
    date_type: str
        검색날짜 유형. `date_type` 속성의 키를 전달할 수 있다. 기본값은 주문일(`"order_date"`)
    order_status: str | Iterable[str] | None
        주문상태. `order_status` 속성의 키를 단일 값 또는 배열로 전달할 수 있다. 생략하면 전체 상태를 조회한다.
    shop_no: int
        멀티쇼핑몰 번호. 기본값은 `1`
    embed: str | None
        함께 조회할 하위 리소스. 쉼표(,)로 구분하여 여러 개를 지정할 수 있다.
        - 사용 가능한 값: items, receivers, buyer, benefits, coupons, return, cancellation, exchange, refunds
    page_size: int
        조회결과 최대건수. 기본값은 `100`, 최대 `1000`
    offset: int
        조회결과 시작위치. 기본값은 `0`, 최대 `15000`
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        조회 기간 및 커서별 요청 간 대기 시간(초). 기본값은 `0.5`
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
    from linkmerce.core.cafe24.api.order.extract import Order
    from linkmerce.core.cafe24.api.order.transform import OrderTime as T
    return Order(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(mall_id, client_id, client_secret, token_info),
        options = {"CursorAll": {"request_delay": request_delay}},
    )).extract(start_datetime, end_datetime, date_type, order_status, shop_no, embed, page_size, offset)


@with_duckdb_connection(tables={
    "order": "cafe24_order",
    "delivery": "cafe24_order_delivery",
    "detail": "cafe24_order_detail",
    "option": "cafe24_order_option",
})
def order_detail(
        mall_id: str,
        client_id: str,
        client_secret: str,
        token_info: dict[str, str] | str | Path,
        order_id: str | Iterable[str],
        shop_no: int = 1,
        embed: str | None = "items,receivers",
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int | tuple[int, int] = 0.5,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str, DuckDBResult] | dict | list[dict] | None:
    """카페24 주문번호별 주문 상세 내역을 수집해 DuckDB 테이블에 변환 및 적재한다.

    - **Tables** ( *table_key: table_name (description)* ):
        1. `order: cafe24_order` (주문 정보)
        2. `delivery: cafe24_order_delivery` (주문 배송 정보)
        3. `detail: cafe24_order_detail` (주문 결제 정보)
        4. `option: cafe24_order_option` (주문 품목 정보)

    Parameters
    ----------
    mall_id: str
        카페24 쇼핑몰 아이디
    client_id: str
        카페24 앱 클라이언트 아이디
    client_secret: str
        카페24 앱 클라이언트 시크릿
    token_info: dict[str, str] | str | Path
        카페24 액세스 토큰을 포함하는 OAuth 응답 결과가 기록된 파일 경로 또는 객체.
        `access_token`, `refresh_token` 키를 반드시 포함해야 하며,
        `expires_at` 값에 따라 액세스 토큰 만료 여부를 판단해 갱신한다.
    order_id: str | Iterable[str]
        주문번호. 단일 값 또는 배열
    shop_no: int
        멀티쇼핑몰 번호. 기본값은 `1`
    embed: str
        함께 조회할 하위 리소스. 쉼표(,)로 구분하여 여러 개를 지정할 수 있다.
        - 사용 가능한 값: items, receivers, buyer, benefits, coupons, return, cancellation, exchange, refunds
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        주문별 요청 간 대기 시간(초). 기본값은 `0.5`
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
            - `"raw"`: 데이터 수집 후 `dict | list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.cafe24.api.order.extract import OrderDetail
    from linkmerce.core.cafe24.api.order.transform import OrderDetail as T
    return OrderDetail(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(mall_id, client_id, client_secret, token_info),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(order_id, shop_no, embed)


@with_duckdb_connection(table="cafe24_order_change")
def order_change(
        mall_id: str,
        client_id: str,
        client_secret: str,
        token_info: dict[str, str] | str | Path,
        start_datetime: dt.datetime | str,
        end_datetime: dt.datetime | str | Literal[":end_of_day:"] = ":end_of_day:",
        shop_no: int = 1,
        page_size: int = 100,
        offset: int = 0,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int | tuple[int, int] = 0.5,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | list[dict] | None:
    """카페24 품목별 상태 변경 주문 목록을 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: cafe24_order_change`

    Parameters
    ----------
    mall_id: str
        카페24 쇼핑몰 아이디
    client_id: str
        카페24 앱 클라이언트 아이디
    client_secret: str
        카페24 앱 클라이언트 시크릿
    token_info: dict[str, str] | str | Path
        카페24 액세스 토큰을 포함하는 OAuth 응답 결과가 기록된 파일 경로 또는 객체.
        `access_token`, `refresh_token` 키를 반드시 포함해야 하며,
        `expires_at` 값에 따라 액세스 토큰 만료 여부를 판단해 갱신한다.
    start_datetime: dt.datetime | str
        검색 시작 일시. `dt.datetime` 객체 또는 ISO 8601 형식의 문자열을 입력한다.
    end_datetime: dt.datetime | str
        검색 종료 일시. `dt.datetime` 객체 또는 ISO 8601 형식의 문자열을 입력한다.
            - `":end_of_day:"`: `start_datetime`의 하루 중 마지막 시점 (기본값)
    shop_no: int
        멀티쇼핑몰 번호. 기본값은 `1`
    page_size: int
        조회결과 최대건수. 기본값은 `100`, 최대 `1000`
    offset: int
        조회결과 시작위치. 기본값은 `0`, 최대 `15000`
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        조회 기간 및 커서별 요청 간 대기 시간(초). 기본값은 `0.5`
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
    DuckDBResult | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.cafe24.api.order.extract import OrderChange
    from linkmerce.core.cafe24.api.order.transform import OrderChange as T
    return OrderChange(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(mall_id, client_id, client_secret, token_info),
        options = {"CursorAll": {"request_delay": request_delay}},
    )).extract(start_datetime, end_datetime, shop_no, page_size, offset)
