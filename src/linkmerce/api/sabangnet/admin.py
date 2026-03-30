from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


def _get_login_configs(userid: str, passwd: str, domain: int) -> dict:
    """사방넷 로그인에 필요한 설정을 구성한다."""
    return _get_login_configs(userid, passwd, domain)


def login(userid: str, passwd: str) -> dict[str, str]:
    """사방넷에 로그인하고 쿠키와 인증 토큰을 반환한다."""
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
        order_status: Sequence[str] = list(),
        shop_id: str = str(),
        sort_type: str = "ord_no_asc",
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """사방넷 주문서 확인 처리 조회 결과를 `sabangnet_order_detail` 테이블에 적재한다."""
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


@with_duckdb_connection(tables={
    "order": "sabangnet_order",
    "option": "sabangnet_option",
    "invoice": "sabangnet_invoice",
    "dispatch": "sabangnet_dispatch",
})
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
        order_status: Sequence[str] = list(),
        shop_id: str = str(),
        sort_type: str = "ord_no_asc",
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str, bytes]:
    """사방넷 주문서 확인 처리 다운로드 결과를 `download_type`에 따라 정해진 테이블에 적재한다.

    테이블 키 | 테이블명 | 설명
    - `order` | `sabangnet_order` | 사방넷 주문 내역
    - `option` | `sabangnet_option` | 사방넷 주문 옵션 목록
    - `invoice` | `sabangnet_invoice` | 사방넷 발주 내역
    - `dispatch` | `sabangnet_dispatch` | 사방넷 발송 내역"""
    from linkmerce.core.sabangnet.admin.order.extract import OrderDownload
    from linkmerce.core.sabangnet.admin.order.transform import OrderDownload as T
    from linkmerce.utils.nested import merge
    transform_options = merge(transform_options or dict(), {"download_type": download_type})
    return OrderDownload(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_login_configs(userid, passwd, domain),
    )).extract(download_no, start_date, end_date, date_type, order_seq, order_status_div, order_status, shop_id, sort_type)


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
        order_status: Sequence[str] = list(),
        shop_id: str = str(),
        sort_type: str = "ord_no_asc",
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """사방넷 주문서 확인 처리 다운로드 결과로부터 주문 상태에 따른 변경 날짜를 파싱해
    `sabangnet_order_status` 테이블에 적재한다."""
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
    ) -> JsonObject:
    """사방넷 품번코드 매핑 내역을 조회하고 `sabangnet_product_mapping` 테이블에 적재한다."""
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
        query: Sequence[dict],
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """사방넷 단품코드 매핑 내역을 조회하고 `sabangnet_sku_mapping` 테이블에 적재한다."""
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
    "merged": "sabangnet_mapping",
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
    ) -> JsonObject:
    """사방넷 품번코드 매핑 내역과 각 사방넷 단품코드 매핑 내역을 조회해 각각의 테이블에 적재한다.

    테이블 키 | 테이블명 | 설명
    - `product` | `sabangnet_product_mapping` | 사방넷 품번코드 매핑 내역
    - `sku` | `sabangnet_sku_mapping` | 사방넷 단품코드 매핑 내역"""
    PRODUCT, SKU = 0, 1
    results = dict()
    return_type = return_type if return_type == "raw" else "none"

    results["product"] = product_mapping(
        userid, passwd, domain, start_date, end_date, shop_id,
        connection=connection, request_delay=request_delay, progress=progress, return_type=return_type,
        extract_options=extract_options[PRODUCT], transform_options=transform_options[PRODUCT])

    query = "SELECT DISTINCT product_id_shop, shop_id, product_id FROM sabangnet_product_mapping WHERE mapping_count > 0"
    results["sku"] = sku_mapping(
        userid, passwd, domain, connection.fetch_all_to_json(query),
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
    ) -> JsonObject:
    """사방넷 상품 조회 결과를 `sabangnet_product` 테이블에 적재한다."""
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
        product_id: Sequence[str],
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """사방넷 단품 옵션 조회 결과를 `sabangnet_option` 테이블에 적재한다."""
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
    ) -> JsonObject:
    """사방넷 단품 대량 수정 메뉴를 다운로드하여 `sabangnet_option_download` 테이블에 적재한다."""
    from linkmerce.core.sabangnet.admin.product.extract import OptionDownload
    from linkmerce.core.sabangnet.admin.product.transform import OptionDownload as T
    return OptionDownload(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_login_configs(userid, passwd, domain),
    )).extract(start_date, end_date, date_type, sort_type, sort_asc, is_deleted, product_status)


@with_duckdb_connection(table="sabangnet_add_product_group")
def add_product(
        userid: str,
        passwd: str,
        domain: int,
        group_id: Sequence[str] = list(),
        start_date: dt.date | str | Literal[":base_date:", ":today:"] = ":base_date:",
        end_date: dt.date | str | Literal[":start_date:", ":today:"] = ":today:",
        shop_id: str = str(),
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """사방넷 추가상품 관리 메뉴에서 전체 `group_id`를 추출하고,   
    각 사방넷 추가 상품 그룹 내 상품 목록을 조회해 `sabangnet_add_product_group` 테이블에 적재한다."""
    if not group_id:
        from linkmerce.core.sabangnet.admin.product.extract import AddProductGroup
        from linkmerce.core.sabangnet.admin.product.transform import AddProductGroup as T

        groups = AddProductGroup(**prepare_duckdb_extract(
            T, connection, transform_options={"table": "temp_product"}, return_type="json",
            configs = _get_login_configs(userid, passwd, domain),
            options = {
                "PaginateAll": {
                    "request_delay": request_delay,
                    "tqdm_options": {"disable": (not progress)}
                }
            },
        )).extract(start_date, end_date, shop_id)
        connection.execute("DROP TABLE IF EXISTS temp_product")
        group_id = [group["group_id"] for group in groups]

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
