from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence
    from linkmerce.common.extract import JsonObject
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
    """쿠팡 Wing 또는 서플라이어 허브에 로그인하고 쿠키를 반환한다."""
    from linkmerce.core.coupang.wing.common import CoupangLogin
    auth = CoupangLogin()
    cookies = auth.login(userid, passwd, domain, with_token)
    if cookies and save_to:
        with open(save_to, 'w', encoding="utf-8") as file:
            file.write(cookies)
    return cookies


@with_duckdb_connection(tabless={"products": "coupang_product", "details": "coupang_product_detail"})
def product_option(
        cookies: str,
        is_deleted: bool = False,
        see_more: bool = False,
        domain: Literal["wing", "supplier"] = "wing",
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """쿠팡 상품 옵션 목록을 조회한다. `see_more=True`의 경우 상품 상세 정보를 추가로 조회한다.

    테이블 키 | 테이블명 | 설명
    - `products` | `coupang_product` | 쿠팡 상품 옵션 목록
    - `details` | `coupang_product_detail` | 쿠팡 상품 상세 정보"""
    from linkmerce.core.coupang.wing.product.extract import ProductOption
    from linkmerce.core.coupang.wing.product.transform import ProductOption as T

    products = ProductOption(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"domain": domain},
        headers = {"cookies": cookies},
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
            query = "SELECT DISTINCT vendor_inventory_id FROM coupang_product"
            vendor_inventory_id = [row[0] for row in connection.execute(query)[0].fetchall()]

        details = ProductDetail(**prepare_duckdb_extract(
            T, connection, extract_options, transform_options, return_type,
            headers = {"cookies": cookies},
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
        vendor_inventory_id: Sequence[int | str],
        domain: Literal["wing", "supplier"] = "wing",
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """쿠팡 상품 상세 정보를 조회하고 `coupang_product_detail` 테이블에 적재한다."""
    from linkmerce.core.coupang.wing.product.extract import ProductDetail
    from linkmerce.core.coupang.wing.product.transform import ProductDetail as T
    return ProductDetail(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"domain": domain},
        headers = {"cookies": cookies},
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
        request_type = "VENDOR_INVENTORY_ITEM",
        fields: list[str] = list(),
        is_deleted: bool = False,
        vendor_id: str | None = None,
        wait_seconds: int = 60,
        wait_interval: int = 1,
        domain: Literal["wing", "supplier"] = "wing",
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """쿠팡 상품 다운로드 결과를 다운로드하고 `coupang_product_download` 테이블에 적재한다."""
    from linkmerce.core.coupang.wing.product.extract import ProductDownload
    from linkmerce.core.coupang.wing.product.transform import ProductDownload as T
    return ProductDownload(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"domain": domain},
        headers = {"cookies": cookies},
    )).extract(request_type, fields, is_deleted, vendor_id, wait_seconds, wait_interval)


@with_duckdb_connection(table="coupang_rocket_inventory")
def rocket_inventory(
        cookies: str,
        hidden_status: Literal["VISIBLE", "HIDDEN"] | None = None, 
        vendor_id: str | None = None,
        domain: Literal["wing", "supplier"] = "wing",
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """쿠팡 로켓 재고 내역을 수집하고 `coupang_rocket_inventory` 테이블에 적재한다."""
    from linkmerce.core.coupang.wing.product.extract import RocketInventory
    from linkmerce.core.coupang.wing.product.transform import RocketInventory as T
    return RocketInventory(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"domain": domain},
        headers = {"cookies": cookies},
        options = {"CursorAll": {"request_delay": request_delay}},
    )).run(hidden_status, vendor_id, how_to_run="sync")


@with_duckdb_connection(tables={"options": "coupang_rocket_option", "details": "coupang_product_detail"})
def rocket_option(
        cookies: str,
        hidden_status: Literal["VISIBLE", "HIDDEN"] | None = None, 
        vendor_id: str | None = None,
        see_more: bool = False,
        domain: Literal["wing", "supplier"] = "wing",
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """쿠팡 로켓 재고 현황을 조회한다. `see_more=True` 시 상세 정보를 추가 조회한다.

    테이블 키 | 테이블명 | 설명
    - `options` | `coupang_rocket_option` | 쿠팡 상품 옵션 목록
    - `details` | `coupang_product_detail` | 쿠팡 상품 상세 정보"""
    from linkmerce.core.coupang.wing.product.extract import RocketInventory
    from linkmerce.core.coupang.wing.product.transform import RocketOption as T

    product = RocketInventory(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"domain": domain},
        headers = {"cookies": cookies},
        options = {"CursorAll": {"request_delay": request_delay}},
    )).extract(hidden_status, vendor_id)

    if see_more:
        from linkmerce.core.coupang.wing.product.extract import ProductDetail
        from linkmerce.core.coupang.wing.product.transform import ProductDetail as T

        table = "coupang_rocket_option"
        query = "SELECT DISTINCT vendor_inventory_id FROM {}".format(table)
        vendor_inventory_id = [row[0] for row in connection.execute(query)[0].fetchall()]

        return ProductDetail(**prepare_duckdb_extract(
            T, connection, extract_options, transform_options, return_type,
            configs = {"domain": domain},
            headers = {"cookies": cookies},
            options = {
                "RequestEach": {
                    "request_delay": request_delay,
                    "tqdm_options": {"disable": (not progress)}
                }
            },
        )).run(vendor_inventory_id, referer="rfm", how_to_run="sync")
    return product


def summary(
        cookies: str,
        start_from: str,
        end_to: str,
        extract_options: dict = dict(),
    ) -> JsonObject:
    """쿠팡 로켓 손익 현황 요약 데이터를 조회한다."""
    from linkmerce.core.coupang.wing.settlement.extract import Summary
    from linkmerce.utils.nested import merge
    extractor = Summary(**merge(extract_options or dict(), headers={"cookies": cookies}))
    return extractor.extract(start_from, end_to)


@with_duckdb_connection(table="coupang_rocket_settlement")
def rocket_settlement(
        cookies: str,
        start_date: dt.date | str, 
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["PAYMENT", "SALES"] = "SALES",
        vendor_id: str | None = None,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """쿠팡 로켓 정산 현황을 조회하고 `coupang_rocket_settlement` 테이블에 적재한다."""
    from linkmerce.core.coupang.wing.settlement.extract import RocketSettlement
    from linkmerce.core.coupang.wing.settlement.transform import RocketSettlement as T
    return RocketSettlement(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        headers = {"cookies": cookies},
    )).extract(start_date, end_date, date_type, vendor_id)


@with_duckdb_connection(tables={"sales": "coupang_rocket_sales", "shipping": "coupang_rocket_shipping"})
def rocket_settlement_download(
        cookies: str,
        start_date: dt.date | str, 
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["PAYMENT", "SALES"] = "SALES",
        vendor_id: str | None = None,
        wait_seconds: int = 60,
        wait_interval: int = 1,
        progress: bool = True,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> dict[str, JsonObject]:
    """쿠팡 로켓 정산 보고서를 다운로드하고 `report_type`에 따라 각각의 테이블에 적재한다.

    테이블 키 | 테이블명 | 설명
    - `sales` | `coupang_rocket_sales` | 쿠팡 판매 수수료 리포트
    - `shipping` | `coupang_rocket_shipping` | 쿠팡 입출고비/배송비 리포트"""
    from linkmerce.core.coupang.wing.settlement.extract import RocketSettlementDownload
    from linkmerce.core.coupang.wing.settlement.transform import RocketSettlementDownload as T
    return RocketSettlementDownload(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        headers = {"cookies": cookies},
    )).extract(start_date, end_date, date_type, vendor_id, wait_seconds, wait_interval, progress)
