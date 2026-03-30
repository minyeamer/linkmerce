from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import JsonObject
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
    ) -> JsonObject:
    """이카운트 오픈 API에 임의의 요청을 보낸다."""
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
    ) -> JsonObject:
    """이카운트 오픈 API에 테스트 요청을 보낸다."""
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
    ) -> JsonObject:
    """이카운트 품목 리스트 API 조회 결과를 `ecount_product` 테이블에 적재한다."""
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
    ) -> JsonObject:
    """이카운트 재고 현황 API 조회 결과를 `ecount_inventory` 테이블에 적재한다."""
    from linkmerce.core.ecount.api.inventory.extract import Inventory
    from linkmerce.core.ecount.api.inventory.transform import Inventory as T
    return Inventory(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {"com_code": com_code, "userid": userid, "api_key": api_key},
    )).extract(base_date, warehouse_code, product_code, zero_yn, balanced_yn, deleted_yn, safe_yn)
