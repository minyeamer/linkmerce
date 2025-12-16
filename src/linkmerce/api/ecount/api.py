from __future__ import annotations

from linkmerce.common.api import run_with_duckdb, update_options

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


def get_module(name: str) -> str:
    return (".ecount.api" + name) if name.startswith('.') else name


def request(
        com_code: int | str,
        userid: str,
        api_key: str,
        path: str,
        body: dict | None = None,
        extract_options: dict = dict(),
        **kwargs
    ) -> JsonObject:
    from linkmerce.core.ecount.api.common import EcountRequestAPI
    extractor = EcountRequestAPI(**update_options(
        extract_options,
        variables = dict(com_code=com_code, userid=userid, api_key=api_key),
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
    from linkmerce.core.ecount.api.common import EcountTestAPI
    extractor = EcountTestAPI(**update_options(
        extract_options,
        variables = dict(com_code=com_code, userid=userid, api_key=api_key),
    ))
    return extractor.extract(path, body)


def product(
        com_code: int | str,
        userid: str,
        api_key: str,
        product_code: str | None = None,
        comma_yn: bool = True,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.ecount.api.product.extract import Product
    # from linkmerce.core.ecount.api.product.transform import Product
    return run_with_duckdb(
        module = get_module(".product"),
        extractor = "Product",
        transformer = "Product",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (product_code, comma_yn),
        extract_options = update_options(
            extract_options,
            variables = dict(com_code=com_code, userid=userid, api_key=api_key),
        ),
        transform_options = transform_options,
    )


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
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.ecount.api.inventory.extract import Inventory
    # from linkmerce.core.ecount.api.inventory.transform import Inventory
    return run_with_duckdb(
        module = get_module(".inventory"),
        extractor = "Inventory",
        transformer = "Inventory",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (base_date, warehouse_code, product_code, zero_yn, balanced_yn, deleted_yn, safe_yn),
        extract_options = update_options(
            extract_options,
            variables = dict(com_code=com_code, userid=userid, api_key=api_key),
        ),
        transform_options = transform_options,
    )
