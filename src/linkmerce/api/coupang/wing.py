from __future__ import annotations

from linkmerce.common.api import run, run_with_duckdb

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    from pathlib import Path
    import datetime as dt


def get_module(name: str) -> str:
    return (".coupang.wing" + name) if name.startswith('.') else name


def login(
        userid: str,
        passwd: str,
        domain: Literal["wing","supplier"] = "wing",
        with_token: bool = False,
        save_to: str | Path | None = None,
    ) -> dict[str,str]:
    from linkmerce.core.coupang.wing.common import CoupangLogin
    auth = CoupangLogin()
    credentials = auth.login(userid, passwd, domain, with_token)
    if credentials.get("cookies") and save_to:
        with open(save_to, 'w', encoding="utf-8") as file:
            file.write(credentials["cookies"])
    return credentials


def product_option(
        cookies: str,
        is_deleted: bool = False,
        see_more: bool = False,
        domain: Literal["advertising","domain","wing"] = "advertising",
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 0.1,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.coupang.wing.product.extract import ProductOption, ProductDetail
    # from linkmerce.core.coupang.wing.product.transform import ProductOption, ProductDetail
    common = dict(
        module = get_module(".product"),
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        extract_options = dict(
            extract_options,
            headers = dict(cookies=cookies),
            variables = dict(domain=domain),
        ),
        transform_options = transform_options,
    )

    product = run_with_duckdb(
        extractor = "ProductOption",
        transformer = "ProductOption",
        args = (is_deleted,),
        **common,
    )

    if see_more:
        table = (tables or dict()).get("default", "data")
        query = "SELECT DISTINCT vendor_inventory_id FROM {}".format(table)
        vendor_inventory_id = [row[0] for row in connection.execute(query).fetchall()]

        common["extract_options"]["options"] = dict(RequestEach = dict(request_delay=request_delay))
        return run_with_duckdb(
            extractor = "ProductDetail",
            transformer = "ProductDetail",
            args = (vendor_inventory_id,),
            **common,
        )
    else:
        return product


def product_download(
        cookies: str,
        request_type = "VENDOR_INVENTORY_ITEM",
        fields: list[str] = list(),
        is_deleted: bool = False,
        vendor_id: str | None = None,
        wait_seconds: int = 60,
        wait_interval: int = 1,
        domain: Literal["advertising","domain","wing"] = "advertising",
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.coupang.wing.product.extract import ProductDownload
    # from linkmerce.core.coupang.wing.product.transform import ProductDownload
    return run_with_duckdb(
        module = get_module(".product"),
        extractor = "ProductDownload",
        transformer = "ProductDownload",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (request_type, fields, is_deleted, vendor_id, wait_seconds, wait_interval),
        extract_options = dict(
            extract_options,
            headers = dict(cookies=cookies),
            variables = dict(domain=domain),
        ),
        transform_options = transform_options,
    )


def summary(
        cookies: str,
        start_from: str,
        end_to: str,
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    # from linkmerce.core.coupang.wing.settlement.extract import Summary
    return run(
        module = get_module(".settlement"),
        extractor = "Summary",
        transformer = None,
        how = "sync",
        args = (start_from, end_to),
        extract_options = dict(
            extract_options,
            headers = dict(cookies=cookies),
        ),
        transform_options = transform_options,
    )


def rocket_settlement(
        cookies: str,
        start_date: dt.date | str, 
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["PAYMENT","SALES"] = "SALES",
        vendor_id: str | None = None,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.coupang.wing.settlement.extract import RocketSettlement
    # from linkmerce.core.coupang.wing.settlement.transform import RocketSettlement
    return run_with_duckdb(
        module = get_module(".settlement"),
        extractor = "RocketSettlement",
        transformer = "RocketSettlement",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (start_date, end_date, date_type, vendor_id),
        extract_options = dict(
            extract_options,
            headers = dict(cookies=cookies),
        ),
        transform_options = transform_options,
    )


def rocket_settlement_download(
        cookies: str,
        start_date: dt.date | str, 
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        date_type: Literal["PAYMENT","SALES"] = "SALES",
        vendor_id: str | None = None,
        wait_seconds: int = 60,
        wait_interval: int = 1,
        progress: bool = True,
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> dict[str,JsonObject]:
    """`tables = {'sales': 'coupang_rocket_sales', 'shipping': 'coupang_rocket_shipping'}`"""
    # from linkmerce.core.coupang.wing.settlement.extract import RocketSettlementDownload
    # from linkmerce.core.coupang.wing.settlement.transform import RocketSettlementDownload
    return run_with_duckdb(
        module = get_module(".settlement"),
        extractor = "RocketSettlementDownload",
        transformer = "RocketSettlementDownload",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (start_date, end_date, date_type, vendor_id, wait_seconds, wait_interval, progress),
        extract_options = dict(
            extract_options,
            headers = dict(cookies=cookies),
        ),
        transform_options = transform_options,
    )
