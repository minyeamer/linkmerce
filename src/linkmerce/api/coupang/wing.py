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
        save_to: str | Path | None = None,
        token_to: str | Path | None = None,
    ) -> dict[str,str]:
    from linkmerce.core.coupang.wing.common import CoupangLogin
    auth = CoupangLogin()
    credentials = auth.login(userid, passwd, domain, with_token=bool(token_to))
    if credentials.get("cookies") and save_to:
        with open(save_to, 'w', encoding="utf-8") as file:
            file.write(credentials["cookies"])
    if credentials.get("xsrf_token") and token_to:
        with open(token_to, 'w', encoding="utf-8") as file:
            file.write(credentials["xsrf_token"])
    return credentials


def summary(
        cookies: str,
        xsrf_token: str,
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
            headers = dict(cookies=cookies, xsrf_token=xsrf_token),
        ),
        transform_options = transform_options,
    )


def rocket_settlement(
        cookies: str,
        xsrf_token: str,
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
            headers = dict(cookies=cookies, xsrf_token=xsrf_token),
        ),
        transform_options = transform_options,
    )


def rocket_settlement_download(
        cookies: str,
        xsrf_token: str,
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
            headers = dict(cookies=cookies, xsrf_token=xsrf_token),
        ),
        transform_options = transform_options,
    )
