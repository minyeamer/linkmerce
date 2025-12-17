from __future__ import annotations

from linkmerce.common.api import run_with_duckdb, update_options

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


def get_module(name: str) -> str:
    return (".cj.eflexs" + name) if name.startswith('.') else name


def stock(
        userid: str,
        passwd: str,
        mail_info: dict,
        customer_id: int | str | Iterable,
        start_date: dt.date | str | Literal[":last_week:"] = ":last_week:",
        end_date: dt.date | str | Literal[":start_date:",":today:"] = ":today:",
        connection: DuckDBConnection | None = None,
        tables: dict | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> JsonObject:
    """`tables = {'default': 'data'}`"""
    # from linkmerce.core.cj.eflexs.stock.extract import Stock
    # from linkmerce.core.cj.eflexs.stock.transform import Stock
    return run_with_duckdb(
        module = get_module(".stock"),
        extractor = "Stock",
        transformer = "Stock",
        connection = connection,
        tables = tables,
        how = "sync",
        return_type = return_type,
        args = (customer_id, start_date, end_date),
        extract_options = update_options(
            extract_options,
            options = dict(RequestEach = dict(request_delay=request_delay, tqdm_options=dict(disable=(not progress)))),
            variables = dict(userid=userid, passwd=passwd, mail_info=mail_info),
        ),
        transform_options = transform_options,
    )
