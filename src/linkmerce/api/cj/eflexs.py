from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject
    from linkmerce.common.load import DuckDBConnection
    import datetime as dt


@with_duckdb_connection(table="eflexs_stock")
def stock(
        userid: str,
        passwd: str,
        mail_info: dict,
        customer_id: int | str | Iterable[int | str],
        start_date: dt.date | str | Literal[":last_week:"] = ":last_week:",
        end_date: dt.date | str | Literal[":start_date:", ":today:"] = ":today:",
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> JsonObject:
    """CJ eFLEXs 재고 검색 결과를 수집하고 `eflexs_stock` 테이블에 적재한다."""
    from linkmerce.core.cj.eflexs.stock.extract import Stock
    from linkmerce.core.cj.eflexs.stock.transform import Stock as T
    return Stock(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = {
            "userid": userid,
            "passwd": passwd,
            "mail_info": mail_info
        },
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(customer_id, start_date, end_date)
