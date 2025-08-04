from __future__ import annotations

from gspread import service_account_from_dict
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Literal, Sequence, TypeVar
    from gspread.worksheet import Worksheet, JSONResponse
    JsonString = TypeVar("JsonString", str)
    Path = TypeVar("Path", str)


DEFAULT_ACCOUNT = "env/service_account.json"


class ServiceAccount(dict):
    def __init__(self, info: JsonString | Path | dict[str,str]):
        super().__init__(self.read_account(info))

    def read_account(self, info: JsonString | Path | dict[str,str]) -> dict:
        if isinstance(info, dict):
            return info
        elif isinstance(info, str):
            import json
            if info.startswith('{') and info.endswith('}'):
                return json.loads(info)
            else:
                with open(info, 'r', encoding="utf-8") as file:
                    return json.loads(file.read())
        else:
            raise ValueError("Unrecognized service account.")


def open_worksheet(key: str, sheet: str, account: ServiceAccount) -> Worksheet:
    account = account if isinstance(account, ServiceAccount) else ServiceAccount(account)
    gs_acc = service_account_from_dict(account)
    gs = gs_acc.open_by_key(key)
    return gs.worksheet(sheet)


def with_service_account(func):
    @functools.wraps(func)
    def wrapper(*args, account: ServiceAccount | None = None, **kwargs):
        if account is None:
            account = ServiceAccount(DEFAULT_ACCOUNT)
        return func(*args, account=account, **kwargs)
    return wrapper


###################################################################
########################### Get Records ###########################
###################################################################

@with_service_account
def get_all_records(
        key: str,
        sheet: str,
        head: int = 1,
        expected_headers: Any | None = None,
        value_render_option: Any | None = None,
        default_blank: str | None = None,
        numericise_ignore: Sequence[int] | bool = list(),
        allow_underscores_in_numeric_literals: bool = False,
        empty2zero: bool = False,
        convert_dtypes: bool = True,
        *,
        account: ServiceAccount | None = None,
    ) -> list[dict[str,Any]]:
    worksheet = open_worksheet(key, sheet, account)
    return _get_all_records(
        worksheet, head, expected_headers, value_render_option, default_blank, numericise_ignore,
        allow_underscores_in_numeric_literals, empty2zero, convert_dtypes)


def _get_all_records(
        worksheet: Worksheet,
        head: int = 1,
        expected_headers: Any | None = None,
        value_render_option: Any | None = None,
        default_blank: str | None = None,
        numericise_ignore: Sequence[int] | bool = list(),
        allow_underscores_in_numeric_literals: bool = False,
        empty2zero: bool = False,
        convert_dtypes: bool = True,
    ) -> list[dict[str,Any]]:
    if isinstance(numericise_ignore, bool):
        numericise_ignore = ["all"] if numericise_ignore else list()
    records = worksheet.get_all_records(
        head, expected_headers, value_render_option, default_blank, numericise_ignore,
        allow_underscores_in_numeric_literals, empty2zero)
    return _convert_dtypes(records) if convert_dtypes else records


def _convert_dtypes(records: list[dict[str,Any]]) -> list[dict[str,Any]]:
    import datetime as dt
    import re
    def _convert_dtype(value) -> Any:
        if isinstance(value, str):
            if value == "TRUE":
                return True
            elif value == "FALSE":
                return False
            elif re.match(r"^\d+(\.\d*)?%$", value):
                return float(value) / 100
            elif re.match(r"^\d{4}-\d{2}-\d{2}$", value):
                return dt.datetime.strptime(value, "%Y-%m-%d").date()
            elif re.match(r"^\d{4}-\d{2}-\d{2}$ \d{2}:\d{2}:\d{2}", value):
                return dt.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        return value

    return [{key: _convert_dtype(value) for key, value in record.items()} for record in records]


###################################################################
########################## Insert Records #########################
###################################################################

@with_service_account
def insert_all_records(
        key: str,
        sheet: str,
        data: list[dict],
        include_header: bool = False,
        col: str = 'A',
        row: int = 0,
        cell: str = str(),
        clear: bool = False,
        clear_header: bool = False,
        *,
        account: ServiceAccount | None = None,
    ) -> JSONResponse:
    if not data:
        return
    worksheet = open_worksheet(key, sheet, account)
    return _insert_all_records(worksheet, data, include_header, col, row, cell, clear, clear_header)


@with_service_account
def overwrite_all_records(
        key: str,
        sheet: str,
        data: list[dict],
        include_header: bool = False,
        col: str = 'A',
        row: int = 0,
        cell: str = str(),
        clear_header: bool = False,
        *,
        account: ServiceAccount | None = None,
    ) -> JSONResponse:
    if not data:
        return
    worksheet = open_worksheet(key, sheet, account)
    success = False
    existing_data = _get_all_records(worksheet)

    try:
        response = _insert_all_records(worksheet, data, include_header, col, row, cell, clear=True, clear_header=clear_header)
        success = True
        return response
    finally:
        if not success:
            _insert_all_records(worksheet, existing_data, cell="A1", clear=True, clear_header=False)


@with_service_account
def upsert_all_records(
        key: str,
        sheet: str,
        data: list[dict],
        by: str | Sequence[str],
        agg: str | dict[str,Literal["first","count","sum","avg","min","max"]],
        include_header: bool = False,
        col: str = 'A',
        row: int = 0,
        cell: str = str(),
        clear_header: bool = False,
        *,
        account: ServiceAccount | None = None,
    ) -> JSONResponse:
    if not data:
        return
    worksheet = open_worksheet(key, sheet, account)
    success = False

    from linkmerce.utils.duckdb import groupby
    existing_data = _get_all_records(worksheet)
    combined_data = groupby(data + existing_data, by, agg)

    try:
        response = _insert_all_records(worksheet, combined_data, include_header, col, row, cell, clear=True, clear_header=clear_header)
        success = True
        return response
    finally:
        if not success:
            _insert_all_records(worksheet, existing_data, cell="A1", clear=True, clear_header=False)


def _insert_all_records(
        worksheet: Worksheet,
        data: list[dict],
        include_header: bool = False,
        col: str = 'A',
        row: int = 0,
        cell: str = str(),
        clear: bool = False,
        clear_header: bool = False,
    ) -> JSONResponse:
    table = _to_table(data, include_header)
    if clear:
        _clear_worksheet(worksheet, clear_header)
    if not cell:
        cell = col + str(row if row else len(worksheet.get_all_records())+2)
        worksheet.add_rows(len(table))
    return worksheet.update(cell, table)


def _to_table(data: list[dict], include_header: bool = False) -> list[list]:
    import datetime as dt
    def _to_excel_format(value: Any) -> Any:
        if value is None:
            return None
        elif isinstance(value, dt.date):
            offset = 693594
            days = value.toordinal() - offset
            if isinstance(value, dt.datetime):
                seconds = (value.hour*60*60 + value.minute*60 + value.second)/(24*60*60)
                return days + seconds
            else:
                return days
        else:
            return value

    header = [list(data[0].keys())] if include_header and data else list()
    return header + [[_to_excel_format(value) for value in __m.values()] for __m in data]


###################################################################
######################### Clear Worksheet #########################
###################################################################

@with_service_account
def clear_worksheet(
        key: str,
        sheet: str,
        include_header=False,
        *,
        account: ServiceAccount | None = None,
    ) -> JSONResponse:
    worksheet = open_worksheet(key, sheet, account)
    return _clear_worksheet(worksheet, include_header)


def _clear_worksheet(worksheet: Worksheet, include_header=False) -> JSONResponse:
    if include_header:
        return worksheet.clear()
    else:
        last_row = len(worksheet.get_all_records())+1
        worksheet.insert_row([], 2)
        return worksheet.delete_rows(3, last_row+2)
