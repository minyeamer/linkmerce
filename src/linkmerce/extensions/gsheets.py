from __future__ import annotations

from linkmerce.common.extract import Client

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Hashable, Literal, Sequence, TypeVar
    _KT = TypeVar("_KT", Hashable)
    _VT = TypeVar("_VT", Any)
    JsonString = TypeVar("JsonString", str)
    Path = TypeVar("Path", str)

    from gspread import Client as ServiceClient
    from gspread.spreadsheet import Spreadsheet
    from gspread.worksheet import Worksheet, JSONResponse
    from gspread.exceptions import WorksheetNotFound


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


def worksheet2py(
        records: list[dict[_KT,_VT]],
        keys: _KT | list[_KT] | None = None,
    ) -> list[dict[_KT,_VT]] | list[_VT]:
    """Convert worksheet values to python objects."""
    from linkmerce.utils.map import list_apply
    import datetime as dt
    import re

    def to_python_object(value: Any) -> Any:
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

    return list_apply(records, func=to_python_object, keys=keys)


def py2worksheet(
        records: list[dict],
        expected_headers: list[str] | None = None,
        include_header: bool = False,
    ) -> list[tuple]:
    """Convert python objects to Worksheet values without keys."""
    from linkmerce.utils.map import to_csv
    import datetime as dt

    def to_excel_format(value: Any) -> Any:
        if isinstance(value, dt.date):
            offset = 693594
            days = value.toordinal() - offset
            if isinstance(value, dt.datetime):
                seconds = (value.hour*60*60 + value.minute*60 + value.second)/(24*60*60)
                return days + seconds
            else:
                return days
        else:
            return value

    return to_csv(records, to_excel_format, expected_headers, include_header)


###################################################################
######################### Worksheet Client ########################
###################################################################

class WorksheetClient(Client):
    def __init__(self, account: ServiceAccount, key: str | None = None, sheet: str | None = None):
        self.set_client(account)
        if key is not None:
            self.set_spreadsheet(key)
        if sheet is not None:
            self.set_worksheet(sheet)

    def get_client(self) -> ServiceClient:
        return self.__client

    def set_client(self, account: ServiceAccount):
        from gspread import service_account_from_dict
        self.__client = service_account_from_dict(ServiceAccount(account))

    ########################### Spreadsheet ###########################

    @property
    def key(self) -> str:
        return self.get_key()

    @property
    def spreadsheet(self) -> Spreadsheet:
        return self.get_spreadsheet()

    def get_key(self) -> str:
        return self.__key

    def get_spreadsheet(self) -> Spreadsheet:
        return self.__spreadsheet

    def set_spreadsheet(self, key: str):
        self.__key = key
        self.__spreadsheet = self.get_client().open_by_key(key)

    ############################ Worksheet ############################

    @property
    def sheetname(self) -> str:
        return self.get_sheetname()

    @property
    def worksheet(self) -> Worksheet:
        return self.get_worksheet()

    def get_sheetname(self) -> str:
        return self.__sheetname

    def get_worksheet(self) -> Worksheet:
        return self.__worksheet

    def set_worksheet(self, sheet: str):
        self.__sheetname = sheet
        self.__worksheet = self.spreadsheet.worksheet(sheet)

    def worksheet_exists(self, sheet: str) -> bool:
        try:
            self.spreadsheet.worksheet(sheet)
            return True
        except WorksheetNotFound:
            return False

    def clear(self, include_header=False) -> JSONResponse:
        if include_header:
            return self.worksheet.clear()
        else:
            last_row = self.count_rows()
            self.worksheet.insert_row([], 2)
            return self.worksheet.delete_rows(3, last_row+2)

    ########################### Get Records ###########################

    def get_all_records(
            self,
            head: int = 1,
            expected_headers: Any | None = None,
            filter_headers: _KT | list[_KT] | None = None,
            value_render_option: Any | None = None,
            default_blank: str | None = None,
            numericise_ignore: Sequence[int] | bool = list(),
            allow_underscores_in_numeric_literals: bool = False,
            empty2zero: bool = False,
            convert_dtypes: bool = True,
        ) -> list[dict]:
        records = self.worksheet.get_all_records(
            head, expected_headers, value_render_option, default_blank,
            self._numericise_ignore(numericise_ignore), allow_underscores_in_numeric_literals, empty2zero)
        if convert_dtypes:
            return worksheet2py(records, filter_headers)
        else:
            from linkmerce.utils.map import list_get
            return list_get(records, filter_headers) if filter_headers is not None else records

    def count_rows(self, include_header: bool = False) -> int:
        len(self.worksheet.get_values("A:A")) - bool(not include_header)

    def get_header_row(self) -> list[str]:
        return self.worksheet.get_values("1:1")[0]

    def _auto_detect_header(self, columns: list[str]) -> list[int]:
        header = self.get_header_row()
        not_exists = [col for col in columns if col not in header]
        if not_exists:
            raise ValueError(f"Could not found columns in the header row: {', '.join(not_exists)}.")
        return [header.index(col) for col in columns]

    def _numericise_ignore(self, columns: list[str | int] | bool) -> list[int] | list[Literal["all"]]:
        if not columns:
            return list()
        elif isinstance(columns, bool):
            return ["all"]
        elif all(map(lambda x: isinstance(x, str), columns)):
            return self._auto_detect_header(columns)
        else:
            return columns

    ############################## Update #############################

    def update_worksheet(
            self,
            records: list[dict],
            expected_headers: list[str] | None = None,
            include_header: bool = False,
            cell: str = str(),
            col: str = 'A',
            row: int | Literal["append","last"] = "append",
        ) -> JSONResponse:
        if not records:
            return
        table = py2worksheet(records, expected_headers, include_header)
        if row == "append":
            self.worksheet.add_rows(len(table))
        return self.worksheet.update(table, range_name=(cell if cell else self.ref_cell(col, row)))

    def ref_cell(self, col: str = 'A', row: int | Literal["append","last"] = "last") -> str:
        if row in ("append","last"):
            row = self.count_rows(include_header=True) + int(row == "append")
        return f"{col}{row}"

    ############################ Overwrite ############################

    def overwrite_worksheet(
            self,
            records: list[dict],
            expected_headers: list[str] | None = None,
            include_header: bool = False,
            match_header: bool = False,
        ) -> JSONResponse:
        if not records:
            return
        table = py2worksheet(records, expected_headers, include_header=True)
        if match_header:
            table = self._match_table_header(table)
        if not include_header:
            table = table[1:]
        self.clear(include_header)
        return self.worksheet.update(table, range_name=("A1" if include_header else "A2"))

    def _match_table_header(self, table: list[tuple]) -> list[tuple]:
        sheet_header = self.get_header_row()
        table_header = table[0]
        if set(table_header) - set(sheet_header):
            mismatch = ", ".join(sorted(set(table_header) - set(sheet_header)))
            raise ValueError(f"Worksheet header mismatch: {mismatch}.")
        elif sheet_header != table_header:
            reorder = [(sheet_header.index(col) if col in sheet_header else None) for col in table_header]
            return [tuple((row[i] if isinstance(i, int) else None) for i in reorder) for row in table]
        else:
            return table

    ############################## Upsert #############################

    def upsert_worksheet(
            self,
            records: list[dict],
            on: str | Sequence[str],
            expected_headers: list[str] | None = None,
            include_header: bool = False,
            match_header: bool = False,
            **kwargs
        ) -> JSONResponse:
        if not records:
            return
        existing_records = self.get_all_records(**kwargs)
        records = upsert_records(existing_records, records, on)
        return self.overwrite_worksheet(records, expected_headers, include_header, match_header)


def upsert_records(left: list[dict], right: list[dict], on: str | Sequence[str]) -> list[dict]:
    def key(row: dict) -> Any | tuple:
        return row[on] if isinstance(on, str) else tuple(row[key] for key in on)
    groupby = {key(row): row for row in right}
    records = [dict(row, **groupby.pop(key(row), dict())) for row in left]
    if groupby:
        return records + list(groupby.values())
    else:
        return records
