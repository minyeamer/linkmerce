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
    """구글 클라우드 서비스 계정 인증 정보를 딕셔너리로 관리하는 클래스."""

    def __init__(self, info: JsonString | Path | dict[str, str]):
        super().__init__(self.read_account(info))

    def read_account(self, info: JsonString | Path | dict[str, str]) -> dict:
        """JSON 문자열, 파일 경로, 또는 딕셔너리에서 서비스 계정 정보를 읽는다."""
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
        records: list[dict[_KT, _VT]],
        filter_headers: list[_KT] | None = None,
    ) -> list[dict[_KT, _VT]] | list[_VT]:
    """워크시트 값을 파이썬 객체(날짜, 블리언, 백분율 등)로 변환한다."""
    import datetime as dt
    import re

    def to_python_object(value: Any) -> Any:
        if isinstance(value, str):
            if value == "TRUE":
                return True
            elif value == "FALSE":
                return False
            elif re.match(r"^\d+(\.\d*)?%$", value):
                return float(value[:-1]) / 100
            elif re.match(r"^\d{4}-\d{2}-\d{2}", value):
                if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
                    return dt.datetime.strptime(value, "%Y-%m-%d").date()
                elif re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", value):
                    return dt.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                elif re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}", value):
                    return dt.datetime.strptime(value, "%Y-%m-%d %H:%M")
                elif re.match(r"^\d{4}-\d{2}-\d{2} \d{2}", value):
                    return dt.datetime.strptime(value, "%Y-%m-%d %H")
        return value

    return [{key: to_python_object(row.get(key)) for key in (filter_headers or row.keys())}
            for row in records]


def py2worksheet(
        records: list[dict],
        expected_headers: list[str] | None = None,
        include_header: bool = False,
    ) -> list[tuple]:
    """딕셔너리 리스트에서 날짜 값을 구글 워크시트에 호환되는 정수 도는 실수로 변환하고, 튜플 리스트 형태로 변환한다."""
    if not records:
        return list()
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

    headers = expected_headers if expected_headers else list(records[0].keys())
    rows = [[to_excel_format(row[column]) for column in headers if column in row] for row in records]
    return ([headers]+rows) if include_header else rows


###################################################################
######################### Worksheet Client ########################
###################################################################

class WorksheetClient(Client):
    """구글 워크시트 클라이언트. 워크시트에 대한 읽기 및 쓰기 작업을 지원한다."""

    def __init__(self, account: ServiceAccount, key: str | None = None, sheet: str | None = None):
        """서비스 계정을 초기화하고, 스프레드시트 키와 워크시트 명칭을 입력받는다."""
        self.set_client(account)
        if key is not None:
            self.set_spreadsheet(key)
        if sheet is not None:
            self.set_worksheet(sheet)

    def get_client(self) -> ServiceClient:
        return self.__client

    def set_client(self, account: ServiceAccount):
        """서비스 계정을 초기화한다."""
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
        """스프레드시트 키로 스프레드시트 객체를 생성한다."""
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
        """워크시트 명칭으로 워크시트 객체를 불러온다."""
        self.__sheetname = sheet
        self.__worksheet = self.spreadsheet.worksheet(sheet)

    def worksheet_exists(self, sheet: str) -> bool:
        """워크시트 존재 여부를 확인한다."""
        try:
            self.spreadsheet.worksheet(sheet)
            return True
        except WorksheetNotFound:
            return False

    def clear(self, include_header=False) -> JSONResponse:
        """워크시트의 데이터를 삭제한다. `include_header=False`라면 헤더는 유지한다."""
        if include_header:
            return self.worksheet.clear()
        else:
            last_row = self.count_rows()
            self.worksheet.insert_row(list(), 2)
            return self.worksheet.delete_rows(3, last_row+2)

    ########################### Get Records ###########################

    def get_all_records(
            self,
            head: int = 1,
            expected_headers: Any | None = None,
            filter_headers: list[_KT] | None = None,
            value_render_option: Any | None = None,
            default_blank: str | None = None,
            numericise_ignore: Sequence[int] | bool = list(),
            allow_underscores_in_numeric_literals: bool = False,
            empty2zero: bool = False,
            convert_dtypes: bool = True,
        ) -> list[dict]:
        """워크시트의 모든 데이터를 딕셔너리 리스트로 조회한다."""
        records = self.worksheet.get_all_records(
            head, expected_headers, value_render_option, default_blank,
            self._numericise_ignore(numericise_ignore), allow_underscores_in_numeric_literals, empty2zero)
        if convert_dtypes:
            return worksheet2py(records, filter_headers)
        elif filter_headers is not None:
            return [{key: row.get(key) for key in filter_headers} for row in records]
        else:
            return records

    def count_rows(self, include_header: bool = False) -> int:
        """워크시트의 행 수를 반환한다."""
        return len(self.worksheet.get_values("A:A")) - bool(not include_header)

    def get_header_row(self) -> list[str]:
        """워크시트의 헤더 행을 리스트로 반환한다."""
        return self.worksheet.get_values("1:1")[0]

    def _auto_detect_header(self, columns: list[str]) -> list[int]:
        """헤더 행에서 칼럼명으로 칼럼 인덱스를 자동 감지한다."""
        header = self.get_header_row()
        not_exists = [col for col in columns if col not in header]
        if not_exists:
            raise ValueError(f"Could not found columns in the header row: {', '.join(not_exists)}.")
        return [header.index(col) for col in columns]

    def _numericise_ignore(self, columns: list[str | int] | bool) -> list[int] | list[Literal["all"]]:
        """숫자 변환을 무시할 칼럼 인덱스 리스트를 반환한다."""
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
            row: int | Literal["append", "last"] = "append",
        ) -> JSONResponse:
        """데이터를 워크시트의 지정된 위치에 덮어쓴다."""
        if not records:
            return
        table = py2worksheet(records, expected_headers, include_header)
        if row == "append":
            self.worksheet.add_rows(len(table))
        return self.worksheet.update(table, range_name=(cell if cell else self.ref_cell(col, row)))

    def ref_cell(self, col: str = 'A', row: int | Literal["append", "last"] = "last") -> str:
        """칼럼과 행 번호로 셀 참조 문자열을 생성한다."""
        if row in ("append", "last"):
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
        """워크시트의 기존 데이터를 삭제한 후 새 데이터로 덮어쓴다."""
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
        """테이블 헤더를 워크시트 헤더 순서에 맞춰 재정렬한다."""
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
        """기존 워크시트 데이터와 제공되는 데이터를 `on` 키 기준으로 병합(UPSERT)한다."""
        if not records:
            return
        existing_records = self.get_all_records(**kwargs)
        records = upsert_records(existing_records, records, on)
        return self.overwrite_worksheet(records, expected_headers, include_header, match_header)


def upsert_records(left: list[dict], right: list[dict], on: str | Sequence[str]) -> list[dict]:
    """`on` 키 기준으로 두 딕셔너리 리스트를 UPSERT 병합한다."""
    def key(row: dict) -> Any | tuple:
        return row[on] if isinstance(on, str) else tuple(row[key] for key in on)
    groupby = {key(row): row for row in right}
    records = [dict(row, **groupby.pop(key(row), dict())) for row in left]
    if groupby:
        return records + list(groupby.values())
    else:
        return records
