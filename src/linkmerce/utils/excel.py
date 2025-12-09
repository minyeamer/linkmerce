from __future__ import annotations

from typing import TypeVar, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from openpyxl import Workbook, _ZipFileFileProtocol
    from openpyxl.worksheet.worksheet import Worksheet
    from openpyxl.cell.cell import Cell

Column = TypeVar("Column", int, str)
StyleConfig = TypeVar("StyleConfig", bound=dict[str,dict])


def filter_warnings():
    import warnings
    warnings.filterwarnings("ignore", module="openpyxl.*")


def to_unique_headers(headers: list[str]) -> list[str]:
    unique = list()
    for header in headers:
        header_str, suffix = str(header), 1
        while header_str in unique:
            header_str = f"{header}_{suffix}"
            suffix += 1
        unique.append(header_str)
    return unique


def csv2json(
        io: _ZipFileFileProtocol,
        header: int = 0,
        delimiter: str = ",",
        lineterminator: str = "\r\n",
        encoding: str | None = "utf-8",
    ) -> list[dict]:
    import os
    if isinstance(io, str) and os.path.exists(io):
        with open(io, 'r', encoding=encoding) as file:
            csv2json(file, header)

    import csv
    if isinstance(io, bytes):
        from io import BytesIO, TextIOWrapper
        io = TextIOWrapper(BytesIO(io), encoding=encoding)
    rows = list(csv.reader(io, delimiter=delimiter, lineterminator=lineterminator))
    header_row = to_unique_headers(rows[header])
    return [dict(zip(header_row, row)) for row in rows[(header+1):]]


def excel2json(
        io: _ZipFileFileProtocol,
        sheet_name: str | None = None,
        header: int = 1,
        warnings: bool = True
    ) -> list[dict]:
    from openpyxl import load_workbook
    from io import BytesIO
    if not warnings:
        filter_warnings()

    wb = load_workbook(BytesIO(io) if isinstance(io, bytes) else io)
    ws = wb.active if sheet_name is None else wb[sheet_name]

    headers = to_unique_headers([cell.value for cell in next(ws.iter_rows(min_row=header, max_row=header))])
    return [dict(zip(headers, row)) for row in ws.iter_rows(min_row=header+1, values_only=True)]


def json2excel(
        obj: list[dict] | dict[str,list[dict]],
        sheet_name: str = "Sheet1",
        header: bool = True,
        header_style: StyleConfig | Literal["yellow"] = "yellow",
        column_style: dict[Column,StyleConfig] = dict(),
        row_style: dict[int,StyleConfig] = dict(),
        auto_link: list[Column] = list(),
        auto_width: list[Column] = list(),
        overflow: bool = False,
        wrap_text: bool = False,
        row_height: float | None = 16.5,
        freeze_panes: str | None = "A2",
    ) -> Workbook:
    from openpyxl import Workbook
    wb = Workbook()
    obj = {sheet_name: obj} if isinstance(obj, list) else obj
    kwargs = dict(
        column_style=column_style, row_style=row_style, auto_link=auto_link, auto_width=auto_width,
        overflow=overflow, wrap_text=wrap_text, row_height=row_height, freeze_panes=freeze_panes)

    for index, (name, values) in enumerate(obj.items()):
        _json2sheet(wb, index, values, name, header, header_style, **kwargs)
    return wb


def _json2sheet(
        wb: Workbook,
        index: int,
        values: list[dict],
        sheet_name: str = "Sheet1",
        header: bool = True,
        header_style: StyleConfig | Literal["yellow"] = "yellow",
        **kwargs
    ):
    if index == 0:
        ws = wb.active
        ws.title = sheet_name
    else:
        ws = wb.create_sheet(sheet_name)

    if not values:
        return

    headers = list(values[0].keys())
    if header:
        ws.append(headers)
    for row_data in values:
        ws.append([row_data.get(header, None) for header in headers])

    if not isinstance(header_style, dict):
        header_style = _yellow_header() if header_style == "yellow" else dict()

    style_sheet(ws, header, header_style, **kwargs)


def style_sheet(
        ws: Worksheet,
        header: bool = True,
        header_style: StyleConfig = dict(),
        column_style: dict[Column,StyleConfig] = dict(),
        row_style: dict[int,StyleConfig] = dict(),
        auto_link: list[Column] = list(),
        auto_width: list[Column] = list(),
        overflow: bool = False,
        wrap_text: bool = False,
        row_height: float | None = 16.5,
        freeze_panes: str | None = "A2",
    ) -> Worksheet:
    from openpyxl.styles import Alignment, Font
    from openpyxl.utils import get_column_letter
    HEADER = 1
    headers = [cell.value for cell in ws[HEADER]] if header else list()

    def get_column_index(column: Column) -> int:
        if isinstance(column, int):
            return column
        elif isinstance(column, str):
            if column.startswith('!'):
                exclude, column = -1, column[1:]
            else:
                exclude = 1
            if column in headers:
                return (headers.index(column) + 1) * exclude
        return None

    def build_column_indices(indices: list[Column]) -> list[Column]:
        columns = list(range(1, ws.max_column+1))
        if not indices:
            return columns

        plus, minus = list(), list()
        for index in map(get_column_index, indices):
            if isinstance(index, int):
                (minus if index < 0 else plus).append(abs(index))

        if plus:
            return plus
        elif minus:
            return [index for index in columns if index not in minus]
        else:
            return columns

    def get_cell_width(value: str) -> int:
        try:
            # 한글: 1.8배, 공백: 1.2배, 영문/숫자: 1배
            return sum(1.8 if ord(c) > 12799 else 1.2 if c.isspace() else 1 for c in str(value or ''))
        except:
            return 0

    column_style = {get_column_index(column): style for column, style in column_style.items()}

    if (not wrap_text) and isinstance(row_height, float):
        for row in range(1, ws.max_row + 1):
            ws.row_dimensions[row].height = row_height

    auto_link = build_column_indices(auto_link)
    auto_width = build_column_indices(auto_width)

    for col_idx, column in enumerate(ws.columns, start=1):
        auto_link_ = (col_idx in auto_link)
        auto_width_ = (col_idx in auto_width)
        max_width = 0

        for row_idx, cell in enumerate(column, start=1):
            value = cell.value

            if auto_link_ and str(value).startswith("https://"):
                cell.hyperlink = value
                cell.font = Font(color="0000FF", underline="single")

            if auto_width_:
                max_width = max(max_width, get_cell_width(value))

            if wrap_text or ((not overflow) and isinstance(row_height, float)):
                cell.alignment = Alignment(wrap_text=True)

            if header and (row_idx == HEADER):
                if header_style:
                    style_cell(cell, **header_style)
            elif col_idx in column_style:
                style_cell(cell, **column_style[col_idx])
            elif row_idx in row_style:
                style_cell(cell, **row_style[row_idx])

        if auto_width_:
            width = max(min(max_width + 2, 25), 8.38)
            ws.column_dimensions[get_column_letter(column[0].column)].width = width

    if freeze_panes:
        ws.freeze_panes = freeze_panes


def style_cell(
        cell: Cell,
        align: dict = dict(),
        border: dict = dict(),
        fill: dict = dict(),
        font: dict = dict(),
        number_format: str | None = None,
        **kwargs
    ):
    from openpyxl.styles import Alignment, Border, Side, PatternFill, Font

    if align:
        cell.alignment = Alignment(**align)

    if border:
        cell.border = Border(**{k: Side(**v) for k, v in border.items()})

    if fill:
        cell.fill = PatternFill(**fill)

    if font:
        cell.font = Font(**font)

    if number_format is not None:
        cell.number_format = number_format


def _yellow_header() -> StyleConfig:
    return {
        "align": {"horizontal": "center"},
        "fill": {"fgColor": "FFFF00", "fill_type": "solid"},
        "font": {"bold": True, "color": "000000"},
    }
