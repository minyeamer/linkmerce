from __future__ import annotations

import pandas as pd
import os

from typing import Iterable, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Callable, Dict, Hashable, List, Literal, Union
    from io import BytesIO, StringIO
    from pandas._typing import DtypeArg
    IndexLabel = Union[Sequence[Hashable], Hashable]


TABLE_FORMAT = {".xlsx", ".xls", ".xlsm", ".xlsb", ".csv", ".html", ".xml"}


def select_table(data: pd.DataFrame, cols: IndexLabel, dropna: bool=False, reorder: bool=True) -> pd.DataFrame:
    from_cols, to_cols = (data.columns, _to_sequence(cols)) if reorder else (_to_sequence(cols), data.columns)
    if dropna:
        to_cols = [col for col in to_cols if col in from_cols]
    else:
        for col in cols:
            if col not in data:
                data[col] = None
    return data[to_cols]


###################################################################
############################ Read Table ###########################
###################################################################

def read_table(
        io: bytes | str,
        table_format: Literal["excel", "csv", "html", "xml"] | Sequence = "xlsx",
        sheet_name: str | int | List | None = 0,
        header: int | Sequence[int] | None = 0,
        dtype: DtypeArg | None = None,
        engine: Literal["xlrd", "openpyxl", "odf", "pyxlsb", "calamine"] | None = None,
        parse_dates: bool | Sequence[int] | Sequence[Sequence[str] | Sequence[int]] | Dict[str, Sequence[int] | list[str]] = None,
        file_pattern: Dict | None = None,
        **kwargs
    ) -> pd.DataFrame:
    def read_io(io: BytesIO | StringIO, format: Literal["excel", "csv", "html", "xml"]) -> pd.DataFrame:
        if format == "excel":
            if engine == "xlrd":
                from xlrd import open_workbook
                with open_workbook(file_contents=io.getvalue(), logfile=open(os.devnull, 'w')) as wb:
                    return pd.read_excel(wb, engine="xlrd", **kwargs)
            else:
                return pd.read_excel(io, header=header, dtype=dtype, engine=engine, parse_dates=parse_dates, **kwargs)
        elif format == "csv":
            return pd.read_csv(io, header=header, dtype=dtype, parse_dates=parse_dates, **kwargs)
        elif format == "html":
            index = sheet_name if isinstance(sheet_name, int) else 0
            return pd.read_html(io, header=header, parse_dates=parse_dates, **kwargs)[index]
        elif format == "xml":
            return pd.read_xml(io, parse_dates=parse_dates, **kwargs)
        else:
            raise ValueError("Invalid value for table_format. Supported formats are: excel, csv, html, xml.")

    if isinstance(table_format, str):
        return read_io(_read_bytes(io, file_pattern), table_format)
    elif isinstance(table_format, Sequence):
        return _try_sequence(read_io,
            kwargs = dict(format=_to_sequence(table_format)),
            persist = dict(io=_read_bytes(io, file_pattern)))
    else:
        raise TypeError("Invalid type for table_format. A string or sequence type is allowed.")


def _read_bytes(io: bytes | str, file_pattern: Dict | None=None) -> BytesIO | StringIO:
    from io import BytesIO, StringIO
    if isinstance(io, str):
        if (os.path.splitext(io)[1] in TABLE_FORMAT):
            with open(_get_file_path(io, **(file_pattern or dict())), 'rb') as file:
                return BytesIO(file.read())
        else:
            return StringIO(io)
    else:
        return BytesIO(io)


def _get_file_path(io: str, regex: bool=False, index: int=0, **kwargs) -> str:
    if regex:
        import re
        root, pattern = os.path.split(io)
        def search(file_name: str) -> bool:
            return bool(re.search(pattern, file_name))
        filtered = list(filter(search, os.listdir(root or '.')))
        io = os.path.join(root, filtered[index]) if -len(filtered) <= index < len(filtered) else str()
    if os.path.exists(io):
        return io
    else:
        raise ValueError("File path does not exist: {}".format(f"'{io}'" if isinstance(io, str) else "None"))


###################################################################
############################# General #############################
###################################################################

def _to_sequence(cols: IndexLabel) -> Iterable[Hashable]:
    return [cols] if isinstance(cols, str) or not isinstance(cols, Iterable) else cols


def _try_sequence(func: Callable, kwargs: Dict[str,Sequence]=dict(), persist: Dict=dict()) -> Any:
    from itertools import product
    params = list(map(lambda values: dict(zip(kwargs.keys(), values)),list(product(*kwargs.values()))))
    for param in params[:-1]:
        try:
            return func(**param, **persist)
        except:
            pass
    return func(**params[-1], **persist)
