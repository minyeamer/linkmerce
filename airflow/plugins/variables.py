from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from typing import Any, Sequence
    from pathlib import Path


def _exists(obj: Any, dtype: type,  list: bool = False, dict: bool = False) -> bool:
    if list:
        return _list_exists(obj, dtype)
    elif dict:
        return _dict_exists(obj, dtype)
    else:
        return isinstance(obj, dtype) and bool(obj)


def _list_exists(obj: list[Any], dtype: type) -> bool:
    return isinstance(obj, list) and obj and all([isinstance(e, dtype) for e in obj])


def _dict_exists(obj: dict[str,Any], dtype: type) -> bool:
    return isinstance(obj, dict) and obj and all([isinstance(e, dtype) for e in obj.values()])


###################################################################
############################### Read ##############################
###################################################################

def read(file_path: str | Path, format: Literal["auto","json","yaml"] = "auto") -> dict:
    if format == "auto":
        import os
        return read(file_path, format=os.path.splitext(file_path)[1][1:])
    elif format.lower() == "json":
        return read_json(file_path)
    elif format.lower() in ("yaml","yml"):
        return read_yaml(file_path)
    else:
        raise ValueError("Invalid value for format. Supported formats are: json, yaml.")


def read_json(file_path: str | Path) -> dict:
    import json
    with open(file_path, 'r', encoding="utf-8") as file:
        return json.loads(file.read())


def read_yaml(file_path: str | Path) -> dict:
    import yaml
    with open(file_path, 'r', encoding="utf-8") as file:
        return yaml.safe_load(file.read())


def read_variable(
        name: str,
        path: str | int | Sequence[str | int] = list(),
        format: Literal["auto","json","yaml"] = "auto",
        dtype: type | None = None,
        list: bool = False,
        dict: bool = False,
    ) -> dict | list:
    from airflow.sdk import Variable
    obj = read(Variable.get(name), format)
    for key in ([path] if isinstance(path, (str,int)) else path):
        obj = obj[key]
    if not _exists(obj, dtype, list, dict):
        raise ValueError(f"Unknown {name} type.")
    return obj


def read_service_account() -> dict:
    from airflow.sdk import Variable
    return read_json(Variable.get("service_account"))


###################################################################
############################# Accounts ############################
###################################################################

def read_account(path: str | int | Sequence[str | int]) -> dict:
    return read_variable("accounts", path, dtype=dict)


def list_accounts(path: str | int | Sequence[str | int]) -> list[dict]:
    return read_variable("accounts", path, list=True, dtype=dict)


def map_accounts(path: str | int | Sequence[str | int]) -> dict[str,dict]:
    return read_variable("accounts", path, dict=True, dtype=dict)


###################################################################
############################## Cookies ############################
###################################################################

def read_cookies(path: str | int | Sequence[str | int]) -> str:
    return read_variable("cookies", path, dtype=str)


def list_cookies(path: str | int | Sequence[str | int]) -> list[str]:
    return read_variable("cookies", path, list=True, dtype=str)


def map_cookies(path: str | int | Sequence[str | int]) -> dict[str,str]:
    return read_variable("cookies", path, dict=True, dtype=str)


###################################################################
############################ File Path ############################
###################################################################

def read_file_path(path: str | int | Sequence[str | int]) -> str:
    return read_variable("file_path", path, dtype=str)


def list_file_path(path: str | int | Sequence[str | int]) -> list[str]:
    return read_variable("file_path", path, list=True, dtype=str)


def map_file_path(path: str | int | Sequence[str | int]) -> dict[str,str]:
    return read_variable("file_path", path, dict=True, dtype=str)


###################################################################
############################## Params #############################
###################################################################

def read_params(path: str | int | Sequence[str | int]) -> dict:
    return read_variable("params", path, dtype=dict)


def list_params(path: str | int | Sequence[str | int]) -> list[dict]:
    return read_variable("params", path, list=True, dtype=dict)


def map_params(path: str | int | Sequence[str | int]) -> dict[str,dict]:
    return read_variable("params", path, dict=True, dtype=dict)


###################################################################
############################# Schemas #############################
###################################################################

def read_schema(path: str | int | Sequence[str | int]) -> list[dict]:
    from extensions.bigquery import to_bigquery_schema
    schema = read_variable("schemas", path, dtype=list)
    return _to_bigquery_schema(schema)


def list_schemas(path: str | int | Sequence[str | int]) -> list[list[dict]]:
    from extensions.bigquery import to_bigquery_schema
    schemas = read_variable("schemas", path, list=True, dtype=list)
    return [_to_bigquery_schema(schema) for schema in schemas]


def map_schemas(path: str | int | Sequence[str | int]) -> dict[str,list[dict]]:
    schemas = read_variable("schemas", path, dict=True, dtype=list)
    return {key: _to_bigquery_schema(schema) for key, schema in schemas.items()}


def _to_bigquery_schema(schema: Sequence[dict]) -> list[dict]:
    def map(type: str | None = None, **kwargs) -> dict:
        if type is not None:
            kwargs["field_type"] = type
        return kwargs
    return [map(**field) for field in schema]


###################################################################
############################## Sheets #############################
###################################################################

def read_sheet(path: str | int | Sequence[str | int]) -> list[str,str]:
    return read_variable("sheets", path, dtype=list)


def list_sheets(path: str | int | Sequence[str | int]) -> list[list[str,str]]:
    return read_variable("sheets", path, list=True, dtype=list)


def map_sheets(path: str | int | Sequence[str | int]) -> dict[str,list[str,str]]:
    return read_variable("sheets", path, dict=True, dtype=list)


###################################################################
############################## Tables #############################
###################################################################

def read_table(path: str | int | Sequence[str | int]) -> str:
    return read_variable("tables", path, dtype=str)


def list_tables(path: str | int | Sequence[str | int]) -> list[str]:
    return read_variable("tables", path, list=True, dtype=str)


def map_tables(path: str | int | Sequence[str | int]) -> dict[str,str]:
    return read_variable("tables", path, dict=True, dtype=str)


###################################################################
########################## Google Sheets ##########################
###################################################################

def read_google_sheets(
        path: str | int | Sequence[str | int],
        column: str | Sequence[str],
        head: int = 1,
        expected_headers: Any | None = None,
        value_render_option: Any | None = None,
        default_blank: str | None = None,
        numericise_ignore: Sequence[int] | bool = list(),
        allow_underscores_in_numeric_literals: bool = False,
        empty2zero: bool = False,
        convert_dtypes: bool = True,
    ) -> list | list[list]:
    from extensions.gsheets import get_all_records
    service_account = read_service_account()
    key_sheet = read_sheet(path)
    args = (head, expected_headers, value_render_option, default_blank, numericise_ignore,
            allow_underscores_in_numeric_literals, empty2zero, convert_dtypes)
    records = get_all_records(key_sheet[0], key_sheet[1], *args, account=service_account)
    if isinstance(column, str):
        return [record[column] for record in records]
    else:
        return [[record[col] for record in records] for col in column]
