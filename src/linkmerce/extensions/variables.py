from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from typing import Any, Sequence
    from pathlib import Path
    from linkmerce.extensions.gsheets import ServiceAccount, WorksheetClient


DEFAULT_ACCOUNT = "env/service_account.json"
DEFAULT_CREDENTIALSS = "env/credentials.yaml"
DEFAULT_SCHEMAS = "env/schemas.json"
DEFAULT_VARIABLES = "env/variables.yaml"


def exists(obj: Any, dtype: type,  list: bool = False, dict: bool = False) -> bool:
    if list:
        return list_exists(obj, dtype)
    elif dict:
        return dict_exists(obj, dtype)
    else:
        return isinstance(obj, dtype) and bool(obj)


def list_exists(obj: list[Any], dtype: type) -> bool:
    return isinstance(obj, list) and obj and all([isinstance(e, dtype) for e in obj])


def dict_exists(obj: dict[str,Any], dtype: type) -> bool:
    return isinstance(obj, dict) and obj and all([isinstance(e, dtype) for e in obj.values()])


def path_exists(path: str, name: str) -> bool:
    import os
    if not path:
        raise ValueError(f"'{name}' is required.")
    elif not os.path.exists(path):
        raise FileNotFoundError(f"'{name}' does not exists: {path}")
    else:
        return True


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


def read_file(file_path: str | Path) -> str:
    with open(file_path, 'r', encoding="utf-8") as file:
        return file.read()


###################################################################
############################# Variable ############################
###################################################################

def read_variable(
        file_path: str | Path,
        key_path: str | int | Sequence[str | int] = list(),
        format: Literal["auto","json","yaml"] = "auto",
        dtype: type | None = None,
        list: bool = False,
        dict: bool = False,
    ) -> Any | dict | list:
    return parse_variable(read(file_path, format), key_path, dtype, list, dict)


def parse_variable(
        variable: dict | list,
        key_path: str | int | Sequence[str | int] = list(),
        dtype: type | None = None,
        list: bool = False,
        dict: bool = False,
    ) -> dict | list:
    for key in ([key_path] if isinstance(key_path, (str,int)) else key_path):
        variable = variable[key]
    if (dtype is not None) and (not exists(variable, dtype, list, dict)):
        raise ValueError("Invalid variable format.")
    return variable


###################################################################
############################ Variables ############################
###################################################################

def read_variables(
        file_path: str | Path,
        key_path: str | int | Sequence[str | int] = list(),
        format: Literal["auto","json","yaml"] = "auto",
        account: ServiceAccount | None = None,
        credentials_path: str | Path | None = None,
        schemas_path: str | Path | None = None,
        with_table_schema: bool | None = False,
    ) -> dict:
    variables = read_variable(file_path, key_path, format, dtype=dict)
    if ("credentials" in variables) and path_exists(credentials_path, "credentials_path"):
        variables["credentials"] = parse_credentials(credentials_path, variables["credentials"])
    if "sheets" in variables:
        variables.update(parse_sheets(account, variables["sheets"]))
    if ("tables" in variables) and isinstance(with_table_schema, bool):
        variables["tables"] = parse_tables(variables["tables"], schemas_path, with_table_schema)
    return variables


def parse_credentials(credentials_path: str, credentials_info: str | int | Sequence[str | int] = list()) -> dict | list:
    credentials = read_variable(credentials_path, credentials_info, dtype=dict)

    def read_if_path(value: Any) -> Any:
        if isinstance(value, str) and value.startswith("Path(") and value.endswith(")"):
            return read_file(value[5:-1])
        else:
            return value

    if isinstance(credentials, list):
        from linkmerce.utils.map import list_apply
        return list_apply(credentials, read_if_path)
    elif isinstance(credentials, dict):
        return {key: read_if_path(value) for key, value in credentials.items()}
    else:
        raise ValueError("Could not parse the credentials from variables.")


def parse_sheets(account: ServiceAccount, sheets_info: dict | list) -> dict:
    from linkmerce.extensions.gsheets import WorksheetClient
    client = WorksheetClient(account)
    if isinstance(sheets_info, dict):
        if ("key" in sheets_info) and ("sheet" in sheets_info):
            return x if isinstance(x := read_google_sheets(client, **sheets_info), dict) else dict(records=x)
        else:
            return {key: read_google_sheets(client, **info) for key, info in sheets_info.items()}
    elif isinstance(sheets_info, list):
        return [read_google_sheets(client, **info) for info in sheets_info if isinstance(info, dict)]
    else:
        raise ValueError("Could not parse the sheets from variables.")


def parse_tables(
        tables_info: dict[str,dict[str,Any]],
        schemas_path: str | Path | None = None,
        with_table_schema: bool | None = False,
    ) -> dict[str,dict[str,Any]]:
    if not isinstance(tables_info, dict):
        raise ValueError("Could not parse the tables from variables.")
    elif with_table_schema:
        if path_exists(schemas_path, "schemas_path"):
            for db, info in tables_info.copy().items():
                if "schema" in info:
                    tables_info[db]["schema"] = read_variable(schemas_path, info["schema"], dtype=dict, list=True)
            return tables_info
    else:
        return {db: info["table"] for db, info in tables_info.items()}


def read_google_sheets(
        client: WorksheetClient,
        key: str,
        sheet: str,
        column: str | Sequence[str] | None = None,
        axis: Literal[0,"by_col",1,"by_row"] = 0,
        head: int = 1,
        expected_headers: Any | None = None,
        value_render_option: Any | None = None,
        default_blank: str | None = None,
        numericise_ignore: Sequence[int] | bool = list(),
        allow_underscores_in_numeric_literals: bool = False,
        empty2zero: bool = False,
        convert_dtypes: bool = True,
    ) -> dict[str,list] | list[dict]:
    client.set_spreadsheet(key)
    client.set_worksheet(sheet)
    records = client.get_all_records(
        head, expected_headers, (column or None), value_render_option, default_blank,
            numericise_ignore, allow_underscores_in_numeric_literals, empty2zero, convert_dtypes)

    if isinstance(column, str):
        return {column: records}
    elif axis in (0,"by_col"):
        keys = list(records[0].keys())
        return {key: [record[key] for record in records] for key in keys}
    else:
        return records
