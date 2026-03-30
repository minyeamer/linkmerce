from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Literal, Sequence
    from pathlib import Path
    from linkmerce.extensions.gsheets import ServiceAccount, WorksheetClient


DEFAULT_CONFIG = "env/config.yaml"
DEFAULT_CREDENTIALS = "env/credentials.yaml"
DEFAULT_SCHEMAS = "env/schemas.json"
DEFAULT_SERVICE_ACCOUNT = "env/service_account.json"


def exists(obj: Any, dtype: type, list: bool = False, dict: bool = False) -> bool:
    """객체가 지정된 타입인지 확인한다."""
    if list:
        return list_exists(obj, dtype)
    elif dict:
        return dict_exists(obj, dtype)
    else:
        return isinstance(obj, dtype) and bool(obj)


def list_exists(obj: list[Any], dtype: type) -> bool:
    """리스트 내 모든 요소가 지정된 타입인지 확인한다."""
    return isinstance(obj, list) and obj and all([isinstance(e, dtype) for e in obj])


def dict_exists(obj: dict[str, Any], dtype: type) -> bool:
    """딕셔너리 내 모든 값이 지정된 타입인지 확인한다."""
    return isinstance(obj, dict) and obj and all([isinstance(e, dtype) for e in obj.values()])


def path_exists(path: str, name: str) -> bool:
    """파일 경로가 존재하는지 확인하고, 없으면 예외를 발생시킨다."""
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

def read(file_path: str | Path, format: Literal["auto", "json", "yaml"] = "auto") -> dict | list:
    """파일을 읽어 JSON 또는 YAML 형식으로 파싱한다."""
    if format == "auto":
        import os
        return read(file_path, format=os.path.splitext(file_path)[1][1:])
    elif format.lower() == "json":
        return read_json(file_path)
    elif format.lower() in ("yaml","yml"):
        return read_yaml(file_path)
    else:
        raise ValueError("Invalid value for format. Supported formats are: json, yaml.")


def read_json(file_path: str | Path) -> dict | list:
    """JSON 파일을 읽어 딕셔너리 또는 리스트로 반환한다."""
    import json
    with open(file_path, 'r', encoding="utf-8") as file:
        return json.loads(file.read())


def read_yaml(file_path: str | Path) -> dict | list:
    """YAML 파일을 읽어 딕셔너리 또는 리스트로 반환한다."""
    from ruamel.yaml import YAML
    with open(file_path, 'r', encoding="utf-8") as file:
        yaml = YAML(typ="safe")
        return yaml.load(file.read())


def read_file(file_path: str | Path) -> str:
    """파일을 읽어 문자열로 반환한다."""
    with open(file_path, 'r', encoding="utf-8") as file:
        return file.read()


###################################################################
############################## Config #############################
###################################################################

def read_config(
        file_path: str | Path,
        key_path: str | int | Sequence[str | int] = list(),
        format: Literal["auto", "json", "yaml"] = "auto",
        credentials_path: str | Path | None = None,
        cookies_path: Sequence[str] | Path | None = None,
        schemas_path: str | Path | None = None,
        service_account: ServiceAccount | None = None,
        skip_subpath: bool = False,
        with_table_schema: bool | None = False,
        read_google_sheets: bool = True,
    ) -> dict:
    """설정 파일을 읽고 인증 정보, 테이블 스키마, 구글 시트 데이터를 통합하여 반환한다."""
    config = read_check(file_path, key_path, format, dtype=dict)
    if ("credentials" in config) and (credentials_path is not None):
        if path_exists(credentials_path, "credentials_path"):
            config["credentials"] = parse_credentials(credentials_path, config["credentials"], cookies_path, skip_subpath)
    if ("tables" in config) and isinstance(with_table_schema, bool):
        config["tables"] = parse_tables(config["tables"], schemas_path, with_table_schema)
    if ("sheets" in config) and read_google_sheets and (service_account is not None):
        config.update(parse_sheets(service_account, config["sheets"]))
    return config


def read_check(
        file_path: str | Path,
        key_path: str | int | Sequence[str | int] = list(),
        format: Literal["auto", "json", "yaml"] = "auto",
        dtype: type | None = None,
        list: bool = False,
        dict: bool = False,
    ) -> Any | dict | list:
    """파일을 읽고 `key_path`로 탐색한 후 타입을 검증하여 반환한다."""
    file = read(file_path, format)
    for key in ([key_path] if isinstance(key_path, (str,int)) else key_path):
        file = file[key]
    if (dtype is not None) and (not exists(file, dtype, list, dict)):
        raise ValueError("Invalid data type.")
    return file

########################### Credentials ###########################

def parse_credentials(
        credentials_path: str,
        credentials_info: str | int | Sequence[str | int] = list(),
        cookies_path: Sequence[str] | Path | None = None,
        skip_subpath: bool = False,
    ) -> dict | list:
    """인증 정보 파일을 읽고 `Path()` 참조를 실제 파일 내용으로 치환한다."""
    credentials = read_check(credentials_path, credentials_info)

    if isinstance(cookies_path, list):
        cookies_path = read_check(credentials_path, cookies_path)

    def read_if_path(value: Any) -> Any:
        if isinstance(value, str) and value.startswith("Path(") and value.endswith(")"):
            if cookies_path and ("$cookies" in value):
                value = value.format(**{"$cookies": str(cookies_path)})
            return read_file(value[5:-1])
        return value

    if skip_subpath and isinstance(credentials, (list,dict)):
        return credentials
    elif isinstance(credentials, list):
        return [({key: read_if_path(value) for key, value in credential.items()}
                    if isinstance(credential, dict) else read_if_path(credential))
                for credential in credentials]
    elif isinstance(credentials, dict):
        return {key: read_if_path(value) for key, value in credentials.items()}
    raise ValueError("Could not parse the credentials from config.")


def split_by_credentials(credentials: list[dict], shuffle: bool = False, **kwargs: list) -> list[dict]:
    """인증 정보 목록을 기준으로 키워드 인자를 균등 분배한다."""
    from copy import deepcopy

    n = len(credentials)
    if n == 0:
        return list()

    results = deepcopy(credentials)
    for key, values in kwargs.items():
        if shuffle:
            import random
            random.shuffle(values)

        chunks = list(range(0, len(values), len(values)//n))
        for i in range(n):
            start = chunks[i]
            end = chunks[i+1] if i+1 < n else None
            results[i].update({key: values[start:end]})

    return results

############################## Tables #############################

def parse_tables(
        tables_info: dict[str, dict[str, Any]],
        schemas_path: str | Path | None = None,
        with_table_schema: bool | None = False,
    ) -> dict[str, dict[str, Any]]:
    """테이블 설정을 파싱하고, 필요 시 스키마 파일에서 테이블 스키마를 읽어온다."""
    if not isinstance(tables_info, dict):
        raise ValueError("Could not parse the tables from config.")
    elif with_table_schema and (schemas_path is not None):
        if path_exists(schemas_path, "schemas_path"):
            for db, info in tables_info.copy().items():
                if "schema" in info:
                    tables_info[db]["schema"] = read_check(schemas_path, info["schema"], dtype=dict, list=True)
            return tables_info
    else:
        return {db: info["table"] for db, info in tables_info.items()}

############################## Sheets #############################

def parse_sheets(account: ServiceAccount, sheets_info: dict | list) -> dict:
    """구글 시트 설정을 파싱하고 데이터를 읽어온다."""
    from linkmerce.extensions.gsheets import WorksheetClient
    client = WorksheetClient(account)
    if isinstance(sheets_info, dict):
        if ("key" in sheets_info) and ("sheet" in sheets_info):
            return x if isinstance(x := read_google_sheets(client, **sheets_info), dict) else {"records": x}
        else:
            return {key: read_google_sheets(client, **info) for key, info in sheets_info.items()}
    elif isinstance(sheets_info, list):
        return [read_google_sheets(client, **info) for info in sheets_info if isinstance(info, dict)]
    else:
        raise ValueError("Could not parse the sheets from config.")

def read_google_sheets(
        client: WorksheetClient,
        key: str,
        sheet: str,
        column: str | Sequence[str] | None = None,
        axis: Literal[0, "by_col", 1, "by_row"] = 0,
        head: int = 1,
        expected_headers: Any | None = None,
        value_render_option: Any | None = None,
        default_blank: str | None = None,
        numericise_ignore: Sequence[int] | bool = list(),
        allow_underscores_in_numeric_literals: bool = False,
        empty2zero: bool = False,
        convert_dtypes: bool = True,
    ) -> dict[str, list] | list[dict]:
    """구글 시트 워크시트에서 데이터를 읽어 딕셔너리 또는 리스트로 반환한다."""
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

############################# Default #############################

def _default_config(
        key_path: str | int | Sequence[str | int] = list(),
        file_path: str | Path = DEFAULT_CONFIG,
        format: Literal["auto", "json", "yaml"] = "auto",
        credentials_path: str | Path | None = DEFAULT_CREDENTIALS,
        schemas_path: str | Path | None = DEFAULT_SCHEMAS,
        service_account: ServiceAccount | None = DEFAULT_SERVICE_ACCOUNT,
        skip_subpath: bool = False,
        with_table_schema: bool | None = False,
        read_google_sheets: bool = True,
    ) -> dict:
    """기본 경로로 설정 파일을 읽고 `service_account`를 추가하여 반환한다."""
    config = read_config(
        file_path, key_path, format, credentials_path, schemas_path, service_account,
        skip_subpath, with_table_schema, read_google_sheets
    )
    if service_account:
        config["service_account"] = service_account
    return config
