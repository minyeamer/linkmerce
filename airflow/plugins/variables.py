from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Sequence


def read(
        path: str | int | Sequence[str | int] = list(),
        format: Literal["auto","json","yaml"] = "auto",
        credentials: bool | Literal["expand"] = False,
        tables: bool = False,
        sheets: bool = False,
        service_account: bool = False,
        with_table_schema: bool | None = False,
    ) -> dict:
    from airflow.sdk import Variable
    from linkmerce.extensions.variables import read_variables
    file_path = Variable.get("variables")
    credentials_path = Variable.get("credentials_path") if credentials else None
    schemas_path = Variable.get("schemas_path") if tables else None
    service_account = Variable.get("service_account") if sheets or service_account else None

    variables = read_variables(file_path, path, format, credentials_path, schemas_path, service_account, with_table_schema)
    if credentials == "expand":
        variables.update(variables.get("credentials", dict()))
    if service_account:
        variables["service_account"] = service_account
    return variables
