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
    ) -> dict:
    from airflow.sdk import Variable
    from linkmerce.api.config import read_config

    file_path = Variable.get("config")
    credentials_path = Variable.get("credentials") if credentials else None
    schemas_path = Variable.get("schemas") if tables else None
    service_account = Variable.get("service_account") if sheets or service_account else None

    config = read_config(
        file_path, path, format, credentials_path, schemas_path, service_account, read_google_sheets=sheets)

    for key, isin in zip(["credentials","tables","sheets","service_account"], [credentials,tables,sheets,service_account]):
        if (not isin) and (key in config):
            config.pop(key)

    if credentials == "expand":
        config.update(config.get("credentials", dict()))
    if service_account:
        config["service_account"] = service_account
    return config


def split_by_credentials(credentials: list[dict], shuffle: bool = False, **kwargs: list) -> list[dict]:
    from linkmerce.api.config import split_by_credentials
    return split_by_credentials(credentials, shuffle, **kwargs)
