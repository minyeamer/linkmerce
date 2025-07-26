from __future__ import annotations

from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.bigquery.job import LoadJobConfig
import functools

from typing import Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Iterator, List, Literal, Sequence, TypeVar
    from google.cloud.bigquery.table import Row
    JsonString = TypeVar("JsonString", str)
    Path = TypeVar("Path", str)


BIGQUERY_JOB = {"append": "WRITE_APPEND", "replace": "WRITE_TRUNCATE"}

DEFAULT_ACCOUNT = "env/service_account.json"


class ServiceAccount(dict):
    def __init__(self, info: JsonString | Path | Dict[str,str]):
        super().__init__(self.read_account(info))

    def read_account(self, info: JsonString | Path | Dict[str,str]) -> Dict:
        if isinstance(info, Dict):
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


def create_connection(project_id: str, account: ServiceAccount) -> BigQueryClient:
    account = account if isinstance(account, ServiceAccount) else ServiceAccount(account)
    return BigQueryClient.from_service_account_info(account, project=project_id)


def with_service_account(func):
    @functools.wraps(func)
    def wrapper(*args, account: ServiceAccount | None = None, **kwargs):
        if account is None:
            account = ServiceAccount(DEFAULT_ACCOUNT)
        return func(*args, account=account, **kwargs)
    return wrapper


###################################################################
########################### Select Table ##########################
###################################################################

def _select_table(client: BigQueryClient, query: str) -> Iterator[Dict[str,Any]]:
    if query.split(' ', maxsplit=1)[0].upper() != "SELECT":
        query = f"SELECT * FROM `{query}`;"
    def row_to_dict(row: Row) -> Dict[str,Any]:
        return dict(row.items())
    return map(row_to_dict, client.query(query).result())


@with_service_account
def select_table_to_json(
        query: str,
        project_id: str,
        *,
        account: ServiceAccount | None = None,
    ) -> List[Dict[str,Any]]:
    client = create_connection(project_id, account)
    return _select_table(client, query)


###################################################################
############################ Load Table ###########################
###################################################################

def _write_append(
        client: BigQueryClient, 
        table: str,
        project_id: str,
        data: List[Dict],
        partition: Dict | None = None,
        serialize: bool = True,
        progress: bool = True,
    ) -> bool:
    if isinstance(partition, Dict) and ("field" in partition):
        from linkmerce.utils.duckdb import partition_by
        iterable = partition_by(data, **partition)
    else:
        iterable = [data]

    from tqdm.auto import tqdm
    import json
    job_config = LoadJobConfig(write_disposition="WRITE_APPEND")
    for rows in tqdm(iterable, desc=f"Uploading data to '{project_id}.{table}'", disable=(not progress)):
        if serialize:
            rows = json.loads(json.dumps(rows, ensure_ascii=False, default=str))
        client.load_table_from_json(rows, f"{project_id}.{table}", job_config=job_config).result()
    return True


@with_service_account
def load_table_from_json(
        table: str,
        project_id: str,
        data: List[Dict],
        partition: Dict | None = None,
        serialize: bool = True,
        progress: bool = True,
        *,
        account: ServiceAccount | None = None,
    ) -> bool:
    client = create_connection(project_id, account)
    return _write_append(client, table, project_id, data, partition, serialize, progress)


@with_service_account
def overwrite_table_from_json(
        table: str,
        project_id: str,
        data: List[Dict],
        condition: str | None = None,
        partition: Dict | None = None,
        serialize: bool = True,
        progress: bool = True,
        *,
        account: ServiceAccount | None = None,
    ) -> bool:
    client = create_connection(project_id, account)
    success = False
    where = _where(**(dict(condition=condition) if condition else (partition or dict())))
    source = f"FROM `{table}` {where}"

    existing_data = _select_table(client, f"SELECT * {source};")
    client.query(f"DELETE {source};")

    try:
        success = _write_append(client, table, project_id, data, partition, serialize, progress)
        return success
    finally:
        if not success:
            _write_append(client, table, project_id, list(existing_data), serialize=serialize)


@with_service_account
def upsert_table_from_json(
        table: str,
        project_id: str,
        data: List[Dict],
        by: str | Sequence[str],
        agg: str | Dict[str,Literal["first","count","sum","avg","min","max"]],
        condition: str | None = None,
        partition: Dict | None = None,
        serialize: bool = True,
        progress: bool = True,
        *,
        account: ServiceAccount | None = None,
    ) -> bool:
    client = create_connection(project_id, account)
    success = False
    where = _where(**(dict(condition=condition) if condition else (partition or dict())))
    source = f"FROM `{table}` {where}"

    from linkmerce.utils.duckdb import groupby
    existing_data = list(_select_table(client, f"SELECT * {source};"))
    combined_data = groupby(data + existing_data, by, agg)
    client.query(f"DELETE {source};")

    try:
        success = _write_append(client, table, project_id, combined_data, partition, serialize, progress)
        return success
    finally:
        if not success:
            _write_append(client, table, project_id, existing_data, serialize=serialize)


def _where(condition: str | None = None, field: str | None = None, **kwargs) -> str:
    if condition is not None:
        if condition.split(' ', maxsplit=1)[0].upper() == "WHERE":
            return condition
        elif field:
            return f"WHERE {field} {condition}"
        else:
            return "WHERE TRUE"
    else:
        return "WHERE TRUE"
