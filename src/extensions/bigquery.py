from __future__ import annotations

from typing import Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Iterator, Literal, TypeVar
    JsonString = TypeVar("JsonString", str)
    Path = TypeVar("Path", str)

    from linkmerce.common.load import DuckDBConnection
    from google.cloud.bigquery import Client as BigQueryClient
    from google.cloud.bigquery import SchemaField
    from google.cloud.bigquery.job import LoadJobConfig
    from google.cloud.bigquery.table import Row


BIGQUERY_JOB = {"append": "WRITE_APPEND", "replace": "WRITE_TRUNCATE"}

DEFAULT_ACCOUNT = "env/service_account.json"
TEMP_TABLE = "temp_table"
TEMP_GROUPBY_TABLE = "temp_groupby_table"


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


def connect(project_id: str, account: ServiceAccount) -> BigQueryClient:
    from google.cloud.bigquery import Client as BigQueryClient
    account = account if isinstance(account, ServiceAccount) else ServiceAccount(account)
    return BigQueryClient.from_service_account_info(account, project=project_id)


def select_table(client: BigQueryClient, query: str) -> Iterator[dict[str,Any]]:
    if query.split(' ', maxsplit=1)[0].upper() != "SELECT":
        query = f"SELECT * FROM `{query}`;"
    def row_to_dict(row: Row) -> dict[str,Any]:
        return dict(row.items())
    return map(row_to_dict, client.query(query).result())


###################################################################
############################ Load Table ###########################
###################################################################

class PartitionOptions(dict):
    def __init__(
            self,
            by: str | list[str] | None = None,
            ascending: bool | None = True,
            condition: str | None = None,
            if_errors: Literal["ignore","raise"] = "raise",
            **kwargs
        ):
        super().__init__(by=by, ascending=ascending, condition=condition, if_errors=if_errors)


def init_load_job(
        schema: Sequence[dict | SchemaField] | None = None,
        source_format: str | None = "PARQUET",
        write_disposition: str | None = "WRITE_APPEND",
        **kwargs
    ) -> LoadJobConfig:
    from google.cloud.bigquery.job import LoadJobConfig
    if isinstance(schema, Sequence):
        kwargs["schema"] = to_bigquery_schema(schema)
    if source_format is not None:
        kwargs["source_format"] = source_format
    if write_disposition is not None:
        kwargs["write_disposition"] = write_disposition
    return LoadJobConfig(**kwargs)


def to_bigquery_schema(schema: Sequence[dict | SchemaField]) -> list[SchemaField]:
    from google.cloud.bigquery import SchemaField
    def map(type: str | None = None, **kwargs) -> SchemaField:
        if type is not None:
            kwargs["field_type"] = type
        return SchemaField(**kwargs)
    return [field if isinstance(field, SchemaField) else map(**field) for field in schema]


def load_table_from_duckdb(
        client: BigQueryClient,
        connection: DuckDBConnection,
        from_table: str,
        to_table: str,
        project_id: str,
        schema: Sequence[dict | SchemaField] = None,
        partition_by: PartitionOptions = dict(),
        progress: bool = True,
    ) -> bool:
    from linkmerce.common.load import DuckDBIterator
    from io import BytesIO

    try:
        from tqdm import tqdm
    except:
        tqdm = lambda x, **kwargs: x

    if not connection.exists_table(from_table):
        return True

    iterator = DuckDBIterator(connection, format="parquet").from_table(from_table)
    if partition_by:
        iterator = iterator.partition_by(**partition_by)

    job_config = init_load_job(schema=schema)
    for bytes_ in tqdm(iterator, desc=f"Uploading data to '{project_id}.{to_table}'", disable=(not progress)):
        client.load_table_from_file(BytesIO(bytes_), f"{project_id}.{to_table}", job_config=job_config).result()
    return True


def overwrite_table_from_duckdb(
        client: BigQueryClient,
        connection: DuckDBConnection,
        from_table: str,
        to_table: str,
        project_id: str,
        condition: str | None = None,
        schema: Sequence[dict | SchemaField] = None,
        partition_by: PartitionOptions = dict(),
        progress: bool = True,
        dry_run: bool = False,
    ) -> bool:
    if not connection.exists_table(from_table):
        return True

    success = False
    source = f"FROM `{project_id}.{to_table}` {connection.expr_where(condition, default=str())}"
    existing_values = select_table(client, f"SELECT * {source};")
    if dry_run:
        _copy_bq_to_duckdb(connection, from_table, list(existing_values))
        return True
    client.query(f"DELETE {source};")

    try:
        success = load_table_from_duckdb(client, connection, from_table, to_table, project_id, schema, partition_by, progress)
        return success
    finally:
        if not success:
            _copy_bq_to_duckdb(connection, from_table, list(existing_values))
            load_table_from_duckdb(client, connection, TEMP_TABLE, to_table, project_id, schema, partition_by, progress=False)


def upsert_table_from_duckdb(
        client: BigQueryClient,
        connection: DuckDBConnection,
        from_table: str,
        to_table: str,
        project_id: str,
        by: str | Sequence[str],
        agg: str | dict[str,Literal["first","count","sum","avg","min","max"]],
        condition: str | None = None,
        schema: Sequence[dict | SchemaField] = None,
        partition_by: PartitionOptions = dict(),
        progress: bool = True,
        dry_run: bool = False,
    ) -> bool:
    if not connection.exists_table(from_table):
        return True

    success = False
    source = f"FROM `{project_id}.{to_table}` {connection.expr_where(condition, default=str())}"
    existing_values = select_table(client, f"SELECT * {source};")
    _copy_bq_to_duckdb(connection, from_table, list(existing_values))

    union = f"((SELECT * FROM {from_table}) UNION ALL (SELECT * FROM {TEMP_TABLE}))"
    connection.groupby(union, by, agg).to_table(TEMP_GROUPBY_TABLE)
    if dry_run:
        return True
    client.query(f"DELETE {source};")

    try:
        success = load_table_from_duckdb(client, connection, TEMP_GROUPBY_TABLE, to_table, project_id, schema, partition_by, progress)
        return success
    finally:
        if not success:
            load_table_from_duckdb(client, connection, TEMP_TABLE, to_table, project_id, schema, partition_by, progress=False)


def _copy_bq_to_duckdb(connection: DuckDBConnection, existing_table: str, existing_values: list[dict]):
    connection.copy_table(existing_table, TEMP_TABLE, option="replace", temp=True)
    connection.insert_into_table_from_json(TEMP_TABLE, existing_values)
