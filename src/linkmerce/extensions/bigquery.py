from __future__ import annotations
import functools

from linkmerce.common.load import Connection, concat_sql, where

from typing import Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, IO, Literal, Type, TypeVar
    from types import TracebackType
    JsonString = TypeVar("JsonString", str)
    Path = TypeVar("Path", str)
    TableId = TypeVar("TableId", str)

    from google.cloud.bigquery import Client
    from google.cloud.bigquery import SchemaField, Table
    from google.cloud.bigquery.job import LoadJob, LoadJobConfig, QueryJob
    from google.cloud.bigquery.table import Row, RowIterator
    Clause = TypeVar("Clause", str)
    Columns = TypeVar("Columns", Sequence[str])

    from linkmerce.common.load import DuckDBConnection
    DuckDBTable = TypeVar("DuckDBTable", str)
    BigQueryTable = TypeVar("BigQueryTable", str)


DEFAULT_ACCOUNT = "env/service_account.json"

TEMP_TABLE = "temp_table"


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


class PartitionOptions(dict):
    def __init__(
            self,
            by: str | list[str] | None = None,
            ascending: bool | None = True,
            where_clause: str | None = None,
            if_errors: Literal["ignore","raise"] = "raise",
            **kwargs
        ):
        super().__init__(by=by, ascending=ascending, where_clause=where_clause, if_errors=if_errors)


###################################################################
######################### BigQuery Client #########################
###################################################################

class BigQueryClient(Connection):
    def __init__(self, account: ServiceAccount):
        self.set_connection(account)

    @property
    def conn(self) -> Client:
        return self.get_connection()

    def get_connection(self) -> Client:
        return self.__conn

    def set_connection(self, account: ServiceAccount):
        from google.cloud.bigquery import Client
        account = account if isinstance(account, ServiceAccount) else ServiceAccount(account)
        self.__conn = Client.from_service_account_info(account, project=account["project_id"])
        self.project_id = account["project_id"]

    def close(self):
        try:
            self.conn.close()
        except:
            pass

    def retry_on_concurrent_update(max_retries: int = 5, delay: float | int = 1, random_delay: tuple[float,float] | None = None):
        # BadRequest: 400 POST https://bigquery.googleapis.com/bigquery/v2/projects/{{ project-id }}/queries? \
        # prettyPrint=false:Could not serialize access to table {{ project-id }}:{{ table }} due to concurrent update
        def decorator(func):
            @functools.wraps(func)
            def wrapper(self, *args, **kwargs):
                from google.api_core.exceptions import BadRequest
                import time

                for count in range(1, max_retries+1):
                    try:
                        return func(self, *args, **kwargs)
                    except BadRequest as error:
                        if (count < max_retries) and ("concurrent update" in str(error)):
                            if random_delay is not None:
                                import random
                                time.sleep(random.uniform(*random_delay))
                            else:
                                time.sleep(delay)
                            continue
                        raise error
            return wrapper
        return decorator

    @retry_on_concurrent_update(max_retries=5, random_delay=(1,3))
    def execute(self, query: str, **kwargs) -> QueryJob:
        return self.conn.query(query, **kwargs)

    @retry_on_concurrent_update(max_retries=5, random_delay=(1,3))
    def execute_job(self, query: str, **kwargs) -> RowIterator:
        return self.conn.query_and_wait(query, **kwargs)

    def __enter__(self) -> BigQueryClient:
        return self

    def __exit__(self, type: Type[BaseException], value: BaseException, traceback: TracebackType):
        self.close()

    ############################## Fetch ##############################

    def fetch_all(self, format: Literal["csv","json"], query: str) -> list[dict]:
        try:
            return getattr(self, f"fetch_all_to_{format}")(query)
        except AttributeError:
            raise ValueError("Invalid value for data format. Supported formats are: csv, json.")

    def fetch_all_to_csv(self, query: str, header: bool = True) -> list[tuple]:
        def row_keys(row: Row) -> tuple:
            return tuple(row.keys())
        def row_values(row: Row) -> tuple:
            return tuple(row.values())
        rows = list()
        for row in self.execute_job(query):
            if header and (not rows):
                rows.append(row_keys(row))
            rows.append(row_values(row))
        return rows

    def fetch_all_to_json(self, query: str) -> list[dict]:
        def row_to_dict(row: Row) -> dict[str,Any]:
            return dict(row.items())
        return [row_to_dict(row) for row in self.execute_job(query)]

    ############################ CRUD Table ###########################

    def create_table(
            self,
            table: str,
            schema: TableId | Sequence[dict | SchemaField],
            exists_ok: bool = True,
            **kwargs
        ) -> Table:
        table_ref = self.ref_table(table, schema)
        return self.conn.create_table(table_ref, exists_ok=exists_ok, **kwargs)

    def copy_table(
            self,
            source_table: str,
            target_table: str,
            where_clause: str | None = None,
            limit: int | None = 0,
            option: Literal["replace","ignore"] | None = None,
        ) -> RowIterator:
        select = f"SELECT * FROM `{self.project_id}.{source_table}`"
        limit_ = f"LIMIT {limit}" if isinstance(limit, int) else None
        query = concat_sql(f"{self.expr_create(option)} `{self.project_id}.{target_table}` AS", select, where(where_clause), limit_)
        return self.execute_job(query)

    def delete_table(self, table: str, where: str = "TRUE") -> RowIterator:
        return self.execute_job(f"DELETE FROM `{self.project_id}.{table}` WHERE {where};")

    def select_table_to_json(self, table: str) -> list[dict]:
        return self.fetch_all_to_json(f"SELECT * FROM `{self.project_id}.{table}`;")

    def table_exists(self, table: str) -> bool:
        from google.api_core.exceptions import NotFound
        try:
            self.conn.get_table(f"{self.project_id}.{table}")
            return True
        except NotFound:
            return False

    def table_has_rows(self, table: str, where_clause: str | None = None) -> bool:
        if self.table_exists(table):
            query = concat_sql(f"SELECT COUNT(*) FROM `{self.project_id}.{table}`", where(where_clause))
            rows = list(self.execute_job(query))
            return bool(list(rows[0].values())[0])
        return False

    def get_table(self, table: str) -> Table:
        return self.conn.get_table(f"{self.project_id}.{table}")

    def ref_table(self, table: str, schema: TableId | Sequence[dict | SchemaField]) -> Table:
        from google.cloud.bigquery import Table
        if isinstance(schema, str):
            schema = self.get_schema(schema)
        if not isinstance(schema, Sequence):
            raise ValueError("Invalid schema: expected sequence of schema fields.")
        return Table(table, schema)

    def get_schema(self, table: str) -> list[SchemaField]:
        return self.conn.get_table(f"{self.project_id}.{table}").schema

    def get_columns(self, table: str) -> list[str]:
        return [field.name for field in self.get_schema(table)]

    ############################# Load Job ############################

    def load_table_from_json(
            self,
            table: str,
            values: list[dict] | str | Path,
            serialize: bool = True,
            schema: Literal["auto"] | TableId | Sequence[dict | SchemaField] = "auto",
            write: Literal["append","empty","truncate","truncate_data"] = "append",
            if_not_found: Literal["create","errors","ignore"] = "errors",
        ) -> LoadJob:
        schema = self._auto_detect_schema(table, schema)
        if self._find_table(table, schema, if_not_found):
            if serialize:
                import json
                values = json.loads(json.dumps(values, ensure_ascii=False, default=str))
            job_config = self.build_load_job_config(schema, write_disposition=write.upper())
            return self.conn.load_table_from_json(values, f"{self.project_id}.{table}", job_config=job_config).result()

    def load_table_from_file(
            self,
            table: str,
            file_obj: IO[bytes],
            format: Literal["avgo","csv","json","orc","parquet"],
            schema: Literal["auto"] | TableId | Sequence[dict | SchemaField] = "auto",
            write: Literal["append","empty","truncate","truncate_data"] = "append",
            if_not_found: Literal["create","errors","ignore"] = "errors",
        ) -> LoadJob:
        schema = self._auto_detect_schema(table, schema)
        if self._find_table(table, schema, if_not_found):
            job_config = self.build_load_job_config(schema, source_format=format.upper(), write_disposition=write.upper())
            return self.conn.load_table_from_file(file_obj, f"{self.project_id}.{table}", job_config=job_config).result()

    def build_load_job_config(
            self,
            schema: Sequence[dict | SchemaField] | None = None,
            source_format: Literal["AVRO", "CSV", "JSON", "ORC", "PARQUET"] | None = None,
            write_disposition: Literal["APPEND", "EMPTY", "TRUNCATE", "TRUNCATE_DATA"] | None = None,
            **kwargs
        ) -> LoadJobConfig:
        from google.cloud.bigquery.job import LoadJobConfig
        if schema is not None:
            kwargs["schema"] = self.build_schema(schema)
        if source_format is not None:
            kwargs["source_format"] = "NEWLINE_DELIMITED_JSON" if source_format == "JSON" else source_format
        if write_disposition is not None:
            kwargs["write_disposition"] = f"WRITE_{write_disposition}"
        return LoadJobConfig(**kwargs)

    def build_schema(self, fields: Sequence[dict | SchemaField]) -> list[SchemaField]:
        from google.cloud.bigquery import SchemaField
        def build(type: str | None = None, **kwargs) -> SchemaField:
            if type is not None:
                kwargs["field_type"] = type
            return SchemaField(**kwargs)
        return [field if isinstance(field, SchemaField) else build(**field) for field in fields]

    def _auto_detect_schema(
            self,
            table: str,
            schema: Literal["auto"] | TableId | Sequence[dict | SchemaField] = "auto",
        ) -> list[dict | SchemaField]:
        if isinstance(schema, str):
            return self.get_schema(table if schema == "auto" else schema)
        else:
            return schema

    def _find_table(
            self,
            table: str,
            schema: list[dict | SchemaField],
            if_not_found: Literal["create","errors","ignore"] = "errors",
        ) -> bool:
        if if_not_found == "create":
            if not self.table_exists(table):
                self.create_table(table, schema)
        elif if_not_found == "ignore":
            return self.table_exists(table)
        return True

    ############################## Merge ##############################

    def merge_into_table(
            self,
            source_table: str,
            target_table: str,
            on: str | Sequence[str],
            matched: Clause | dict[str,Literal["replace","ignore","greatest","least","source_first","target_first"]] | Literal[":replace_all:",":do_nothing:"] = ":replace_all:",
            not_matched: Clause | Columns | Literal[":insert_all:",":do_nothing:"] = ":insert_all:",
            where_clause: Clause | None = None,
        ) -> LoadJob:
        """When using where_clause, reference target table columns with \"T.\" and source table columns with \"S.\""""
        where = [where_clause] if where_clause else list()
        on = " AND ".join([f"T.{col} = S.{col}" for col in ([on] if isinstance(on, str) else on)]+where)
        query = f"MERGE INTO `{self.project_id}.{target_table}` AS T USING `{self.project_id}.{source_table}` AS S ON {on}"
        query = concat_sql(query, self._merge_update(matched, target_table, on), self._merge_insert(not_matched, target_table))
        return self.execute_job(query)

    def _merge_update(
            self,
            matched: Clause | dict[str,Literal["replace","ignore","greatest","least","source_first","target_first"]] | Literal[":replace_all:",":do_nothing:"] = ":replace_all:",
            target_table: str = str(),
            on: str | Sequence[str] = list(),
        ) -> str:
        prefix = "WHEN MATCHED THEN UPDATE SET "
        if matched == ":replace_all:":
            on = [on] if isinstance(on, str) else on
            return self._merge_update({col: "replace" for col in self.get_columns(target_table) if col not in on})
        elif matched == ":do_nothing:":
            return None
        elif isinstance(matched, dict):
            def render(col: str, agg: str) -> str:
                if agg in {"source_first","target_first"}:
                    kwargs = dict(zip(["left","right"], ('S','T') if agg == "source_first" else ('T','S')))
                    return "COALESCE({left}.{col}, {right}.{col})".format(col=col, **kwargs)
                elif agg in {"greatest","least"}:
                    return f"{agg.upper()}(S.{col}, T.{col})"
                elif agg in {"replace","ignore"}:
                    return f"S.{col}" if agg == "replace" else f"T.{col}"
                else:
                    return f"{agg}({col})"
            return prefix + ", ".join([f"T.{col} = {render(col, agg)}" for col, agg in matched.items()])
        else:
            return prefix + str(matched)

    def _merge_insert(self, not_matched: Clause | Columns | Literal[":insert_all:",":do_nothing:"] = ":insert_all:", target_table: str = str()) -> str:
        prefix = "WHEN NOT MATCHED THEN "
        if not_matched == ":insert_all:":
            return self._merge_insert(self.get_columns(target_table))
        elif not_matched == ":do_nothing:":
            return None
        elif isinstance(not_matched, str):
            return prefix + not_matched
        elif isinstance(not_matched, Sequence):
            return prefix + "INSERT ({}) VALUES ({})".format(", ".join(not_matched), "S."+", S.".join(not_matched))
        else:
            return prefix + str(not_matched)

    ########################## Load and Merge #########################

    def merge_into_table_from_file(
            self,
            stage_table: str,
            target_table: str,
            source_file: IO[bytes],
            source_format: Literal["avgo","csv","json","orc","parquet"],
            on: str | Sequence[str],
            matched: Clause | dict[str,Literal["replace","ignore","greatest","least","source_first","target_first"]] | Literal[":replace_all:",":do_nothing:"] = ":replace_all:",
            not_matched: Clause | Columns | Literal[":insert_all:",":do_nothing:"] = ":insert_all:",
            where_clause: Clause | None = None,
            schema: Literal["auto"] | TableId | Sequence[dict | SchemaField] = "auto",
            write: Literal["append","empty","truncate","truncate_data"] = "truncate",
            if_not_found: Literal["create","errors","ignore"] = "errors",
            table_lock_wait_interval: float | int | None = None,
            table_lock_wait_timeout: float | int | None = 60.,
            drop_stage_after_merge: bool = True,
        ) -> LoadJob:
        """When using where_clause, reference target table columns with \"T.\" and source table columns with \"S.\""""
        try:
            self._wait_until_table_not_found(stage_table, table_lock_wait_interval, table_lock_wait_timeout)
            self.load_table_from_file(source_file, stage_table, source_format, schema, write, if_not_found)
            return self.merge_into_table(stage_table, target_table, on, matched, not_matched, where_clause)
        finally:
            if drop_stage_after_merge and self.table_exists(stage_table):
                self.execute_job(f"DROP TABLE `{self.project_id}.{stage_table}`")

    def _wait_until_table_not_found(self, table: str, interval: float | int | None = 1., timeout: float | int | None = 60.):
        has_interval, has_timeout = isinstance(interval, (float,int)), isinstance(timeout, (float,int))
        if has_interval:
            import time
            total = 0
            while self.table_exists(table):
                if has_timeout and (total > timeout):
                    raise TimeoutError("Timed out waiting until the table does not exist.")
                total += interval
                time.sleep(interval)

    ############################ Expression ###########################

    def expr_cast(self, value: Any | None, type: str, alias: str = str(), safe: bool = False) -> str:
        cast = "SAFE_CAST" if safe else "CAST"
        alias = f" AS {alias}" if alias else str()
        return f"{cast}({self.expr_value(value)} AS {type.upper()})" + alias

    def expr_interval(expr: str, days: int | None = None, time: bool = True) -> str:
        if isinstance(days, int):
            func = ("DATE{}_SUB" if days < 0 else "DATE{}_ADD").format("TIME" if time else str())
            return f"{func}({expr}, INTERVAL {abs(days)} DAY)"
        else:
            return expr

    def expr_now(
            self,
            type: Literal["DATETIME","STRING"] = "DATETIME",
            format: str | None = "%Y-%m-%d %H:%M:%S",
            interval: str | int | None = None,
            tzinfo: str | None = None,
        ) -> str:
        expr = "CURRENT_DATETIME({})".format(f"'{tzinfo}'" if tzinfo else str())
        expr = self.expr_interval(expr, interval, time=True)
        if format:
            expr = f"FORMAT_DATE('{format}', {expr})"
            if type.upper() == "DATETIME":
                return f"CAST({expr} AS DATETIME)"
        return expr if type.upper() == "DATETIME" else "NULL"

    def expr_today(
            self,
            type: Literal["DATE","STRING"] = "DATE",
            format: str | None = "%Y-%m-%d",
            interval: str | int | None = None,
            tzinfo: str | None = None,
        ) -> str:
        expr = "CURRENT_DATE({})".format(f"'{tzinfo}'" if tzinfo else str())
        expr = self.expr_interval(expr, interval, time=False)
        if (type.upper() == "STRING") and format:
            return f"FORMAT_DATE('{format}', {expr})"
        return expr if type.upper() == "DATE" else "NULL"

    ############################## DuckDB #############################

    def load_table_from_duckdb(
            self,
            connection: DuckDBConnection,
            source_table: DuckDBTable,
            target_table: BigQueryTable,
            partition_by: PartitionOptions = dict(),
            schema: Literal["auto"] | TableId | Sequence[dict | SchemaField] = "auto",
            write: Literal["append","empty","truncate","truncate_data"] = "append",
            if_not_found: Literal["create","errors","ignore"] = "errors",
            progress: bool = True,
            if_source_table_empty: Literal["break","continue"] = "break",
        ) -> bool:
        from linkmerce.common.load import DuckDBIterator
        from linkmerce.utils.progress import import_tqdm
        from io import BytesIO

        if not (connection.table_has_rows(source_table) if if_source_table_empty == "break" else connection.table_exists(source_table)):
            return True
        schema = self._auto_detect_schema(target_table, schema)

        iterator = DuckDBIterator(connection, format="parquet").from_table(source_table)
        if partition_by:
            iterator = iterator.partition_by(**partition_by)

        tqdm = import_tqdm()
        for bytes_ in tqdm(iterator, desc=f"Uploading data to '{target_table}'", disable=(not progress)):
            self.load_table_from_file(target_table, BytesIO(bytes_), "parquet", schema, write, if_not_found)
        return True

    def overwrite_table_from_duckdb(
            self,
            connection: DuckDBConnection,
            source_table: DuckDBTable,
            target_table: BigQueryTable,
            where_clause: Clause = "TRUE",
            partition_by: PartitionOptions = dict(),
            schema: Literal["auto"] | TableId | Sequence[dict | SchemaField] = "auto",
            if_not_found: Literal["create","errors","ignore"] = "errors",
            progress: bool = True,
            backup_table: DuckDBTable | None = TEMP_TABLE,
            if_source_table_empty: Literal["break","continue"] = "break",
            if_backup_table_exists: Literal["errors","ignore","replace"] = "replace",
            truncate_target_table: bool = False,
        ) -> bool:
        if not (connection.table_has_rows(source_table) if if_source_table_empty == "break" else connection.table_exists(source_table)):
            return True
        elif not self.table_has_rows(target_table, where_clause):
            return self.load_table_from_duckdb(connection, source_table, target_table, partition_by, schema, "append", if_not_found, progress)

        success = False
        project_table = f"`{self.project_id}.{target_table}`"
        from_clause = concat_sql(f"FROM {project_table}", where(where_clause))

        existing_values = self.fetch_all_to_json(concat_sql("SELECT *", from_clause))
        self.conn.query(concat_sql("TRUNCATE TABLE", project_table) if truncate_target_table else concat_sql("DELETE", from_clause))

        try:
            success = self.load_table_from_duckdb(connection, source_table, target_table, partition_by, schema, "append", if_not_found, progress)
            return success
        finally:
            if (not success) and (backup_table is not None) and existing_values:
                create_option = self._raise_error_if_table_exists(if_backup_table_exists)
                connection.copy_table(source_table, backup_table, option=create_option, temp=True)
                connection.insert_into_table_from_json(backup_table, existing_values)

    def merge_into_table_from_duckdb(
            self,
            connection: DuckDBConnection,
            source_table: DuckDBTable,
            staging_table: BigQueryTable,
            target_table: BigQueryTable,
            on: str | Sequence[str],
            matched: Clause | dict[str,Literal["replace","ignore","greatest","least","source_first","target_first"]] | Literal[":replace_all:",":do_nothing:"] = ":replace_all:",
            not_matched: Clause | Columns | Literal[":insert_all:",":do_nothing:"] = ":insert_all:",
            schema: Literal["auto"] | TableId | Sequence[dict | SchemaField] = "auto",
            where_clause: Clause | None = None,
            if_not_found: Literal["create","errors","ignore"] = "errors",
            progress: bool = True,
            table_lock_wait_interval: float | int | None = None,
            table_lock_wait_timeout: float | int | None = 60.,
            if_source_table_empty: Literal["break","continue"] = "break",
            if_staging_table_exists: Literal["errors","ignore","replace"] = "replace",
            drop_stage_after_merge: bool = True,
        ) -> bool:
        """When using where_clause, reference target table columns with \"T.\" and source table columns with \"S.\""""
        import re
        if not (connection.table_has_rows(source_table) if if_source_table_empty == "break" else connection.table_exists(source_table)):
            return True
        elif not self.table_has_rows(target_table, (re.sub(r"(^|[^A-Za-z0-9_])(T|S)\.", r"\1", where_clause) if where_clause else None)):
            return self.load_table_from_duckdb(connection, source_table, target_table, dict(), schema, "append", if_not_found, progress)

        try:
            self._wait_until_table_not_found(staging_table, table_lock_wait_interval, table_lock_wait_timeout)
            create_option = self._raise_error_if_table_exists(if_staging_table_exists)
            self.copy_table(target_table, staging_table, option=create_option)
            self.load_table_from_duckdb(connection, source_table, staging_table, dict(), schema, "append", if_not_found, progress)
            self.merge_into_table(staging_table, target_table, on, matched, not_matched, where_clause)
            return True
        finally:
            if drop_stage_after_merge and self.table_exists(staging_table):
                self.execute_job(f"DROP TABLE `{self.project_id}.{staging_table}`;")

    def _raise_error_if_table_exists(
            self,
            table: BigQueryTable,
            option: Literal["errors","ignore","replace"] = "replace"
        ) -> Literal["ignore","replace"]:
        if option == "errors":
            if self.table_exists(table):
                from google.api_core.exceptions import AlreadyExists
                raise AlreadyExists(f"Error: Table '{table}' already exists.")
            else:
                return "replace"
        else:
            return option
