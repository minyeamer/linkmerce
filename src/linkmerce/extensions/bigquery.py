from __future__ import annotations
import functools

from linkmerce.common.load import Connection, concat_sql, where, build_temp_table_name

from typing import Sequence, TypedDict, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, IO, Iterator, Literal, TypeVar
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


class ServiceAccount(dict):
    """구글 클라우드 서비스 계정 인증 정보를 딕셔너리로 관리하는 클래스."""

    def __init__(self, info: JsonString | Path | dict[str, str]):
        super().__init__(self.read_account(info))

    def read_account(self, info: JsonString | Path | dict[str, str]) -> dict:
        """JSON 문자열, 파일 경로, 또는 딕셔너리에서 서비스 계정 정보를 읽는다."""
        if isinstance(info, dict):
            return info
        elif isinstance(info, str):
            import json
            if info.startswith('{') and info.endswith('}'):
                return json.loads(info)
            else:
                with open(info, 'r', encoding="utf-8") as file:
                    return json.loads(file.read())
        raise ValueError("Unrecognized service account.")


class PartitionOptions(TypedDict, total=False):
    """DuckDB 테이블 파티션 옵션."""
    by: str | list[str] | None
    ascending: bool | None
    where_clause: str | None
    if_errors: Literal["ignore", "raise"]


###################################################################
######################### BigQuery Client #########################
###################################################################

class BigQueryClient(Connection):
    """구글 BigQuery 클라이언트. `Connection`을 상속하며 테이블 CRUD 작업을 지원한다.

    BigQuery 연결을 위해 [`google-cloud-bigquery`](https://pypi.org/project/google-cloud-bigquery/)
    라이브러리를 사용한다.
    """

    def __init__(self, account: ServiceAccount):
        self.set_connection(account)

    @property
    def conn(self) -> Client:
        return self.get_connection()

    def get_connection(self) -> Client:
        return self.__conn

    def set_connection(self, account: ServiceAccount):
        """서비스 계정 정보로 BigQuery 클라이언트 연결을 설정한다."""
        from google.cloud.bigquery import Client
        account = account if isinstance(account, ServiceAccount) else ServiceAccount(account)
        self.__conn = Client.from_service_account_info(account, project=account["project_id"])
        self.project_id = account["project_id"]

    def close(self):
        try:
            self.conn.close()
        except:
            pass

    def __enter__(self) -> BigQueryClient:
        return self

    def __exit__(self, type: type[BaseException], value: BaseException, traceback: TracebackType):
        self.close()

    ############################# Execute #############################

    def retry_on_concurrent_update(max_retries: int = 5, delay: float | int = 1, random_delay: tuple[float, float] | None = None):
        """BigQuery 테이블 동시 업데이트 오류 발생 시 재시도하는 데코레이터."""
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
        """SQL 쿼리를 비동기로 실행하여 `QueryJob`을 반환한다."""
        return self.conn.query(query, **kwargs)

    @retry_on_concurrent_update(max_retries=5, random_delay=(1,3))
    def execute_job(self, query: str, **kwargs) -> RowIterator | Iterator[Row]:
        """SQL 쿼리를 실행하고 결과가 준비될 때까지 대기한다."""
        return self.conn.query_and_wait(query, **kwargs)

    ############################## Fetch ##############################

    def fetch_one(self, query: str, index: int = 0) -> Any:
        """SQL 쿼리를 실행하여 값 하나를 가져온다."""
        for row in self.execute_job(query):
            return list(row.values())[index]

    def fetch_values(self, query: str, axis: int = 0) -> tuple[Any, ...]:
        """SQL 쿼리 실행 결과를 1차원 리스트로 반환한다. `axis=0`은 첫 번째 행, `axis=1`은 첫 번째 열을 반환한다."""
        if axis == 1:
            return tuple(list(row.values())[0] for row in self.execute_job(query))
        else:
            for row in self.execute_job(query):
                return tuple(row.values())

    def fetch_all(self, format: Literal["csv", "json"], query: str) -> list[dict]:
        """SQL 쿼리를 실행하고 결과를 지정한 형식(`csv` 또는 `json`)으로 반환한다."""
        try:
            return getattr(self, f"fetch_all_to_{format}")(query)
        except AttributeError:
            raise ValueError("Invalid value for data format. Supported formats are: csv, json.")

    def fetch_all_to_csv(self, query: str, header: bool = True) -> list[tuple]:
        """SQL 쿼리를 실행하고 결과를 CSV 형식의 튜플 리스트로 반환한다."""
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
        """SQL 쿼리를 실행하고 결과를 JSON 형식으로 반환한다."""
        def row_to_dict(row: Row) -> dict[str, Any]:
            return dict(row.items())
        return [row_to_dict(row) for row in self.execute_job(query)]

    ############################### CRUD ##############################

    def create_table(
            self,
            table: str,
            schema: TableId | Sequence[dict | SchemaField],
            exists_ok: bool = True,
            **kwargs
        ) -> Table:
        """지정된 스키마로 BigQuery 테이블을 생성한다."""
        table_ref = self.ref_table(table, schema)
        return self.conn.create_table(table_ref, exists_ok=exists_ok, **kwargs)

    def copy_table(
            self,
            source_table: str,
            target_table: str,
            where_clause: str | None = None,
            limit: int | None = None,
            option: Literal["replace", "ignore"] | None = None,
        ) -> RowIterator:
        """SELECT 문으로 조회한 소스 테이블을 타겟 테이블로 복사한다."""
        query = concat_sql(
            f"{self.expr_create(option)} `{self.project_id}.{target_table}` AS",
            f"SELECT * FROM `{self.project_id}.{source_table}`",
            where(where_clause),
            (f"LIMIT {limit}" if isinstance(limit, int) else None),
        )
        return self.execute_job(query)

    def delete_table(self, table: str, where: str = "TRUE") -> RowIterator:
        """WHERE 조건에 맞는 행을 삭제한다."""
        return self.execute_job(f"DELETE FROM `{self.project_id}.{table}` WHERE {where};")

    def select_table_to_json(self, table: str) -> list[dict]:
        """테이블의 모든 행을 JSON 형태로 조회한다."""
        return self.fetch_all_to_json(f"SELECT * FROM `{self.project_id}.{table}`;")

    ############################# Load Job ############################

    def load_table_from_json(
            self,
            table: str,
            values: list[dict] | str | Path,
            serialize: bool = True,
            schema: Literal["auto"] | TableId | Sequence[dict | SchemaField] = "auto",
            write: Literal["append", "empty", "truncate", "truncate_data"] = "append",
            if_table_not_found: Literal["break", "raise"] = "raise",
        ) -> LoadJob | None:
        """JSON 형식의 `list[dict]` 데이터를 BigQuery 테이블에 적재한다."""
        if not self._validate_table_exists(table, if_table_not_found):
            return None

        if serialize:
            import json
            values = json.loads(json.dumps(values, ensure_ascii=False, default=str))

        project_table = f"{self.project_id}.{table}"
        table_schema = self._auto_detect_schema(table, schema)
        job_option = {"write_disposition": write.upper()}
        job_config = self._build_load_job_config(table_schema, **job_option)
        return self.conn.load_table_from_json(values, project_table, job_config=job_config).result()

    def load_table_from_file(
            self,
            table: str,
            file_obj: IO[bytes],
            format: Literal["avro", "csv", "json", "orc", "parquet"],
            schema: Literal["auto"] | TableId | Sequence[dict | SchemaField] = "auto",
            write: Literal["append", "empty", "truncate", "truncate_data"] = "append",
            if_table_not_found: Literal["break", "raise"] = "raise",
        ) -> LoadJob | None:
        """지정된 형식의 바이너리 파일을 BigQuery 테이블에 적재한다."""
        if not self._validate_table_exists(table, if_table_not_found):
            return None

        project_table = f"{self.project_id}.{table}"
        table_schema = self._auto_detect_schema(table, schema)
        job_option = {"source_format": format.upper(), "write_disposition": write.upper()}
        job_config = self._build_load_job_config(table_schema, **job_option)
        return self.conn.load_table_from_file(file_obj, project_table, job_config=job_config).result()

    def _auto_detect_schema(
            self,
            table: str,
            schema: Literal["auto"] | TableId | Sequence[dict | SchemaField] = "auto",
        ) -> list[dict | SchemaField] | None:
        """스키마가 `auto`라면 기존 테이블에서 스키마를 자동 감지한다."""
        if isinstance(schema, str):
            if schema == "auto":
                return self.get_schema(table) if self.table_exists(table) else None
            return self.get_schema(schema)
        return schema

    def _build_load_job_config(
            self,
            schema: Sequence[dict | SchemaField] | None = None,
            source_format: Literal["AVRO", "CSV", "JSON", "ORC", "PARQUET"] | None = None,
            write_disposition: Literal["APPEND", "EMPTY", "TRUNCATE", "TRUNCATE_DATA"] | None = None,
            **kwargs
        ) -> LoadJobConfig:
        """스키마, 파일 포맷, 쓰기 모드 설정을 적용한 `LoadJobConfig` 객체를 생성한다."""
        from google.cloud.bigquery.job import LoadJobConfig
        if schema is not None:
            kwargs["schema"] = self._build_schema(schema)
        if source_format is not None:
            kwargs["source_format"] = "NEWLINE_DELIMITED_JSON" if source_format == "JSON" else source_format
        if write_disposition is not None:
            kwargs["write_disposition"] = f"WRITE_{write_disposition}"
        return LoadJobConfig(**kwargs)

    def _build_schema(self, fields: Sequence[dict | SchemaField]) -> list[SchemaField]:
        """`list[dict]`를 `SchemaField` 리스트로 변환한다."""
        from google.cloud.bigquery import SchemaField
        def build(type: str | None = None, **kwargs) -> SchemaField:
            if type is not None:
                kwargs["field_type"] = type
            return SchemaField(**kwargs)
        return [field if isinstance(field, SchemaField) else build(**field) for field in fields]

    def _validate_table_exists(self, table: str, if_table_not_found: Literal["break", "raise"] = "raise") -> bool:
        """스키마 및 테이블 존재 여부를 검증한다.
            1. 테이블이 있으면 `True`를 반환한다.
            2. 테이블이 없고 `if_table_not_found="break"` 조건에서는 `False`를 반환한다.
            3. 테이블이 없고 `if_table_not_found="raise"` 조건에서는 `NotFound` 예외를 발생시킨다.
        """
        if self.table_exists(table):
            return True
        elif if_table_not_found == "break":
            return False
        else:
            from google.api_core.exceptions import NotFound
            raise NotFound(f"Table '{self.project_id}.{table}' was not found.")

    ############################## Merge ##############################

    def merge_into_table(
            self,
            source_table: str,
            target_table: str,
            on_conflict: str | Sequence[str],
            matched: Clause
                | dict[str, Literal["replace", "ignore", "greatest", "least", "source_first", "target_first"]]
                | Literal[":replace_all:", ":do_nothing:"] = ":replace_all:",
            not_matched: Clause | Columns | Literal[":insert_all:", ":do_nothing:"] = ":insert_all:",
            where_clause: Clause | None = None,
        ) -> LoadJob:
        """MERGE 문으로 소스 테이블을 타겟 테이블에 병합한다.

        **NOTE** WHERE 절에서 타겟 테이블 칼럼은 `T.`, 소스 테이블 칼럼은 `S.`로 참조한다.
        """
        query = self._compose_merge_query(source_table, target_table, on_conflict, matched, not_matched, where_clause)
        return self.execute_job(query)

    def _compose_merge_query(
            self,
            source_table: str,
            target_table: str,
            on_conflict: str | Sequence[str],
            matched: Clause
                | dict[str, Literal["replace", "ignore", "greatest", "least", "source_first", "target_first"]]
                | Literal[":replace_all:", ":do_nothing:"] = ":replace_all:",
            not_matched: Clause | Columns | Literal[":insert_all:", ":do_nothing:"] = ":insert_all:",
            where_clause: Clause | None = None,
        ) -> str:
        """MERGE 실행에 사용할 쿼리 문자열을 생성한다."""
        on = [f"T.{col} = S.{col}" for col in ([on_conflict] if isinstance(on_conflict, str) else on_conflict)]
        return concat_sql(
            f"MERGE INTO `{self.project_id}.{target_table}` AS T",
            f"USING `{self.project_id}.{source_table}` AS S",
            "ON {}".format(" AND ".join(on + ([where_clause] if where_clause else list()))),
            self._compose_merge_matched(matched, target_table, on_conflict), # WHEN MATCHED THEN UPDATE SET
            self._compose_merge_not_matched(not_matched, target_table), # WHEN NOT MATCHED THEN INSERT
        )

    def _compose_merge_matched(
            self,
            matched: Clause
                | dict[str, Literal["replace", "ignore", "greatest", "least", "source_first", "target_first"]]
                | Literal[":replace_all:", ":do_nothing:"] = ":replace_all:",
            target_table: str = str(),
            on_conflict: str | Sequence[str] = list(),
        ) -> str:
        """MERGE 문의 WHEN MATCHED THEN UPDATE SET 절을 생성한다."""
        prefix = "WHEN MATCHED THEN UPDATE SET "
        if matched == ":replace_all:":
            on = [on_conflict] if isinstance(on_conflict, str) else on_conflict
            return self._compose_merge_matched({col: "replace" for col in self.get_columns(target_table) if col not in on})
        elif matched == ":do_nothing:":
            return None
        elif isinstance(matched, dict):
            def render(col: str, agg: str) -> str:
                if agg in {"source_first", "target_first"}:
                    kwargs = dict(zip(["left", "right"], ('S','T') if agg == "source_first" else ('T','S')))
                    return "COALESCE({left}.{col}, {right}.{col})".format(col=col, **kwargs)
                elif agg in {"greatest", "least"}:
                    return f"{agg.upper()}(S.{col}, T.{col})"
                elif agg in {"replace", "ignore"}:
                    return f"S.{col}" if agg == "replace" else f"T.{col}"
                return f"{agg}({col})"
            return prefix + ", ".join([f"T.{col} = {render(col, agg)}" for col, agg in matched.items()])
        return prefix + str(matched)

    def _compose_merge_not_matched(
            self,
            not_matched: Clause | Columns | Literal[":insert_all:", ":do_nothing:"] = ":insert_all:",
            target_table: str = str(),
        ) -> str:
        """MERGE 문의 WHEN NOT MATCHED THEN INSERT 절을 생성한다."""
        prefix = "WHEN NOT MATCHED THEN "
        if not_matched == ":insert_all:":
            return self._compose_merge_not_matched(self.get_columns(target_table))
        elif not_matched == ":do_nothing:":
            return None
        elif isinstance(not_matched, str):
            return prefix + not_matched
        elif isinstance(not_matched, Sequence):
            return prefix + "INSERT ({}) VALUES ({})".format(", ".join(not_matched), "S."+", S.".join(not_matched))
        return prefix + str(not_matched)

    ########################## Load and Merge #########################

    def merge_into_table_from_file(
            self,
            target_table: str,
            source_file: IO[bytes],
            source_format: Literal["avro", "csv", "json", "orc", "parquet"],
            on_conflict: str | Sequence[str],
            matched: Clause
                | dict[str, Literal["replace", "ignore", "greatest", "least", "source_first", "target_first"]]
                | Literal[":replace_all:", ":do_nothing:"] = ":replace_all:",
            not_matched: Clause | Columns | Literal[":insert_all:", ":do_nothing:"] = ":insert_all:",
            where_clause: Clause | None = None,
            schema: Literal["auto"] | TableId | Sequence[dict | SchemaField] = "auto",
            cleanup_staging_table: bool = True,
        ) -> LoadJob | None:
        """파일을 스테이징 테이블에 적재한 후 타겟 테이블에 MERGE한다.

        **NOTE** WHERE 절에서 타겟 테이블 칼럼은 `T.`, 소스 테이블 칼럼은 `S.`로 참조한다.
        """
        staging_table = None
        try:
            staging_table = self._prepare_staging_table(target_table)
            self.load_table_from_file(staging_table, source_file, source_format, schema)
            query = self._compose_merge_query(staging_table, target_table, on_conflict, matched, not_matched, where_clause)
            return self.execute_job(query)
        finally:
            if cleanup_staging_table and staging_table:
                self.execute_job(f"DROP TABLE IF EXISTS `{self.project_id}.{staging_table}`;")

    def _prepare_staging_table(
            self,
            target_table: str,
            staging_table: str | None = None,
        ) -> BigQueryTable:
        """대상 테이블 스키마를 복제하여 빈 스테이징 테이블을 생성한다."""
        staging_table = staging_table or build_temp_table_name(target_table)
        while self.table_exists(staging_table):
            staging_table = build_temp_table_name(target_table)
        self.copy_table(target_table, staging_table, limit=0, option="replace")
        return staging_table

    ############################## DuckDB #############################

    def load_table_from_duckdb(
            self,
            connection: DuckDBConnection,
            source_table: DuckDBTable,
            target_table: BigQueryTable,
            columns: Sequence[str] = list(),
            schema: Literal["auto"] | TableId | Sequence[dict | SchemaField] = "auto",
            write: Literal["append", "empty", "truncate", "truncate_data"] = "append",
            if_source_table_empty: Literal["break", "continue"] | None = "break",
            if_target_table_not_found: Literal["break", "raise"] = "raise",
        ) -> bool:
        """DuckDB 테이블을 BigQuery 테이블에 적재한다."""
        from io import BytesIO

        if self._is_duckdb_table_empty(connection, source_table, if_source_table_empty):
            return True

        if not self._validate_table_exists(target_table, if_target_table_not_found):
            return False

        columns = ", ".join(columns) if columns else '*'
        table_schema = self._auto_detect_schema(target_table, schema)
        file_obj = BytesIO(connection.fetch_all_to_parquet(f"SELECT {columns} FROM {source_table}"))
        self.load_table_from_file(target_table, file_obj, "parquet", table_schema, write)
        return True

    def load_table_from_duckdb_by_partition(
            self,
            connection: DuckDBConnection,
            source_table: DuckDBTable,
            target_table: BigQueryTable,
            partition_by: PartitionOptions,
            columns: Sequence[str] = list(),
            schema: Literal["auto"] | TableId | Sequence[dict | SchemaField] = "auto",
            write: Literal["append", "empty", "truncate", "truncate_data"] = "append",
            if_source_table_empty: Literal["break", "continue"] | None = "break",
            if_target_table_not_found: Literal["break", "raise"] = "raise",
            progress: bool = True,
        ) -> tuple[bool, int]:
        """DuckDB 테이블을 BigQuery 테이블에 파티션 단위로 나눠서 적재한다. 성공 여부와 성공한 파티션 개수를 반환한다."""
        from linkmerce.common.load import DuckDBIterator
        from linkmerce.utils.progress import import_tqdm
        from io import BytesIO
        tqdm = import_tqdm()

        if self._is_duckdb_table_empty(connection, source_table, if_source_table_empty):
            return True, 0

        if not self._validate_table_exists(target_table, if_target_table_not_found):
            return False, 0

        iterator = (DuckDBIterator(connection, format="parquet")
                    .from_table(source_table)
                    .with_columns(columns)
                    .partition_by(**partition_by))

        step = 0
        table_schema = self._auto_detect_schema(target_table, schema)
        for partition in tqdm(iterator, desc=f"Uploading data to '{target_table}'", disable=(not progress)):
            write_ = write if step == 0 else "append"
            job = self.load_table_from_file(target_table, BytesIO(partition), "parquet", table_schema, write_)
            if job is None:
                return False, step
            step += 1
        return True, step

    def overwrite_table_from_duckdb(
            self,
            connection: DuckDBConnection,
            source_table: DuckDBTable,
            target_table: BigQueryTable,
            columns: Sequence[str] = list(),
            where_clause: Clause | None = "TRUE",
            schema: Literal["auto"] | TableId | Sequence[dict | SchemaField] = "auto",
            if_source_table_empty: Literal["break", "continue"] | None = "break",
            if_target_table_not_found: Literal["break", "raise"] = "raise",
            cleanup_staging_table: bool = True,
        ) -> bool:
        """DuckDB 테이블을 스테이징 테이블에 적재한 후, BigQuery 테이블의 기존 데이터를 삭제하고 스테이징 테이블 행을 덮어쓴다.

        **NOTE** 삭제(DELETE) 및 적재(INSERT) 쿼리 실행 앞뒤로 트랜잭션 시작과 커밋을 실행하며,   
        쿼리가 실패하면 직전 시점으로 롤백한다.
        """
        if self._is_duckdb_table_empty(connection, source_table, if_source_table_empty):
            return True

        common = (connection, source_table, target_table, columns, schema)
        if not self.table_has_rows(target_table, where_clause):
            return self.load_table_from_duckdb(*common, "append", None, if_target_table_not_found)

        staging_table = None
        try:
            staging_table = self._prepare_staging_table_from_duckdb(*common)
            project_table = f"`{self.project_id}.{target_table}`"

            if where_clause is None:
                delete_clause = f"TRUNCATE TABLE {project_table};"
            else:
                delete_clause = concat_sql("DELETE FROM", project_table, where(where_clause))

            insert_clause = f"INSERT INTO {project_table} SELECT * FROM `{self.project_id}.{staging_table}`;"
            query = f"BEGIN TRANSACTION; {delete_clause} {insert_clause} COMMIT TRANSACTION;"
            self.execute_job(query)
            return True
        finally:
            if cleanup_staging_table and staging_table:
                self.execute_job(f"DROP TABLE IF EXISTS `{self.project_id}.{staging_table}`;")

    def merge_into_table_from_duckdb(
            self,
            connection: DuckDBConnection,
            source_table: DuckDBTable,
            target_table: BigQueryTable,
            columns: Sequence[str] = list(),
            where_clause: Clause | None = None,
            on_conflict: str | Sequence[str] = list(),
            matched: Clause
                | dict[str, Literal["replace", "ignore", "greatest", "least", "source_first", "target_first"]]
                | Literal[":replace_all:", ":do_nothing:"] = ":replace_all:",
            not_matched: Clause | Columns | Literal[":insert_all:", ":do_nothing:"] = ":insert_all:",
            schema: Literal["auto"] | TableId | Sequence[dict | SchemaField] = "auto",
            if_source_table_empty: Literal["break", "continue"] | None = "break",
            if_target_table_not_found: Literal["break", "raise"] = "raise",
            cleanup_staging_table: bool = True,
        ) -> bool:
        """DuckDB 테이블을 스테이징 테이블에 적재한 후, 스테이징 테이블을 BigQuery 테이블에 MERGE 한다.

        **NOTE** WHERE 절에서 타겟 테이블 칼럼은 `T.`, 소스 테이블 칼럼은 `S.`로 참조한다.
        """
        if self._is_duckdb_table_empty(connection, source_table, if_source_table_empty):
            return True

        common = (connection, source_table, target_table, columns, schema)
        if where_clause is not None:
            import re
            unalias_where = re.sub(r"(^|[^A-Za-z0-9_])(T|S)\.", r"\1", where_clause)
        else:
            unalias_where = "TRUE"

        if not self.table_has_rows(target_table, unalias_where):
            return self.load_table_from_duckdb(*common, "append", None, if_target_table_not_found)

        staging_table = None
        try:
            staging_table = self._prepare_staging_table_from_duckdb(*common)
            query = self._compose_merge_query(staging_table, target_table, on_conflict, matched, not_matched, where_clause)
            self.execute_job(query)
            return True
        finally:
            if cleanup_staging_table and staging_table:
                self.execute_job(f"DROP TABLE IF EXISTS `{self.project_id}.{staging_table}`;")

    def _is_duckdb_table_empty(
            self,
            connection: DuckDBConnection,
            source_table: str,
            if_source_table_empty: Literal["break", "continue"] | None = "break",
        ) -> bool:
        """`if_source_table_empty` 값에 따라 DuckDB 테이블을 검증한다.
            - `"break"`: 테이블 행이 없는지 여부를 반환한다.
            - `"continue"`: 테이블이 없는지 여부를 반환한다.
            - `None`: 테이블을 검증하지 않는다.
        """
        if if_source_table_empty == "break":
            return not connection.table_has_rows(source_table)
        elif if_source_table_empty == "continue":
            return not connection.table_exists(source_table)
        return False

    def _prepare_staging_table_from_duckdb(
            self,
            connection: DuckDBConnection,
            source_table: DuckDBTable,
            target_table: BigQueryTable,
            columns: Sequence[str] = list(),
            schema: Literal["auto"] | TableId | Sequence[dict | SchemaField] = "auto",
        ) -> BigQueryTable:
        """DuckDB 소스 테이블 행을 적재할 빈 스테이징 테이블을 생성하고 적재한다."""
        staging_table = self._prepare_staging_table(target_table)
        self.load_table_from_duckdb(connection, source_table, staging_table, columns, schema,
            write="append", if_source_table_empty=None, if_target_table_not_found="raise")
        return staging_table

    ############################ Expression ###########################

    def expr_cast(self, value: Any | None, type: str, alias: str = str(), safe: bool = False) -> str:
        """`CAST` 또는 `SAFE_CAST` SQL 표현식을 생성한다."""
        cast = "SAFE_CAST" if safe else "CAST"
        alias = f" AS {alias}" if alias else str()
        return f"{cast}({self.expr_value(value)} AS {type.upper()})" + alias

    def expr_interval(expr: str, days: int | None = None, time: bool = True) -> str:
        """`DATE_ADD` 또는 `DATE_SUB` 날짜 간격 SQL 표현식을 생성한다."""
        if isinstance(days, int):
            func = ("DATE{}_SUB" if days < 0 else "DATE{}_ADD").format("TIME" if time else str())
            return f"{func}({expr}, INTERVAL {abs(days)} DAY)"
        return expr

    def expr_now(
            self,
            type: Literal["DATETIME", "STRING"] = "DATETIME",
            format: str | None = "%Y-%m-%d %H:%M:%S",
            interval: str | int | None = None,
            tzinfo: str | None = None,
        ) -> str:
        """`CURRENT_DATETIME` SQL 표현식을 생성한다."""
        expr = "CURRENT_DATETIME({})".format(f"'{tzinfo}'" if tzinfo else str())
        expr = self.expr_interval(expr, interval, time=True)
        if format:
            expr = f"FORMAT_DATE('{format}', {expr})"
            if type.upper() == "DATETIME":
                return f"CAST({expr} AS DATETIME)"
        return expr if type.upper() == "DATETIME" else "NULL"

    def expr_today(
            self,
            type: Literal["DATE", "STRING"] = "DATE",
            format: str | None = "%Y-%m-%d",
            interval: str | int | None = None,
            tzinfo: str | None = None,
        ) -> str:
        """`CURRENT_DATE` SQL 표현식을 생성한다."""
        expr = "CURRENT_DATE({})".format(f"'{tzinfo}'" if tzinfo else str())
        expr = self.expr_interval(expr, interval, time=False)
        if (type.upper() == "STRING") and format:
            return f"FORMAT_DATE('{format}', {expr})"
        return expr if type.upper() == "DATE" else "NULL"

    ############################## Helper #############################

    def table_exists(self, table: str) -> bool:
        """테이블 존재 여부를 확인한다."""
        from google.api_core.exceptions import NotFound
        try:
            self.conn.get_table(f"{self.project_id}.{table}")
            return True
        except NotFound:
            return False

    def table_has_rows(self, table: str, where_clause: str | None = None) -> bool:
        """테이블이 존재하고 데이터가 있는지 확인한다."""
        if self.table_exists(table):
            query = concat_sql(
                f"SELECT EXISTS (SELECT 1 FROM `{self.project_id}.{table}`",
                where(where_clause),
                "LIMIT 1)",
            )
            return self.fetch_one(query)
        return False

    def count_table(self, table: str, where_clause: str | None = None) -> int:
        """테이블의 행 수를 집계한다."""
        query = concat_sql(f"SELECT COUNT(*) FROM `{self.project_id}.{table}`", where(where_clause))
        return self.fetch_one(query)

    def get_table(self, table: str) -> Table:
        """BigQuery `Table` 객체를 반환한다."""
        return self.conn.get_table(f"{self.project_id}.{table}")

    def ref_table(self, table: str, schema: TableId | Sequence[dict | SchemaField]) -> Table:
        """스키마를 적용한 `Table` 참조 객체를 생성한다."""
        from google.cloud.bigquery import Table
        if isinstance(schema, str):
            schema = self.get_schema(schema)
        if not isinstance(schema, Sequence):
            raise ValueError("Invalid schema: expected sequence of schema fields.")
        return Table(table, schema)

    def get_schema(self, table: str) -> list[SchemaField]:
        """테이블의 스키마(`SchemaField` 리스트)를 반환한다."""
        return self.conn.get_table(f"{self.project_id}.{table}").schema

    def get_columns(self, table: str) -> list[str]:
        """테이블의 칼럼명 리스트를 반환한다."""
        return [field.name for field in self.get_schema(table)]
