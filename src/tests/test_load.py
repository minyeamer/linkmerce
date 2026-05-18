"""데이터 적재(Load) 테스트: DuckDB -> BigQuery/PostgreSQL 적재 동작 검증
- 실행: `pytest src/tests/test_load.py -m load -v -s`
- 결과: 외부 저장소 테이블에 직접 적재 후 건수와 행 데이터를 검증한다.
"""
from __future__ import annotations

from tests.conftest import LoaderHarness
import pytest

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Callable
    Harness = Callable[[str], LoaderHarness]

pytestmark = pytest.mark.load


BACKENDS = [
    pytest.param("bigquery", marks=pytest.mark.load_bigquery, id="bigquery"),
    pytest.param("postgres", marks=pytest.mark.load_postgres, id="postgres"),
]

WHERE_CASES = [
    pytest.param(0, id="where-partial"),
    pytest.param(1, id="where-empty"),
    pytest.param(2, id="where-all"),
]

CONFLICT_CASES = [
    pytest.param(0, id="replace-all"),
    pytest.param(1, id="do-nothing"),
    pytest.param(2, id="replace-partial"),
]


###################################################################
###################### load_table_from_duckdb #####################
###################################################################

class TestLoadTable:
    """외부 저장소 적재 테스트 (APPEND)
    - success
    - unique_violation
    - missing_target
    - select_columns
    - column_mismatch
    - type_mismatch
    """

    @pytest.mark.mode_append
    @pytest.mark.parametrize("backend", BACKENDS)
    def test_load_table_success(self, loader_harness: Harness, backend: str):
        """소스 테이블을 타겟 테이블에 적재하여 실행 상태가 정상이고 행 수가 일치하는지 검증한다."""
        harness = loader_harness(backend)
        status = harness.load_table_from_duckdb()
        count = harness.client.fetch_one(f"SELECT COUNT(*) FROM {harness.target_ref};")

        assert status and (count == harness.total)

    @pytest.mark.mode_append
    @pytest.mark.parametrize("backend", BACKENDS)
    def test_load_table_unique_violation(self, loader_harness: Harness, backend: str):
        """소스 테이블을 타겟 테이블에 중복 적재했을 때 UNIQUE 제약 조건을 위반하는지 검증한다."""
        if backend == "bigquery":
            pytest.skip("BigQuery does not support UNIQUE constraints.")
        from duckdb import Error # psycopg2.errors.UniqueViolation

        harness = loader_harness(backend)
        try:
            harness.load_table_from_duckdb()
            harness.load_table_from_duckdb()
            assert False
        except Error:
            assert True

    @pytest.mark.mode_append
    @pytest.mark.parametrize("backend", BACKENDS)
    def test_load_table_missing_target(self, loader_harness: Harness, backend: str):
        """타겟 테이블이 존재하지 않을 시 `break`, `raise`, `create` 정책을 검증한다."""
        from google.api_core.exceptions import NotFound
        from psycopg2.errors import UndefinedTable

        harness = loader_harness(backend)
        harness.execute(f"DROP TABLE IF EXISTS {harness.target_ref};")

        status = harness.load_table_from_duckdb(if_target_table_not_found="break")
        if status:
            assert False

        errors = {"bigquery": NotFound, "postgres": UndefinedTable}
        harness.execute(f"DROP TABLE IF EXISTS {harness.target_ref};")
        try:
            harness.load_table_from_duckdb(if_target_table_not_found="raise")
            assert False
        except errors[backend]:
            pass

        if backend == "postgres":
            harness.execute(f"DROP TABLE IF EXISTS {harness.target_ref};")
            status = harness.load_table_from_duckdb(if_target_table_not_found="create")
            if not (status or harness.client.table_exists(harness.target_table)):
                assert False
        assert True

    @pytest.mark.mode_append
    @pytest.mark.parametrize("backend", BACKENDS)
    def test_load_table_select_columns(self, loader_harness: Harness, backend: str):
        """일부 칼럼만 선택하여 적재되는지 검증한다. 제외할 칼럼은 NULLABLE해야 한다."""
        harness = loader_harness(backend)
        columns = harness.spec["select_columns"]["load"]
        status = harness.load_table_from_duckdb(columns=columns)
        if status:
            expr = lambda column: "COUNT({}) {}".format(column, "> 0" if column in columns else "= 0")
            count_clause = ", ".join(map(expr, harness.columns))
            query = f"SELECT {count_clause} FROM {harness.target_ref};"
            assert all(harness.client.fetch_values(query, axis=0))
        else:
            assert False

    @pytest.mark.mode_append
    @pytest.mark.parametrize("backend", BACKENDS)
    def test_load_table_column_mismatch(self, loader_harness: Harness, backend: str):
        """타겟 테이블의 일부 칼럼명을 변경하여 적재했을 때 의도한 오류가 발생하는지 검증한다."""
        if backend == "bigquery":
            pytest.skip("BigQuery load job uses positional mapping for Parquet files.")
        from duckdb import Error # psycopg2.errors.UndefinedColumn

        harness = loader_harness(backend)
        for old, new in harness.spec["rename_columns"].items():
            query = f"ALTER TABLE {harness.target_ref} RENAME COLUMN {old} TO {new};"
            harness.execute(query)

        errors = {"postgres": Error}
        try:
            harness.load_table_from_duckdb()
            assert False
        except errors[backend]:
            assert True

    @pytest.mark.mode_append
    @pytest.mark.parametrize("backend", BACKENDS)
    def test_load_table_type_mismatch(self, loader_harness: Harness, backend: str):
        """타겟 테이블의 일부 칼럼 타입을 변경하여 적재했을 때 의도한 오류가 발생하는지 검증한다."""
        from google.api_core.exceptions import BadRequest
        from duckdb import Error # psycopg2.errors.InvalidTextRepresentation

        harness = loader_harness(backend)
        harness.execute(f"DROP TABLE IF EXISTS {harness.target_ref};")

        repl = harness.spec["replace_columns"]
        columns = list()
        for column in harness.columns:
            columns.append(f"{repl[column]} AS {column}" if column in repl else column)

        base_ref = harness.table_ref(harness.target["base_table"])
        query = f"CREATE TABLE {harness.target_ref} AS SELECT {', '.join(columns)} FROM {base_ref};"
        harness.execute(query)

        errors = {"bigquery": BadRequest, "postgres": Error}
        try:
            harness.load_table_from_duckdb()
            assert False
        except errors[backend]:
            assert True


###################################################################
################### overwrite_table_from_duckdb ###################
###################################################################

class TestOverwriteTable:
    """외부 저장소 적재 테스트 (OVERWRITE)
    - values
    """

    @pytest.mark.mode_overwrite
    @pytest.mark.parametrize("backend", BACKENDS)
    def test_overwrite_table_values(self, loader_harness: Harness, backend: str):
        """소스 테이블을 타겟 테이블에 덮어쓰기하여 실행 상태가 정상이고 의도적으로 값이 변경되었는지 검증한다.

        값 변경 규칙(`update_rules`)과 WHERE 문(`where_clauses`)을 기준으로,   
        덮어쓰기 조건에 해당되면 값이 변경되고, 해당되지 않으면 값이 유지되는지 비교한다.
        """
        from linkmerce.common.load import concat_sql, where
        from linkmerce.extensions.bigquery import build_staging_table_name

        harness = loader_harness(backend)
        update_rules: dict = harness.spec["update_rules"]["overwrite"]
        projections = ", ".join([f"{update_rules.get(column, column)} AS {column}" for column in harness.columns])
        temp_table = build_staging_table_name(harness.source)
        harness.duckdb.copy_table(harness.source, temp_table, projections, option="replace")

        updated_columns = ", ".join(update_rules.keys())
        for where_clause, where_inverse in harness.spec["where_clauses"]:
            status = harness.overwrite_table_from_duckdb(where_clause)
            if not status:
                assert False

            source_set = harness.duckdb.fetch_all_to_json(f"SELECT {updated_columns} FROM {where(where_clause)};")
            target_changes = harness.client.fetch_all_to_json(f"SELECT {updated_columns} FROM {where(where_clause)};")

            for source, target in zip(harness.fetch_all_source_rows(where_clause), harness.fetch_all_target_rows(where_clause)):
            

    def compare_source_target(
            self,
            source_table: str | None,
            target_table: str | None,
            where_clause: str | None = None,
        ) -> list[dict]:
        """WHERE 문"""
        from linkmerce.common.load import concat_sql
        order_by = "ORDER BY {}".format(self.target["primary_key"])
        query = concat_sql(f"SELECT * FROM {Harness.}", where_clause, order_by)
        return self.duckdb.fetch_all_to_json(query)

        assert status and (harness.count_target_table() == harness.total)






# @pytest.mark.parametrize("backend", BACKENDS)
# def test_load_table_from_duckdb_missing_target_create_schema_mismatch(self, loader_harness: Harness, backend: str):
#     """create 정책과 구성 불일치가 같이 발생할 때 생성된 타겟 테이블이 남는지 검증한다."""
#     harness = loader_harness(backend)
#     target_table = harness.next_target_temp_table()
#     source_temp_table = harness.create_source_temp_table(rename_rules=harness.spec["rename_columns"])

#     try:
#         harness.call(
#             "load_table_from_duckdb",
#             source_table=source_temp_table,
#             target_table=target_table,
#             if_target_table_not_found="create",
#         )
#     except Exception:
#         pass

#     assert harness.target_exists(target_table)


# @pytest.mark.parametrize("backend", BACKENDS)
# def test_load_table_from_duckdb_columns_subset(self, loader_harness: Harness, backend: str):
#     """지정된 일부 칼럼만 적재할 때 누락 칼럼이 NULL인지 검증한다."""
#     harness = loader_harness(backend)
#     target_table = harness.create_target_table()
#     selected_columns = harness.spec["select_columns"]["load"]
#     null_columns = [column for column in ["account_group", "account_seq"] if column not in selected_columns]

#     assert harness.call(
#         "load_table_from_duckdb",
#         target_table=target_table,
#         columns=selected_columns,
#         if_target_table_not_found="raise",
#     ) is True

#     _assert_target_matches_source(
#         harness,
#         target_table,
#         columns=selected_columns,
#         null_columns=null_columns,
#     )


# @pytest.mark.parametrize("backend", BACKENDS)
# @pytest.mark.parametrize("where_index", WHERE_CASES)
# def test_overwrite_table_from_duckdb_where_clause(loader_harness: Harness, backend: str, where_index: int):
#     """WHERE 절에 따라 부분 덮어쓰기 범위가 달라지는지 검증한다."""
#     harness = loader_harness(backend)
#     target_table = _seed_target_table(harness, harness.next_target_temp_table())
#     before_rows = harness.fetch_target_rows(target_table)
#     where_clause = harness.spec["where_clauses"][where_index]
#     source_temp_table = harness.create_source_temp_table(
#         mutation_rules=harness.spec["update_rules"]["overwrite"],
#         where_clause=where_clause,
#     )
#     changed_ids = harness.source_ids(source_temp_table)

#     assert harness.call(
#         "overwrite_table_from_duckdb",
#         source_table=source_temp_table,
#         target_table=target_table,
#         where_clause=where_clause,
#         if_target_table_not_found="raise",
#     ) is True

#     _assert_row_updates(
#         before_rows,
#         harness.fetch_target_rows(target_table),
#         changed_ids,
#         source_rows=harness.fetch_source_rows(source_temp_table),
#     )


# @pytest.mark.parametrize("backend", BACKENDS)
# def test_overwrite_table_from_duckdb_schema_mismatch_keeps_staging(loader_harness: Harness, backend: str):
#     """구성 불일치 오류 시 내부 스테이징 테이블이 롤백되지 않고 남는지 검증한다."""
#     harness = loader_harness(backend)
#     target_table = _seed_target_table(harness, harness.next_target_temp_table())
#     source_temp_table = harness.create_source_temp_table(rename_rules=harness.spec["rename_columns"])
#     staging_before = set(harness.list_staging_tables(target_table))

#     with pytest.raises(Exception):
#         harness.call(
#             "overwrite_table_from_duckdb",
#             source_table=source_temp_table,
#             target_table=target_table,
#             where_clause=None,
#             if_target_table_not_found="raise",
#         )

#     staging_after = set(harness.list_staging_tables(target_table))
#     assert staging_after > staging_before


# @pytest.mark.parametrize("backend", BACKENDS)
# def test_overwrite_table_from_duckdb_columns_subset(loader_harness: Harness, backend: str):
#     """덮어쓰기 시 일부 칼럼만 적재하면 누락 칼럼이 NULL로 대체되는지 검증한다."""
#     harness = loader_harness(backend)
#     target_table = _seed_target_table(harness, harness.next_target_temp_table())
#     before_rows = harness.fetch_target_rows(target_table)
#     where_clause = harness.spec["where_clauses"][0]
#     source_temp_table = harness.create_source_temp_table(
#         mutation_rules=harness.spec["update_rules"]["overwrite"],
#         where_clause=where_clause,
#     )
#     changed_ids = harness.source_ids(source_temp_table)
#     selected_columns = harness.spec["select_columns"]["overwrite"]
#     null_columns = [column for column in before_rows[0].keys() if column not in selected_columns]

#     assert harness.call(
#         "overwrite_table_from_duckdb",
#         source_table=source_temp_table,
#         target_table=target_table,
#         columns=selected_columns,
#         where_clause=where_clause,
#         if_target_table_not_found="raise",
#     ) is True

#     _assert_row_updates(
#         before_rows,
#         harness.fetch_target_rows(target_table),
#         changed_ids,
#         source_rows=harness.fetch_source_rows(source_temp_table),
#         updated_columns=selected_columns,
#         null_columns=null_columns,
#     )


# @pytest.mark.parametrize("backend", BACKENDS)
# def test_overwrite_table_from_duckdb_cleanup_staging_false(loader_harness: Harness, backend: str):
#     """cleanup_staging_table=False 조건에서 내부 스테이징 테이블이 남는지 검증한다."""
#     harness = loader_harness(backend)
#     target_table = _seed_target_table(harness, harness.next_target_temp_table())
#     source_temp_table = harness.create_source_temp_table(mutation_rules=harness.spec["update_rules"]["overwrite"])
#     staging_before = set(harness.list_staging_tables(target_table))

#     assert harness.call(
#         "overwrite_table_from_duckdb",
#         source_table=source_temp_table,
#         target_table=target_table,
#         where_clause=None,
#         cleanup_staging_table=False,
#         if_target_table_not_found="raise",
#     ) is True

#     staging_after = set(harness.list_staging_tables(target_table))
#     assert staging_after > staging_before


# @pytest.mark.parametrize("backend", BACKENDS)
# @pytest.mark.parametrize("where_index", WHERE_CASES)
# @pytest.mark.parametrize("action_index", CONFLICT_CASES)
# def test_conflict_load_methods(loader_harness: Harness, backend: str, where_index: int, action_index: int):
#     """MERGE/UPSERT 동작이 where 절과 충돌 정책에 따라 달라지는지 검증한다."""
#     harness = loader_harness(backend)
#     target_table = _seed_target_table(harness, harness.next_target_temp_table())
#     before_rows = harness.fetch_target_rows(target_table)
#     where_clause = harness.spec["where_clauses"][where_index]
#     source_temp_table = harness.create_source_temp_table(
#         mutation_rules=harness.spec["update_rules"][_conflict_update_rule_key(backend)],
#         where_clause=where_clause,
#     )
#     matched_ids = harness.source_ids(source_temp_table)
#     action = harness.spec["conflict_actions"][action_index]
#     action_key = _conflict_action_key(backend)

#     kwargs = {
#         "source_table": source_temp_table,
#         "target_table": target_table,
#         "on_conflict": action["on_conflict"],
#         action_key: action["matched"],
#         "where_clause": _qualified_conflict_where(harness, backend, where_clause),
#         "if_target_table_not_found": "raise",
#     }

#     assert harness.call(_conflict_method(backend), **kwargs) is True

#     _assert_row_updates(
#         before_rows,
#         harness.fetch_target_rows(target_table),
#         matched_ids,
#         source_rows=harness.fetch_source_rows(source_temp_table),
#         updated_columns=action["updated_columns"],
#         preserved_columns=action["preserved_columns"],
#     )

















# def _rows_by_account_id(rows: list[dict]) -> dict[str, dict]:
#     return {row["account_id"]: row for row in rows}


# def _seed_target_table(harness: LoaderHarness, table: str | None = None) -> str:
#     table = harness.create_target_table(table)
#     assert harness.call(
#         "load_table_from_duckdb",
#         target_table=table,
#         if_target_table_not_found="raise",
#     ) is True
#     return table


# def _assert_target_matches_source(
#         harness: LoaderHarness,
#         target_table: str,
#         source_table: str | None = None,
#         columns: list[str] | None = None,
#         null_columns: list[str] | None = None,
#     ):
#     source_rows = harness.fetch_source_rows(source_table)
#     target_rows = harness.fetch_target_rows(target_table)
#     source_map = _rows_by_account_id(source_rows)
#     target_map = _rows_by_account_id(target_rows)

#     assert len(target_rows) == len(source_rows)
#     assert set(target_map) == set(source_map)

#     for account_id, source_row in source_map.items():
#         target_row = target_map[account_id]
#         for column in (columns or list(source_row.keys())):
#             assert target_row[column] == source_row[column]
#         for column in (null_columns or list()):
#             assert target_row[column] is None


# def _assert_row_updates(
#         before_rows: list[dict],
#         after_rows: list[dict],
#         changed_ids: set[str],
#         *,
#         source_rows: list[dict] | None = None,
#         updated_columns: list[str] | None = None,
#         preserved_columns: list[str] | None = None,
#         null_columns: list[str] | None = None,
#     ):
#     before_map = _rows_by_account_id(before_rows)
#     after_map = _rows_by_account_id(after_rows)
#     source_map = _rows_by_account_id(source_rows or before_rows)

#     assert len(before_map) == len(before_rows)
#     assert len(after_rows) == len(before_rows)
#     assert len(after_map) == len(after_rows)
#     assert set(after_map) == set(before_map)

#     for account_id, before in before_map.items():
#         after = after_map[account_id]
#         if account_id not in changed_ids:
#             assert after == before
#             continue

#         source = source_map[account_id]
#         updated_columns = updated_columns or list()
#         preserved_columns = preserved_columns or list()
#         null_columns = null_columns or list()

#         if not (updated_columns or preserved_columns or null_columns):
#             assert after == source
#             continue

#         for column in updated_columns:
#             assert after[column] == source[column]
#         for column in preserved_columns:
#             assert after[column] == before[column]
#         for column in null_columns:
#             assert after[column] is None


# def _missing_table_error(backend: str) -> type[Exception]:
#     if backend == "bigquery":
#         from google.api_core.exceptions import NotFound

#         return NotFound

#     from psycopg2.errors import UndefinedTable

#     return UndefinedTable


# def _conflict_method(backend: str) -> str:
#     return "merge_into_table_from_duckdb" if backend == "bigquery" else "upsert_table_from_duckdb"


# def _conflict_action_key(backend: str) -> str:
#     return "matched" if backend == "bigquery" else "do_action"


# def _conflict_update_rule_key(backend: str) -> str:
#     return "merge" if backend == "bigquery" else "upsert"


# def _qualified_conflict_where(harness: LoaderHarness, backend: str, where_clause: str | None) -> str | None:
#     if backend == "bigquery":
#         return harness.qualify_where_clause(where_clause, alias="T")
#     return harness.qualify_where_clause(
#         where_clause,
#         alias="S",
#         mutation_rules=harness.spec["update_rules"][_conflict_update_rule_key(backend)],
#     )