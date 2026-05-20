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


BIGQUERY = 0
POSTGRES = 1

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
        assert harness.load_table_from_duckdb()
        assert harness.client.count_table(harness.target_table) == harness.total

    @pytest.mark.mode_append
    @pytest.mark.parametrize("backend", BACKENDS)
    def test_load_table_unique_violation(self, loader_harness: Harness, backend: str):
        """소스 테이블을 타겟 테이블에 중복 적재했을 때 UNIQUE 제약 조건을 위반하는지 검증한다."""
        if backend == "bigquery":
            pytest.skip("BigQuery does not support UNIQUE constraints.")
        from duckdb import Error # psycopg2.errors.UniqueViolation

        harness = loader_harness(backend)
        try:
            assert harness.load_table_from_duckdb()
            assert harness.load_table_from_duckdb()
            assert False
        except Error:
            pass

    @pytest.mark.mode_append
    @pytest.mark.parametrize("backend", BACKENDS)
    def test_load_table_missing_target(self, loader_harness: Harness, backend: str):
        """타겟 테이블이 존재하지 않을 시 `break`, `raise`, `create` 정책을 검증한다."""
        from google.api_core.exceptions import NotFound
        from psycopg2.errors import UndefinedTable

        harness = loader_harness(backend)
        harness.execute(f"DROP TABLE IF EXISTS {harness.target_ref};")

        # 타겟 테이블이 존재하지 않으면 `False` 반환
        assert not harness.load_table_from_duckdb(if_target_table_not_found="break")

        # 타겟 테이블이 존재하지 않으면 오류 발생
        errors = {"bigquery": NotFound, "postgres": UndefinedTable}
        harness.execute(f"DROP TABLE IF EXISTS {harness.target_ref};")
        try:
            harness.load_table_from_duckdb(if_target_table_not_found="raise")
            assert False
        except errors[backend]:
            pass

        # 타겟 테이블이 존재하지 않으면 테이블 생성 후 적재
        if backend == "postgres":
            harness.execute(f"DROP TABLE IF EXISTS {harness.target_ref};")
            assert harness.load_table_from_duckdb(if_target_table_not_found="create")
            assert harness.client.table_exists(harness.target_table)

    @pytest.mark.mode_append
    @pytest.mark.parametrize("backend", BACKENDS)
    def test_load_table_select_columns(self, loader_harness: Harness, backend: str):
        """일부 칼럼만 선택하여 적재하면 제외된 칼럼 값이 NULL이 되는지 검증한다. 제외할 칼럼은 NULLABLE해야 한다."""
        harness = loader_harness(backend)
        columns = harness.spec["select_columns"]
        assert harness.load_table_from_duckdb(columns=columns)

        count_expr = lambda column: "COUNT({}) {}".format(column, "> 0" if column in columns else "= 0")
        all_count_clause = " AND ".join(map(count_expr, harness.columns))
        query = f"SELECT {all_count_clause} FROM {harness.target_ref};"
        assert harness.client.fetch_one(query)

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
            pass

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
            pass


###################################################################
################### overwrite_table_from_duckdb ###################
###################################################################

class TestOverwriteTable:
    """외부 저장소 적재 테스트 (OVERWRITE)
    - values_diff
    - select_columns
    """

    @pytest.mark.mode_overwrite
    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize("where_index", WHERE_CASES)
    def test_overwrite_table_values_diff(self, loader_harness: Harness, backend: str, where_index: int):
        """소스 테이블을 타겟 테이블에 덮어쓰기하여 값이 변경되었는지 검증한다. 다음 순서를 따른다.
        1. 타겟 테이블에 원본 데이터를 적재하고, 타겟 테이블과 동일한 구성의 임시 테이블을 생성한다.
        2. 값 변경 규칙(`overwrite_rules`)에 따라 덮어쓸 데이터를 변경한다.
        3. WHERE 절(`where_clauses[where_index]`)에 해당되는 임시 테이블에 변경된 데이터를 덮어쓰고 검증한다.
            - 값이 변경되지 않은 칼럼은 타겟 테이블과 임시 테이블의 모든 행에 대해 값이 일치하는지 검증한다.
            - 값이 변경된 칼럼은 WHERE 절에 해당되는 덮어쓰기 대상에 따라 일치 또는 불일치하는지 검증한다.

        **NOTE** 값 변경 규칙(`overwrite_rules`)은 `{칼럼명: 연산식}`으로 구성된다.
        1. 값을 변경할 칼럼은 반드시 하나 이상 지정해야 한다.
        2. WHERE 절 목록(`where_clauses`)에 의해 참조되는 칼럼은 변경하면 안된다.
        3. 테이블 내 모든 값에는 NULL이 없어야 하며, 변경할 값 또한 NULL이 아니어야 한다.

        **NOTE** WHERE 절 목록(`where_clauses`)은 아래 3가지 순서로 구성해야 한다.
        1. `where-partial`: 일부 행만 선택
        2. `where-empty`: 모든 행을 제외
        3. `where-all`: 모든 행을 선택 (`null` 지정 시 `"TRUE"`로 대체)
        """
        from linkmerce.common.load import build_temp_table_name

        harness = loader_harness(backend)
        overwrite_rules = harness.spec["overwrite_rules"]
        where_clause = harness.spec["where_clauses"][where_index] or "TRUE"

        assert harness.load_table_from_duckdb()

        # WHERE 절에 해당하는 행만 덮어쓰기 위해 필터
        where_not = "NOT ({})".format(where_clause)
        harness.duckdb.execute(f"DELETE FROM {harness.source_table} WHERE {where_not};")

        # 타겟 테이블 적재 후 소스 테이블의 값을 변경
        for column, projection in overwrite_rules.items():
            harness.duckdb.execute(f"UPDATE {harness.source_table} SET {column} = {projection};")

        # 타겟 테이블의 구성과 내용을 복제한 임시 테이블을 생성하고, 변경된 소스 테이블의 값 덮어쓰기
        temp_table = build_temp_table_name(harness.target_table)
        copy_options = {"option": "replace"}
        if backend == "postgres":
            copy_options.update({"commit": True, "close": True})
        harness.client.copy_table(harness.target_table, temp_table, **copy_options)
        try:
            assert harness.overwrite_table_from_duckdb(target_table=temp_table, where_clause=where_clause)

            # EXCEPT 연산자를 사용해 값의 변경 여부를 검증하는 쿼리 문자열 동적 생성
            tables = {"target": harness.target_ref, "temp": harness.table_ref(temp_table)}
            updated_columns = ", ".join(overwrite_rules.keys())
            preserved_columns = ", ".join([column for column in harness.columns if column not in overwrite_rules])

            op = "EXCEPT DISTINCT" if backend == "bigquery" else "EXCEPT"
            subquery = f"SELECT {{columns}} FROM {{temp}} WHERE {{where}} {op} SELECT {{columns}} FROM {{target}} WHERE {{where}}"
            diff_query = "SELECT COUNT(*) FROM ({}) AS diff;".format(subquery)

            # 덮어쓰기 전후로 테이블 행 수가 일치하는지 여부 검증
            assert ((harness.client.count_table(harness.target_table) == harness.client.count_table(temp_table)))

            if harness.client.count_table(temp_table, where_clause) == 0:
                    # WHERE 조건에 해당하는 행이 없을 경우 모든 값의 일치 여부 검증
                assert (harness.client.fetch_one(diff_query.format(columns="*", **tables, where="TRUE")) == 0)
            else:   # 3가지 검증을 모두 만족하지 못할 경우 `AssertError` 발생
                        # 값이 변경된 칼럼 + 덮어쓰기 대상에 대해 값의 불일치 여부 검증
                assert (((not updated_columns) or (harness.client.fetch_one(
                        diff_query.format(columns=updated_columns, **tables, where=where_clause)) > 0))
                        # 값이 변경된 칼럼 + 덮어쓰기 대상이 아닌 경우에 대해 값의 일치 여부 검증
                    and ((not updated_columns) or (harness.client.fetch_one(
                        diff_query.format(columns=updated_columns, **tables, where=where_not)) == 0))
                        # 값이 변경되지 않은 칼럼에 대해 값의 일치 여부 검증
                    and ((not preserved_columns) or (harness.client.fetch_one(
                        diff_query.format(columns=preserved_columns, **tables, where="TRUE")) == 0))
                    )
        finally:
            harness.execute(f"DROP TABLE IF EXISTS {harness.table_ref(temp_table)};")

    @pytest.mark.mode_overwrite
    @pytest.mark.parametrize("backend", BACKENDS)
    def test_overwrite_table_select_columns(self, loader_harness: Harness, backend: str):
        """일부 칼럼만 선택하여 모든 행에 대해 덮어쓰기하면 제외된 칼럼 값이 NULL이 되는지 검증한다."""
        harness = loader_harness(backend)
        assert harness.load_table_from_duckdb()

        columns = harness.spec["select_columns"]
        assert harness.overwrite_table_from_duckdb(columns=columns)

        count_expr = lambda column: "COUNT({}) {}".format(column, "> 0" if column in columns else "= 0")
        all_count_clause = " AND ".join(map(count_expr, harness.columns))
        query = f"SELECT {all_count_clause} FROM {harness.target_ref};"
        assert harness.client.fetch_one(query)


###################################################################
##################### upsert_table_from_duckdb ####################
###################################################################

class TestUpsertTable:
    """외부 저장소 적재 테스트 (UPSERT)
    - conflict_actions
    """

    @pytest.mark.mode_upsert
    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize("where_index", WHERE_CASES)
    @pytest.mark.parametrize("action_index", CONFLICT_CASES)
    def test_upsert_table_conflict_actions(
            self,
            loader_harness: Harness,
            backend: str,
            where_index: int,
            action_index: int,
        ):
        """UPSERT 동작이 WHERE 절과 충돌 정책에 따라 의도한 결과를 만들어 내는지 검증한다. 다음 순서를 따른다.
        1. 타겟 테이블에 원본 데이터를 적재하고, 타겟 테이블과 동일한 구성의 임시 테이블을 생성한다.
        2. 값 변경 규칙(`upsert_rules`)에 따라 기존 값은 변경하고 추가할 값을 복제한다.
        3. WHERE 절과 충돌 정책에 따라 UPSERT하고 검증한다.
            - 값이 변경되지 않은 칼럼은 타겟 테이블과 임시 테이블의 모든 행에 대해 값이 일치하는지 검증한다.
            - 값이 변경된 칼럼은 WHERE 절에 해당되는 덮어쓰기 대상에 따라 일치 또는 불일치하는지 검증한다.
            - PK를 다르게 복제하여 추가된 행은 그 수량만큼 적재되었는지 검증한다.

        **NOTE** UPDATE 동작에 대한 값 변경 규칙(`upsert_rules['update']`)은 `{칼럼명: 연산식}`으로 구성된다.
        1. 값을 변경할 칼럼은 반드시 하나 이상 지정해야 한다.
        2. 반대로 모든 칼럼의 값을 변경해서도 안된다. 일부 칼럼만 변경해야 한다.
        3. 충돌 기준(`on_conflict`)과 WHERE 절 목록(`where_clauses`)에 의해 참조되는 칼럼은 변경하면 안된다.
        4. 테이블 내 모든 값에는 NULL이 없어야 하며, 변경할 값 또한 NULL이 아니어야 한다.

        **NOTE** INSERT 동작에 대한 값 변경 규칙(`upsert_rules['insert']`)은 PRIMARY KEY를 포함해야 한다.

        **NOTE** WHERE 절 목록(`where_clauses`)은 아래 3가지 순서로 구성해야 한다.
        1. `where-partial`: 일부 행만 선택
        2. `where-empty`: 모든 행을 제외
        3. `where-all`: 모든 행을 선택 (`null` 지정 시 `"TRUE"`로 대체)

        **NOTE** 충돌 정책 목록(`conflict_actions`)은 아래 3가지 순서로 구성해야 한다.
        1. `replace-all`: 모든 값 덮어쓰기
        2. `do-nothing`: 충돌된 값 무시 (원본 유지)
        3. `replace-partial`: 일부 값만 덮어쓰기 (PK를 제외한 칼럼 단위로 정책 지정)

        **NOTE** 각각의 충돌 정책(`conflict_action`)은 아래 3가지 항목을 포함해야 한다.
        - `on_conflict`: 중복 체크할 칼럼
        - `do_action`: 중복 시 충돌 정책
        - `updated_columns`: MERGE 동작으로 변경될 칼럼 목록
        """
        from linkmerce.common.load import build_temp_table_name

        harness = loader_harness(backend)
        
        upsert_rules = harness.spec["upsert_rules"]
        where_clause = harness.spec["where_clauses"][where_index] or "TRUE"
        conflict_action = harness.spec["conflict_actions"][action_index]

        assert harness.load_table_from_duckdb()

        # WHERE 절에 해당하는 행만 덮어쓰기 위해 필터
        where_not = "NOT ({})".format(where_clause)
        harness.duckdb.execute(f"DELETE FROM {harness.source_table} WHERE {where_not};")

        # WHERE 절로 필터되는 행 수 == INESRT 행 수
        num_inserted = harness.duckdb.count_table(harness.source_table, where_clause)

        # 타겟 테이블 적재 후 소스 테이블의 값을 변경
        for column, projection in upsert_rules["update"].items():
            harness.duckdb.execute(f"UPDATE {harness.source_table} SET {column} = {projection};")

        # 변경된 소스 테이블에서 PRIMARY KEY를 다르게 하여 전체 행 복제
        insert_columns = ", ".join([upsert_rules["insert"].get(column, column) for column in harness.columns])
        insert_query = f"INSERT INTO {harness.source_table} SELECT {insert_columns} FROM {harness.source_table};"
        harness.duckdb.execute(insert_query)

        # 타겟 테이블의 구성과 내용을 복제한 임시 테이블을 생성하고, 변경된 소스 테이블을 UPSERT 실행
        temp_table = build_temp_table_name(harness.target_table)
        copy_options = {"option": "replace"}
        if backend == "postgres":
            copy_options.update({"commit": True, "close": True})
        harness.client.copy_table(harness.target_table, temp_table, **copy_options)
        try:
            assert harness.upsert_table_from_duckdb(backend, target_table=temp_table, **conflict_action)

            # EXCEPT 연산자를 사용해 값의 변경 여부를 검증하는 쿼리 문자열 동적 생성
            tables = {"target": harness.target_ref, "temp": harness.table_ref(temp_table)}
            updated_columns = ", ".join(conflict_action["updated_columns"])
            preserved_columns = ", ".join([column for column in harness.columns if column not in conflict_action["updated_columns"]])

            op = "EXCEPT DISTINCT" if backend == "bigquery" else "EXCEPT"
            subquery = f"SELECT {{columns}} FROM {{temp}} WHERE {{where}} {op} SELECT {{columns}} FROM {{target}} WHERE {{where}}"
            diff_query = "SELECT COUNT(*) FROM ({}) AS diff;".format(subquery)

            # UPSERT 전후로 테이블 행 수가 INSERT 행 수만큼만 차이나는지 여부 검증
            assert ((harness.client.count_table(harness.target_table) + num_inserted) == harness.client.count_table(temp_table))

            if harness.client.count_table(temp_table, where_clause) == 0:
                    # WHERE 조건에 해당하는 행이 없을 경우 모든 값의 일치 여부 검증 (INSERT 행 수만큼 보정)
                assert (harness.client.fetch_one(diff_query.format(columns="*", **tables, where="TRUE")) == num_inserted)
            else:   # 3가지 검증을 모두 만족하지 못할 경우 `AssertError` 발생
                        # 값이 변경된 칼럼 + 덮어쓰기 대상에 대해 값의 불일치 여부 검증 (INSERT 행 수만큼 보정)
                assert (((not updated_columns) or (harness.client.fetch_one(
                        diff_query.format(columns=updated_columns, **tables, where=where_clause)) > num_inserted))
                        # 값이 변경된 칼럼 + 덮어쓰기 대상이 아닌 경우에 대해 값의 일치 여부 검증
                    and ((not updated_columns) or (harness.client.fetch_one(
                        diff_query.format(columns=updated_columns, **tables, where=where_not)) == 0))
                        # 값이 변경되지 않은 칼럼에 대해 값의 일치 여부 검증 (INSERT 행 수만큼 보정)
                    and ((not preserved_columns) or (harness.client.fetch_one(
                        diff_query.format(columns=preserved_columns, **tables, where="TRUE")) == num_inserted))
                    )
        finally:
            harness.execute(f"DROP TABLE IF EXISTS {temp_table};")
