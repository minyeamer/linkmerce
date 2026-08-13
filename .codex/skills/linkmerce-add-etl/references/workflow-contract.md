# LinkMerce ETL Integration Workflow Contract

## Contents

1. Change contract
2. Layer matrix
3. Cross-layer invariants
4. Single-table and multi-table decisions
5. Validation commands
6. Completion gate

## 1. Change contract

Before editing, write a private working matrix with these values:

| Field | Required decision |
| --- | --- |
| Domain | Existing domain or new domain requiring `common.py` |
| Endpoint | Module path, HTTP method, URL, referer/docs/menu |
| Authentication | `configs`, cookies, token, credentials path, or external source |
| Request topology | Direct, `RequestEach`, `PaginateAll`, or composed task |
| Expansion axes | Account, space, mall, date, page, report type, or other key |
| Raw shape | Exact Extractor return annotation and runtime shape |
| Parser | Parser class, input shape, output shape, scope, and fields |
| Tables | Stable key, DuckDB name, physical `schema.table`, primary key |
| Load | Append, merge, overwrite, conflict behavior, partition column |
| Orchestration | Dag ID, schedule, date basis, retries, mapped credentials |
| dbt | BigQuery/Postgres source, model, selector, downstream consumers |
| Tests | Marker, fixture/config path, Extractor dump, Transformer harness |
| Release | Documentation surfaces, version, lockfile, commit scope |

Ask the user only when a missing value changes business meaning or an external side effect. Derive mechanical details from current code.

## 2. Layer matrix

Review each row. Add it when applicable or record why it is not applicable.

| Layer | Expected surface |
| --- | --- |
| Domain common | `src/linkmerce/core/<domain>/.../common.py` and package export when shared behavior is new |
| Extract | endpoint `extract.py` and package initialization |
| Transform | paired `transform.py` and `models.sql` |
| Public API | matching module under `src/linkmerce/api` |
| Extract test | domain class and marker in `src/tests/test_extract.py` and `pytest.ini` |
| Transform test | matching class in `src/tests/test_transform.py` |
| Test docs | `src/tests/README.md` and required local config/credential shape |
| Core docs | `src/README.md` and root documentation when the public surface changes |
| Runtime config | credentials, endpoint options, table map, merge map, schedule inputs |
| Postgres | schema/table DDL, keys, indexes/partitions, `postgres/README.md` |
| BigQuery schema | `postgres/resources/bq_schemas.json` physical schema entry |
| Airflow | Dag, credentials, ETL mapping, load calls, date-range propagation |
| dbt BigQuery | source, intermediate model, metadata, selector, downstream union |
| dbt Postgres | equivalent source/model/metadata/selector/downstream union |
| dbt docs | both project READMEs when model inventory changes |
| Release | `pyproject.toml`, `uv.lock`, root version text, commit message |

Do not create a common module merely to mirror a directory layout. Create it only for shared session/authentication/request behavior, and export the common class through the domain package `__init__.py` when endpoint modules import from the package.

## 3. Cross-layer invariants

### Raw response

- Match the Extractor return annotation to its real direct, expanded, paginated, and composed-task behavior.
- Match the Extractor `Returns` type line to the annotation.
- Match the public API `raw` return annotation and description to the Extractor.
- Explain every scalar-versus-list condition. Do not include transformed `DuckDBResult` shapes in an Extractor.

### Table map

For every table key, verify this chain:

```text
Transformer.tables
-> models.sql Jinja placeholder
-> API with_duckdb_connection table/tables
-> Dag sources key and internal DuckDB name
-> config tables/merge key and physical schema.table
-> Postgres DDL and BigQuery schema
-> dbt source
```

Require exact key spelling and stable order. Confirm SQL `{{ rows }}` is the parser input, not a table key.

### Fields and keys

- Trace every parser field used by SQL back to the raw response.
- Trace every transformed SQL column to both warehouse schemas.
- Keep nullability and types intentional; different warehouse types are allowed when explicitly chosen for storage or engine behavior.
- Match primary/conflict keys between DuckDB DDL, warehouse DDL, BigQuery clustering/partitioning where relevant, and Dag merge configuration.
- For a derived dimension table, define its update behavior explicitly. Do not assume `ON CONFLICT DO NOTHING` and warehouse MERGE mean the same thing.

### Runtime parameters

- Match every `$parameter` in `models.sql` to `Transformer.params`.
- Match each Transformer parameter to the Extractor parser context and test harness `map_index` or explicit transform argument.
- Pass API keyword options into the exact task option class used by the Extractor.
- Keep scalar and iterable inputs aligned from public API through Extractor expansion.

### Airflow and dbt

- Read the actual runtime config path used by `PATH`.
- Verify credentials cardinality before choosing direct or dynamic task mapping.
- Derive the extraction date from the same Airflow context convention as neighboring Dags.
- Load append tables before merge tables when the Dag's result and documentation use that order.
- Derive dbt date ranges from the actual partition column returned by the loaded fact table.
- Use the same selector name in Airflow and both dbt projects.
- Keep BigQuery and Postgres models semantically equivalent while retaining dialect-specific functions.
- Add new intermediate output to all intended downstream unions and metadata files in both projects.

## 4. Single-table and multi-table decisions

For a single table:

- Use `tables = {"table": "internal_name"}` in the Transformer.
- Use `@with_duckdb_connection(table="internal_name")` in the API.
- Return one `DuckDBResult` shape for transformed formats.
- Use one load call and one config table unless the existing workflow requires otherwise.

For multiple tables from one input:

- Use semantic table keys in one ordered Transformer `tables` mapping.
- Create all tables in one class's `create` SQL and populate them in ordered `bulk_insert` statements.
- Reuse the parsed input; do not re-run extraction to populate the derived table.
- Use `@with_duckdb_connection(tables={...})` and return `{table_key: result}` for transformed formats.
- Use the same keys in Dag `sources`, config `tables` and `merge`, load results, docstrings, and tests.

## 5. Validation commands

Run the repository audit first:

```powershell
conda run -n main python .codex/skills/linkmerce-add-etl/scripts/audit_etl_integration.py --staged
```

Run focused tests with the exact new marker:

```powershell
conda run -n main python -m pytest src/tests/test_transform.py -m <marker> -p no:cacheprovider -q
conda run -n main python -m pytest src/tests/test_extract.py -m <marker> -p no:cacheprovider -q
```

Run Extractor tests only when required secrets and network authorization are available. Never copy credentials into tracked files or command output.

Parse both dbt projects when touched:

```powershell
conda run -n dbt dbt parse --project-dir dbt_bigquery --no-partial-parse
conda run -n dbt dbt parse --project-dir dbt_postgres --no-partial-parse
```

Check the intended Git surface:

```powershell
git diff --check
git diff --cached --check
git status --short
```

Compile changed Python modules or import them where platform dependencies permit. If Airflow cannot import on the local OS, distinguish platform failure from Dag syntax and test syntax separately.

## 6. Completion gate

Before calling the work complete, verify all of the following:

- No relevant audit or focused test failure remains.
- Every user-specified term and identifier is preserved.
- No existing user edit was reverted.
- Extractor annotations, Returns docs, parser input, API `raw`, and tests agree.
- Table keys, names, columns, keys, load modes, sources, and selectors agree.
- Dag docs describe the current implementation, not the initial request or a copied Dag.
- Both dbt projects parse and remain semantically paired.
- Version strings agree everywhere they changed.
- The proposed commit message describes the final diff and explicitly states the version bump.
