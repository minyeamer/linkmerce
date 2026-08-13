# Golden Sources and Precedence

## Source precedence

Use this order whenever examples disagree:

1. The user's latest explicit wording and decisions
2. The complete current contents and runtime behavior of the files being edited
3. Commit `3a8a354f` for the end-to-end BrandConnect introduction process
4. A current neighboring implementation with the same request topology and table cardinality
5. Historical commits `0f1db781`, `cee60fb2`, and `f70efe5e` for scope examples only
6. General Python, Airflow, dbt, or SQL convention

Never overwrite a higher-priority source with a lower-priority pattern. Historical commits can contain obsolete APIs, later-fixed defects, or formatting that the user subsequently changed.

## End-to-end reference

Use commit `3a8a354f` to identify the complete release surface:

- New domain common Extractor and package export
- Endpoint Extractor, Transformer, and `models.sql`
- Public API
- Extractor and Transformer tests plus marker/docs
- Postgres and BigQuery physical schemas
- Airflow Dag with external cookie retrieval
- BigQuery and Postgres dbt sources, models, selectors, and downstream consumers
- Documentation, package version, and lockfile

Read the current versions of these files before copying any wording:

- `src/linkmerce/core/naver/brandconnect/common.py`
- `src/linkmerce/core/naver/brandconnect/sales/extract.py`
- `src/linkmerce/core/naver/brandconnect/sales/transform.py`
- `src/linkmerce/core/naver/brandconnect/sales/models.sql`
- `src/linkmerce/api/naver/brandconnect.py`
- `airflow/dags/naver/connect_sales.py`

## Request topology references

Use current files, not historical snapshots:

- `RequestEach`: `src/linkmerce/core/naver/brandconnect/sales/extract.py`
- `RequestEach` with retry loop: `src/linkmerce/core/smartstore/hcenter/sales/extract.py`
- `PaginateAll`: `src/linkmerce/core/sabangnet/admin/account/extract.py`
- Date expansion plus pagination: `src/linkmerce/core/smartstore/api/settlement/extract.py`
- Multiple DuckDB tables from one parsed input: `src/linkmerce/core/naver/brandconnect/sales/transform.py` and `models.sql`
- Single-table Transformer: choose the nearest current module with the same parser and runtime parameters

Read the task implementation in `src/linkmerce/common` when chaining or output cardinality is unclear. Do not rely on a class name alone.

## Historical scope references

- `0f1db781`: new Dable domain, API, tests, Airflow, schemas, and paired dbt projects
- `cee60fb2`: endpoint addition within an existing Smartstore domain
- `f70efe5e`: endpoint family and public API addition without the later full Airflow/dbt release surface

Use these commits to decide which files may participate, not to copy old docstrings or stale calls.

## Commit message pattern

Inspect `git log` immediately before drafting. A full versioned API introduction follows this shape:

```text
feat(<primary-scope>): release v<version> with <domain feature> API support

- Add <domain feature> extract/transform support.
- Add <dag_id> Dag with <authentication or orchestration summary>.
- Add BigQuery and Postgres schemas, dbt models, and selector.
- Update <other material surfaces>.
- Bump package version to <version>.
```

Keep only bullets supported by the final diff. Use English imperative-style release bullets, capitalize the first word, and end each bullet with `.`. Select the scope from the primary change and recent repository history; do not reuse a scope mechanically.
