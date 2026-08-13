# Warehouse Schema Contract

## Contract surfaces

Inspect these surfaces together:

- `src/linkmerce/core/**/transform.py` and sibling `models.sql`
- `src/env/config.yaml`
- `postgres/init.sql`
- `postgres/resources/bq_schemas.json`
- `dbt_bigquery/models/sources.yml`
- `dbt_postgres/models/sources.yml`
- the related Airflow Dag and load tests

## Comparison rules

- Compare physical tables by `schema.table`.
- Preserve output column names and order across Transformer SQL and both warehouse schemas.
- Compare nullability meaning: BigQuery `REQUIRED` corresponds to PostgreSQL `NOT NULL` when the load contract is identical.
- Compare PostgreSQL primary keys with configured merge keys and the logical uniqueness contract.
- Treat partitioning, clustering, and indexes as warehouse-specific implementations of access and retention intent.
- Treat `STRING` and text-like PostgreSQL types, `INTEGER` and integer-like types, `FLOAT` and floating or numeric types, `DATETIME` and timestamp-like types as type families, not literal matches.
- Flag precision, timezone, array, JSON, and date-versus-timestamp changes for manual review.
- Never shrink a PostgreSQL type solely to mirror BigQuery or vice versa.

## Change discipline

In audit-only mode, produce file and table references with the mismatched columns or metadata. In synchronize mode, make the smallest complete cross-surface change and preserve intentional local-storage choices. README and docstring prose remains Korean; errors and SKILL content remain English.
