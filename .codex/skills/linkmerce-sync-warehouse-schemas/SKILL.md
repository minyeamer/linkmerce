---
name: linkmerce-sync-warehouse-schemas
description: Audit or explicitly synchronize LinkMerce warehouse table contracts across Transformer outputs, Postgres DDL, BigQuery schema JSON, load configuration, and dbt sources. Use when adding or changing ETL tables, columns, keys, load targets, or warehouse source declarations.
---

# LinkMerce Sync Warehouse Schemas

Keep every physical load target connected to the same ordered table contract. Default to audit-only and never rewrite a warehouse type merely because BigQuery and PostgreSQL use different physical representations.

## Read the contract

Read `references/schema-contract.md` completely. Also read the relevant Transformer, `models.sql`, load configuration, DDL, BigQuery schema entry, dbt source declarations, and Airflow Dag before reporting or editing.

## Select a mode

- `audit-only` is the default. Report mismatches without editing source files.
- `synchronize` requires an explicit request to implement or fix schema integration.

An audit request and a SKILL maintenance request do not authorize product-source changes.

## Audit in dependency order

1. Identify Transformer table names and ordered output columns.
2. Resolve logical table keys through `src/env/config.yaml`.
3. Compare Postgres DDL columns and primary keys with `postgres/resources/bq_schemas.json` fields and required modes.
4. Confirm both dbt projects declare every referenced physical source.
5. Confirm Airflow passes the same table mapping to BigQuery and Postgres load tasks.
6. Classify dialect-specific types as compatible, incompatible, or requiring manual review.

Run:

```powershell
conda run -n main python .codex/skills/linkmerce-sync-warehouse-schemas/scripts/audit_warehouse_schemas.py --working-tree
```

Use `--staged`, or `--base-ref <commit> --target-ref <commit>`, for another Git surface. Use `--all` for an explicit repository-wide inventory.

## Synchronize only when authorized

Preserve table names, column names, order, nullability meaning, primary or merge keys, partition and cluster intent, and existing warehouse-specific type choices. Update all affected load configuration, source declarations, tests, and Korean README documentation. Do not normalize unrelated schemas.

Finish with the audit, scoped tests, and `git diff --check`.
