---
name: linkmerce-sync-dbt-models
description: Audit, create, migrate, or synchronize paired dbt models between the LinkMerce dbt_bigquery and dbt_postgres projects. Use when checking model parity, adding or moving dbt models, translating BigQuery SQL to PostgreSQL, aligning sources/models/selectors, updating Airflow dbt TaskGroups, or documenting dbt model paths in both project READMEs.
---

# LinkMerce Sync dbt Models

Keep `dbt_bigquery` authoritative while preserving the same result contract in `dbt_postgres`. Separate inspection authority from implementation authority.

## Select an operating mode

Use `audit-only` unless the user explicitly requests model implementation.

- `audit-only`: inspect and report mismatches. Do not edit dbt models, metadata, READMEs, or Airflow files.
- `create-counterpart`: create a missing PostgreSQL counterpart only when the user explicitly requests implementation or migration.
- `synchronize-existing`: update an existing pair only when the user explicitly requests synchronization or a fix.

A request to create, update, or review this SKILL authorizes changes only inside this SKILL. Audit findings never authorize product-source changes.

## Read required references

Read these files completely before acting:

1. `references/project-contract.md`
2. `references/dialect-contract.md`
3. `references/golden-sources.md`

When implementation is part of a complete ETL integration, also follow `../linkmerce-add-etl/SKILL.md`. When an Airflow Dag changes, also follow `../linkmerce-airflow-change/SKILL.md` and `../linkmerce-docstring-dag-style/SKILL.md`.

## Establish the contract

1. Read the complete BigQuery model, dependencies, metadata, selectors, README entries, and calling Airflow Dag.
2. Read the PostgreSQL counterpart and the closest approved paired implementation with the same materialization.
3. Record path, model name, schema, alias, materialization, parameters, date range, sources, refs, output columns, types, order, and downstream selectors.
4. Treat a missing counterpart as a finding unless implementation was explicitly requested.
5. Preserve user-authored SQL, Korean documentation, ordering, naming, and unrelated changes.

## Implement only in an authorized mode

1. Keep the BigQuery model as the behavioral source of truth.
2. Translate PostgreSQL SQL with the same output contract using `references/dialect-contract.md`.
3. Synchronize `models/sources.yml`, `models/models.yml`, and selectors.
4. Update both project READMEs for every added, moved, renamed, or deleted model.
5. Update both dbt TaskGroups when an Airflow Dag triggers a dual-warehouse selector.

README updates are part of a model change. A stale path inventory is incomplete.

## Validate

Run the audit first:

```powershell
conda run -n main python .codex/skills/linkmerce-sync-dbt-models/scripts/audit_dbt_model_pairs.py --working-tree
```

Use `--staged` for the index or `--base-ref <commit> --target-ref <commit>` for a historical range.

Run dbt only through the dedicated environment:

```powershell
conda run -n dbt dbt parse --project-dir dbt_bigquery --no-partial-parse
conda run -n dbt dbt parse --project-dir dbt_postgres --no-partial-parse
conda run -n dbt dbt compile --project-dir dbt_bigquery --selector <selector>
conda run -n dbt dbt compile --project-dir dbt_postgres --selector <selector>
```

Never use `conda run -n main dbt ...` or an environment-ambiguous bare `dbt ...`. Run `dbt run` and `dbt test` only against an authorized target. Finish with `git diff --check` and re-read every changed file.
