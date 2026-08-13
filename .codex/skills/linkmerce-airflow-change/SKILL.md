---
name: "linkmerce-airflow-change"
description: "Use when editing Airflow Dags, scheduling, task orchestration, or load behavior in LinkMerce. Keep orchestration, credentials, extract/transform steps, and load strategy aligned with the repository's approved Dag patterns."
---

# LinkMerce Airflow Change

Use this skill when a task involves:

- Editing `airflow/dags/**/*.py`
- Changing Dag schedules, dependencies, or triggers
- Updating doc_md, credential handling, or load strategy

## Workflow

1. Read `.github/instructions/lm.instructions.md` and `../linkmerce-add-etl/references/style-contract.md` before changing Dag documentation or layout.
2. Read the complete Dag and the called API or task modules together.
3. Confirm the real trigger, credentials, extract path, transform path, and BigQuery/Postgres load behavior.
4. Keep the Dag focused on orchestration instead of business logic.
5. Update doc_md so it matches the implementation, not the other way around.
6. Add or update tests only when the Dag behavior is testable in the repo.

For a new ETL integration that also changes core, tests, schemas, or dbt, use `../linkmerce-add-etl/SKILL.md` and its required references before applying this narrower Airflow skill.

## Style Rules

- Write Dag `doc_md` in Korean. Write exception and error messages in English.
- Use `Dag` for an Airflow workflow in prose and `Dags` for the plural. Use `DAG` only for the Python class, its import, or constructor usage.
- Keep schedules, retries, and task order explicit.
- Do not mix extraction, transformation, and loading responsibilities in one description.
- Preserve the approved wording patterns from `.github/instructions/lm.instructions.md`.
- Prefer a minimal Dag change over a redesign unless the user asks for one.
- Apply `../linkmerce-sync-dbt-models/SKILL.md` when dbt TaskGroups, selectors, or paired warehouse execution changes.
- Apply `../linkmerce-sync-warehouse-schemas/SKILL.md` when load targets, table mappings, or warehouse contracts change.

## When Not to Use

- Pure Python library changes outside Airflow
- Docstring-only edits that do not touch Dags
- Non-orchestration code under `src/linkmerce`
