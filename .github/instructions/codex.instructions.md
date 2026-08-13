---
name: "Codex Working Rules"
description: "Use when Codex writes or revises code in this repository. Keep changes narrow, preserve existing layer boundaries, and follow the comment/docstring style below."
applyTo: src/**/*.py, airflow/**/*.py, dbt_bigquery/**/*.sql, dbt_postgres/**/*.sql, postgres/**/*.sql
---

# Codex Working Rules

## Read first

- Read the nearest implementation, tests, and existing docs before editing.
- Prefer the smallest relevant scope: one feature, one bug, or one document set at a time.
- If a file already has a local pattern, follow that pattern instead of inventing a new one.

## Comment style

- Add comments only when the code would otherwise be hard to understand.
- Explain `why`, not `what`.
- Keep comments short and specific.
- Avoid commenting obvious assignments, simple loops, or self-evident glue code.
- If logic is subtle, place one brief comment above the block instead of many inline comments.

## Docstring style

- Write docstrings and README prose in Korean.
- Public classes and functions should have concise docstrings when they are non-trivial.
- Describe the contract: purpose, inputs, outputs, side effects, and notable exceptions.
- Keep wording aligned with the surrounding module and existing approved templates.
- Do not rewrite a docstring just to make it prettier if the current wording is already correct.

## Error message style

- Write exception and error messages in English.

## Change style

- Make the smallest change that solves the request.
- Add or update tests when behavior changes.
- Keep refactors separate from bug fixes unless the user explicitly wants both.
- When a task is ambiguous, inspect the current code and nearby tests before deciding.

## Python layout

- In multiline function definitions, calls, and `dict(...)` constructors, put one space on both sides of `=`.
- Indent multiline function parameters by two levels relative to `def`, and align the closing parenthesis one level relative to `def`.
- Preserve compact single-line calls and mappings when they match the local file.
- Treat formatting as part of the user's changes. Do not reformat unrelated code.

## Repository rules

- Respect the layer boundaries already established in `src/linkmerce`, `airflow/dags`, `dbt_bigquery`, and `dbt_postgres`.
- Preserve the existing docstring and Dag wording patterns from `.github/instructions/lm.instructions.md`.
- In prose use `Dag` or `Dags`; reserve `DAG` for the Python class and its import or constructor usage.
- Use `.codex/skills/linkmerce-add-etl/SKILL.md` for an ETL introduction spanning core, tests, schemas, Airflow, or dbt.
- When a change touches a workflow that is reused often, prefer a reusable skill or helper over repeating instructions in chat.
