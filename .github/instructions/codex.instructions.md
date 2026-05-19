---
name: "Codex Working Rules"
description: "Use when Codex writes or revises code in this repository. Keep changes narrow, preserve existing layer boundaries, and follow the comment/docstring style below."
applyTo: src/**/*.py, airflow/**/*.py, dbt/**/*.sql, postgres/**/*.sql, streamlit/**/*.py
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

- Public classes and functions should have concise docstrings when they are non-trivial.
- Describe the contract: purpose, inputs, outputs, side effects, and notable exceptions.
- Keep wording aligned with the surrounding module and existing approved templates.
- Do not rewrite a docstring just to make it prettier if the current wording is already correct.

## Change style

- Make the smallest change that solves the request.
- Add or update tests when behavior changes.
- Keep refactors separate from bug fixes unless the user explicitly wants both.
- When a task is ambiguous, inspect the current code and nearby tests before deciding.

## Repository rules

- Respect the layer boundaries already established in `src/linkmerce`, `airflow/dags`, and `dbt`.
- Preserve the existing docstring and DAG wording patterns from `.github/instructions/lm.instructions.md`.
- When a change touches a workflow that is reused often, prefer a reusable skill or helper over repeating instructions in chat.
