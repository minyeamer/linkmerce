---
name: "linkmerce-airflow-change"
description: "Use when editing Airflow DAGs, scheduling, task orchestration, or load behavior in LinkMerce. Keep orchestration, credentials, extract/transform steps, and load strategy aligned with the repository's approved DAG patterns."
---

# LinkMerce Airflow Change

Use this skill when a task involves:

- Editing `airflow/dags/**/*.py`
- Changing DAG schedules, dependencies, or triggers
- Updating doc_md, credential handling, or load strategy

## Workflow

1. Read the DAG and the called API or task modules together.
2. Confirm the real trigger, credentials, extract path, transform path, and load behavior.
3. Keep the DAG focused on orchestration instead of business logic.
4. Update doc_md so it matches the implementation, not the other way around.
5. Add or update tests only when the DAG behavior is testable in the repo.

## Style Rules

- Keep schedules, retries, and task order explicit.
- Do not mix extraction, transformation, and loading responsibilities in one description.
- Preserve the approved wording patterns from `.github/instructions/lm.instructions.md`.
- Prefer a minimal DAG change over a redesign unless the user asks for one.

## When Not to Use

- Pure Python library changes outside Airflow
- Docstring-only edits that do not touch DAGs
- Non-orchestration code under `src/linkmerce`
