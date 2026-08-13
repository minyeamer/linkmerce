---
name: "linkmerce-docstring-dag-style"
description: "Use when editing docstrings, Dag doc_md, or adjacent narrative docs in LinkMerce. Follow the repository's approved wording, layer boundaries, and section order from .github/instructions/lm.instructions.md."
---

# LinkMerce Docstring and Dag Style

Use this skill when a task involves:

- Docstrings in `src/linkmerce/core/**/*.py`
- Docstrings in `src/linkmerce/api/**/*.py`
- Dag `doc_md` in `airflow/dags/**/*.py`

## Workflow

1. Read the complete current file, current diff, and nearby approved wording first.
2. Read `.github/instructions/lm.instructions.md` and `../linkmerce-add-etl/references/style-contract.md` completely before editing.
3. Treat those contracts and the closest current approved implementation as the source of truth for format and phrasing.
4. Preserve the existing layer boundary:
   - `Extractor` only describes source collection or download
   - `Transformer` only describes parsing and table loading
   - `API` only describes the public function surface and returned shapes
   - `Dag` only describes orchestration, credentials, extraction, transform, and load strategy
5. Change only the placeholders that are code-specific.
6. Keep table names, parser names, return shapes, and load strategy aligned with the implementation.
7. If behavior changes, update tests or nearby documentation instead of rewriting the whole narrative.

For a new ETL integration that also changes core, tests, schemas, Airflow, or dbt, use `../linkmerce-add-etl/SKILL.md` and its required references before applying this narrower style skill.

## Style Rules

- Write docstrings and README prose in Korean. Write exception and error messages in English.
- Write SKILL, reference, and instruction prose in English. Use Korean only inside examples, templates, or exact repository-facing text that must be preserved or copied.
- Use `Dag` for an Airflow workflow in prose and `Dags` for the plural. Use `DAG` only for the Python class, its import, or constructor usage.
- Prefer the current approved template over inventing a new one.
- Keep summaries short and specific.
- Do not blur responsibilities across layers.
- Keep terminology consistent with the code and product UI.
- When multiple tables are involved, name the split explicitly.
- Treat punctuation as content. Do not add `.` or `이다.` to an existing noun phrase or fragment.
- Put only the class name in a Transformer `Extractor` section; keep input/output types in `Parser` or `Parsers`.

## When Not to Use

- Pure logic changes with no narrative update
- Broad refactors unrelated to docstrings or Dag docs
- New product areas that need a different style guide
