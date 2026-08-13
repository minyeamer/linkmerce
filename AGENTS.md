# LinkMerce Agent Rules

## Mandatory ETL workflow

- When adding or extending an external ETL across `src/linkmerce`, tests, database schemas, Airflow, or dbt, use `.codex/skills/linkmerce-add-etl/SKILL.md` before editing.
- Read every reference marked as required by that skill. Do not reconstruct its templates from memory.
- Treat the user's current edits and the current full file contents as authoritative. Historical commits are examples of scope, not permission to restore older wording or code.

## Preserve local intent

- Read the complete contents of every file before changing it and inspect both staged and unstaged diffs.
- Change only the requested behavior. Do not rephrase an existing docstring, comment, label, table description, or product term unless the request explicitly requires it.
- Treat punctuation as content. Do not add or remove `.` merely for visual consistency.
- Copy service names and domain terms from the current configuration, documentation, or user wording exactly.

## Language

- Write docstrings and README prose in Korean.
- Write exception and error messages in English.
- Write SKILL, reference, and instruction prose in English.
- In English harness documents, use Korean only inside examples, templates, or exact repository-facing text that must be preserved or copied.

## Python formatting

- In multiline function definitions, calls, and `dict(...)` constructors, put one space on both sides of `=`.
- Indent multiline parameters by two levels relative to `def`; align the closing parenthesis one level relative to `def`.
- Preserve the local single-line style. Do not expand unrelated code solely to normalize formatting.

## Airflow terminology

- In prose, headings, comments, labels, and user-facing text, write `Dag` for an Airflow workflow and `Dags` for the plural.
- Use `DAG` only for the Python class itself, including `from airflow ... import DAG`, `DAG(...)`, and an explicit reference to the `DAG` class.
- Preserve lowercase code identifiers such as `dag`, `dag_id`, filenames, CLI arguments, and module paths.
- Do not normalize `Dag` to `DAG` merely because it originated as an acronym.

## Validation gate

- Run `.codex/skills/linkmerce-add-etl/scripts/audit_etl_integration.py` for a new ETL integration.
- Validate the affected Extractor and Transformer tests, Python syntax, both dbt projects when touched, and `git diff --check`.
- Do not state that the change is complete and do not propose a release commit message while a relevant validation error remains.
