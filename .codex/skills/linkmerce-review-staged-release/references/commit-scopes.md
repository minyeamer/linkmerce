# LinkMerce Commit Scopes

Use only the scopes in this file. Select the scope with a short path-based comparison and treat the result as a recommendation that the user may adjust.

| Scope | Selection rule |
| --- | --- |
| `core` | Use when broad changes include `src` or span LinkMerce source layers. |
| `api` | Use only when every changed file is under `src/linkmerce/api`. |
| `extract` | Use only when changes are limited to `extract.py` under `src/linkmerce/common` or `src/linkmerce/core`. |
| `transform` | Use only when changes are limited to `transform.py` or `models.sql` under `src/linkmerce/common` or `src/linkmerce/core`. |
| `extensions` | Use only when every changed file is under `src/linkmerce/extensions`. |
| `test` | Use only when every changed file is under `src/tests`. |
| `airflow` | Use when most relevant changes are under `airflow/dags`. |
| `fastapi` | Use only when every changed file is under `airflow_trigger/fastapi`. |
| `streamlit` | Use only when every changed file is under `airflow_trigger/streamlit`. |
| `dbt` | Use when most relevant changes are under `dbt_bigquery` or `dbt_postgres`. |
| `postgres` | Use when most relevant changes are under `postgres`. |
| `skills` | Use when changes affect `.agents/skills`, `.codex/skills`, or `.github/instructions`. |

When multiple scopes apply, prefer the scope with more changed files. Apply this simple comparison especially to overlapping `airflow` and `dbt` work. Do not spend substantial analysis or tokens resolving a close result; provide one recommendation and let the user revise it.
