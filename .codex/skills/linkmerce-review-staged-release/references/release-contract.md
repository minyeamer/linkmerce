# Staged Release Contract

## Review boundary

- The Git index is authoritative for scope.
- Read working-tree versions of staged files to detect partial staging, but distinguish unstaged content from the proposed commit.
- Never change the index or repository during a review-only request.
- Treat untracked and unstaged files as context, not commit contents.

## Cross-layer checks

Trace an ETL name through core exports, Extractor, Transformer, SQL, public API, tests, warehouse schemas, load configuration, Airflow Dag, both dbt projects, selectors, and READMEs. Require exact table names and compatible return contracts. Preserve `Dag` in prose and `DAG` only for the Python class, import, or constructor.

## Version and message rules

- When version files are staged, require `pyproject.toml` and `uv.lock` to agree.
- Infer wording from recent repository history and select only a scope defined in `commit-scopes.md`.
- Require an English scoped Conventional Commit title in the form `<type>(<scope>): <summary>`.
- When staged changes span multiple scopes, prefer the scope with more changed files. In particular, compare `airflow` and `dbt` this way when they overlap.
- Keep scope selection lightweight. Report one recommendation and allow the user to adjust it without extended analysis.
- Use only the title for a simple change with one material effect.
- Use a title, one blank line, and `- ` list items for multiple material effects, multiple layers, or a release summary.
- Start each list item with an uppercase letter and end it with a period.
- Wrap class names, function names, variable names, selectors, table names, Dag IDs, commands, and other code identifiers in backticks.
- Keep ordinary product names and prose unquoted unless they are literal identifiers.
- A release title names the version and its bullets explicitly mention the version bump.
- Do not include unstaged work in the message.

For example, use `chore(skills)` for a staged bundle that adds several LinkMerce SKILLs, audit scripts, and their repository instructions. Mention an accompanying Airflow change in a bullet when it is secondary to that bundle.

## Validation levels

Classify results as blocking errors, warnings requiring user judgment, or passed checks. Missing credentials, unavailable warehouses, or external network access limits must be reported as unexecuted validation, not as success.
