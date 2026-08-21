---
name: linkmerce-add-etl
description: Add or extend an external API through the complete LinkMerce ETL release path. Use when work spans any combination of a domain common Extractor, endpoint Extractor, DuckDB Transformer and models.sql, public API, pytest fixtures/tests, Postgres and BigQuery schemas, an Airflow Dag, dbt_bigquery and dbt_postgres models/selectors, documentation, version files, or a release commit message. Also use when reviewing whether those ETL layers are connected consistently.
---

# LinkMerce Add ETL

Apply one repository contract from source extraction through release. Preserve current user-authored wording and use deterministic checks before declaring completion.

## Read required references

Read these files completely before editing:

1. `references/workflow-contract.md` for layer scope and cross-layer invariants.
2. `references/style-contract.md` for Python formatting and exact docstring structures.
3. `references/golden-sources.md` for source and commit precedence.

Read `.github/instructions/lm.instructions.md` as the compact repository-wide docstring rule. If it conflicts with this skill, follow this skill and report the conflict instead of silently choosing wording.

## Establish the change contract

1. Inspect `git status --short`, staged diff, unstaged diff, and the complete current contents of every target file.
2. Record the domain path, endpoint path, Extractor class, Transformer class, parser, task pattern, DuckDB table map, physical table map, primary keys, load mode, Dag ID, dbt selector, test marker, and target version.
3. Resolve unclear business names, identifiers, primary keys, and load semantics from the user or authoritative source before coding.
4. Keep existing user changes. Never restore a historical version merely because it looks more familiar.

## Select the request topology

- Use a direct request for one response.
- Use `RequestLoop` to retry one request until a condition succeeds.
- Use `RequestEach` for one request over each value or generated context.
- Use `PaginateAll` for every page of one logical query.
- Use `CursorAll` to follow cursors until no next cursor remains.
- Use the matching composed task already implemented in `linkmerce.common` when multiple dimensions apply: `RequestEachLoop`, `RequestEachPages`, or `RequestEachCursor`.
- Copy the chosen task's chaining order, option name, default options, and Attributes wording from the closest current implementation. Do not invent a new task wrapper when an existing one matches.

## Build request messages by layer

- `BaseSessionClient.build_request_message` composes `method`, `url`, `params`, `data`, `json`, and `headers`, omitting values that are `None`. Set shared `method` and `url` as class attributes unless an endpoint must override them per request.
- Put domain-wide fixed query parameters, body state, and header values in `set_request_params`, `set_request_body`, and `set_request_headers` on the domain common Extractor. `set_request_headers` also owns shared header construction, cookie propagation, and `from_cookies` mappings.
- Put endpoint-specific or call-specific query parameters in `build_request_params`; return form, text, or binary payloads from `build_request_data`; return JSON payloads from `build_request_json`; and add variable headers in `build_request_headers`.
- `build_request_data` and `build_request_json` do not automatically consume the body retained by `set_request_body`. Override the appropriate builder when that value must be sent, and send a request body through either `data` or `json` according to the endpoint contract.
- Execute requests through `build_request_message`; do not manually assemble duplicate `params`, `data`, `json`, or `headers` arguments in a shared request helper.
- Keep a shared request helper limited to transport behavior such as status validation and raw-content return. Do not let it encode endpoint payloads, query parameters, or endpoint-specific headers.

## Implement in dependency order

1. Add or extend the domain common module only when the domain does not already provide the shared session, authentication, URL, headers, or request behavior.
2. Implement `core/.../<feature>/extract.py` and verify its real raw return shape.
3. Implement `transform.py` and `models.sql`; decide single-table or multi-table behavior before writing either file.
4. Before exposing the Extractor-Transformer pair through `src/linkmerce/api`, read `../linkmerce-audit-api-contracts/SKILL.md`; keep decorator tables, Transformer tables, SQL placeholders, return annotations, and Returns text identical in meaning.
5. Before adding Extractor and Transformer pytest coverage, read `../linkmerce-sync-test-fixtures/SKILL.md`; use the existing harness and domain marker.
6. Before adding physical Postgres and BigQuery schemas, read `../linkmerce-sync-warehouse-schemas/SKILL.md`; synchronize configuration table and merge mappings, dbt sources, and documentation entries.
7. Add the Airflow Dag with real credentials, schedule, extraction date, load modes, partition results, dbt date range, and task dependency order.
8. Before changing dbt surfaces, read `../linkmerce-sync-dbt-models/SKILL.md` and its required references. Add equivalent `dbt_bigquery` and `dbt_postgres` sources, models, metadata, selectors, downstream unions, and README inventory entries.
9. Update repository documentation and bump both `pyproject.toml` and `uv.lock` when releasing.

Do not skip a layer silently. Mark it not applicable with evidence in the final review when the requested API genuinely does not need it.

## Protect wording and formatting

- Write docstrings and README prose in Korean. Write exception and error messages in English.
- Write SKILL, reference, and instruction prose in English. Use Korean only inside examples, templates, or exact repository-facing text that must be preserved or copied.
- Copy a matching approved structure, then replace only feature-specific placeholders.
- Treat Korean terminology, capitalization, spacing, code spans, list indentation, and punctuation as exact data.
- Do not append `이다.`, replace a noun phrase with a sentence, or add `.` to a label-like description.
- Put only the Extractor class name in a Transformer `Extractor` section. Put input/output types only in `Parser` or `Parsers`.
- Apply the multiline `=` rule from `references/style-contract.md` only to multiline function calls and `dict(...)` constructors. Keep keyword arguments in compact one-line calls in standard Python form without spaces around `=`.

## Validate before handoff

Run the audit harness against the intended diff:

```powershell
conda run -n main python .codex/skills/linkmerce-add-etl/scripts/audit_etl_integration.py --staged
```

Before staging, use `--working-tree`. To examine a completed reference commit, use `--base-ref <commit>~1 --target-ref <commit>`.

Then run the applicable runtime checks from `references/workflow-contract.md`. At minimum:

1. Run focused Extractor tests when credentials and network access are available.
2. Run focused Transformer tests unconditionally when fixtures exist.
3. Parse both dbt projects through `conda run -n dbt dbt ...` when either project changed.
4. Run `git diff --check` for the exact review surface.
5. Re-read the full final files and compare their table maps, parameter names, return shapes, load modes, and docs.

Do not hide failures from unrelated tests. Separate them from relevant failures with evidence.

## Compose the release commit message

Apply `../linkmerce-review-staged-release/SKILL.md` after the intended changes are staged. Inspect recent `git log` and the reference commits before drafting. Use a Conventional Commit title followed by English list items. For a versioned API release, include the release version in the title and an explicit final version-bump bullet. Do not provide a success-style commit message while relevant validation is failing.
