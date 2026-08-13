---
name: linkmerce-review-staged-release
description: Review staged LinkMerce changes without mutating them, run checks selected by affected layers, verify version consistency, and draft a repository-style commit message. Use when the user asks to inspect git-added changes, confirm release readiness, or write a commit message from the staged diff and recent history.
---

# LinkMerce Review Staged Release

Review only the Git index and the full current contents of staged files. Do not stage, unstage, edit, commit, amend, or push unless the user explicitly requests that separate action.

## Read the contract

Read `references/release-contract.md` and `references/commit-scopes.md` completely before reviewing. Read the applicable repository SKILL for every affected layer.

## Inspect staged scope

1. Run `git diff --cached --name-status`, `git diff --cached --check`, and `git diff --cached`.
2. Read every staged file completely, including surrounding code not visible in the diff.
3. Read recent `git log` subjects to infer the repository's commit wording.
4. Compare `pyproject.toml` and `uv.lock` versions when either is staged.
5. Trace cross-layer names, paths, tables, selectors, schedule, credentials, and return contracts.

Use the read-only summary helper:

```powershell
conda run -n main python .codex/skills/linkmerce-review-staged-release/scripts/summarize_staged_release.py
```

## Run scoped checks

- Apply `linkmerce-add-etl` when ETL release surfaces are staged.
- Apply `linkmerce-audit-api-contracts` for core Extractor, Transformer, or public API changes.
- Apply `linkmerce-sync-warehouse-schemas` for DDL, BigQuery schema, load config, or dbt source changes.
- Apply `linkmerce-sync-dbt-models` for dbt model, metadata, selector, README, or TaskGroup changes.
- Apply `linkmerce-sync-test-fixtures` for ETL test, fixture, marker, or test README changes.

Default every linked audit SKILL to audit-only. Do not repair findings unless the user asks.

## Report or draft

Report blocking issues first with clickable file links. If no issue remains, draft one commit message in a copyable code block:

- Use the scoped Conventional Commit form `<type>(<scope>): <summary>`; a scope is mandatory in this multi-tool repository.
- Select the narrowest canonical repository part from `references/release-contract.md` instead of inventing a scope from the current filename.
- Use one title line when the staged change has one simple material effect.
- Use a title, blank line, and English list when the staged change has multiple material effects or spans multiple surfaces.
- Start every list item with an uppercase letter and end it with `.`.
- Wrap class names, function names, variable names, selectors, table names, Dag IDs, and other code identifiers in backticks.
- For a release, include the version in the title and an explicit version-bump bullet.

Do not claim readiness or provide a final commit message as successful when a blocking check failed.
