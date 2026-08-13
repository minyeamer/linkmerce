---
name: linkmerce-audit-api-contracts
description: Audit LinkMerce Extractor, Transformer, and public API type and table contracts without changing them. Use when checking return annotations, Returns docstrings, parser inputs, raw API returns, decorator table mappings, or type-hint problems under src/linkmerce/api.
---

# LinkMerce Audit API Contracts

Trace runtime data shapes from Extractor through Transformer to public API. Default to report-only; do not fix findings unless the user explicitly requests implementation.

## Read the contract

Read `references/contract-matrix.md` completely. Read each paired `extract.py`, `transform.py`, sibling `models.sql`, package export, and public API function in full before deciding a mismatch.

## Audit rules

1. Compare each `extract` or `extract_async` return annotation with its `Returns` docstring type.
2. Keep Extractor return annotations limited to actual extraction shapes; never include downstream DuckDB or Transformer return shapes.
3. Compare the Transformer parser input contract with the Extractor output, including list and multi-request aggregation shapes.
4. Compare public API `raw=True` annotations and `Returns` text with the Extractor runtime shape.
5. Compare public API transformed returns and decorator table mappings with Transformer `tables`.
6. Scan `src/linkmerce/api` for inconsistent annotations even when no paired file changed.
7. Preserve exact repository docstring wording and punctuation; an audit does not authorize stylistic rewrites.

Run a changed-surface audit:

```powershell
conda run -n main python .codex/skills/linkmerce-audit-api-contracts/scripts/audit_api_contracts.py --working-tree
```

Use `--staged`, or `--base-ref <commit> --target-ref <commit>`. Use `--all` only for an explicit repository-wide audit. Report legacy findings separately from changed-surface errors.

When fixes are explicitly requested, make the smallest contract correction and run focused pytest checks plus `git diff --check`.
