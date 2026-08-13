---
name: linkmerce-sync-test-fixtures
description: Audit or explicitly synchronize LinkMerce ETL tests, fixtures, pytest markers, result paths, and test documentation. Use when adding or changing Extractors, Transformers, public APIs, credential fixtures, test markers, or src/tests README inventories.
---

# LinkMerce Sync Test Fixtures

Keep Extractor and Transformer coverage connected to the repository's shared harness. Default to audit-only and distinguish credentialed network tests from deterministic offline tests.

## Read the contract

Read `references/test-contract.md`, `src/tests/conftest.py`, `src/tests/pytest.ini`, `src/tests/fixtures.yaml`, `src/tests/test_extract.py`, `src/tests/test_transform.py`, and the relevant section of `src/tests/README.md` before editing.

## Audit

1. Map changed or new Extractor and Transformer classes to their test imports and methods.
2. Confirm the domain marker is registered in `src/tests/pytest.ini` and applied consistently.
3. Confirm fixture keys, credential paths, and result paths follow nearby tests.
4. Confirm Transformer tests consume representative saved extraction data through `transformer_harness`.
5. Confirm test README inventories and setup instructions reflect new domains or paths.
6. Report credentialed Extractor tests as skipped or unexecuted when authorization is unavailable; do not treat them as passed.

Run:

```powershell
conda run -n main python .codex/skills/linkmerce-sync-test-fixtures/scripts/audit_test_fixtures.py --working-tree
```

Use `--staged`, or `--base-ref <commit> --target-ref <commit>`. Use `--all` for a repository-wide inventory; legacy gaps are warnings unless the user asks to enforce them.

## Synchronize only when authorized

When the user requests implementation, add the smallest matching tests, fixture entries, marker registration, and Korean README update. Never embed live credentials or cookies in tracked files. Run offline Transformer tests first, then credentialed Extractor tests only with explicit available authorization. Finish with `git diff --check`.
