---
name: "linkmerce-test-triage"
description: "Use when a test fails, coverage needs adjustment, or a code change must be validated in LinkMerce. Focus on reproducing the failure, finding the minimal fix, and updating tests with the smallest safe change."
---

# LinkMerce Test Triage

Use this skill when a task involves:

- Failing tests in `src/tests`
- Adding or updating tests for a code change
- Reproducing a bug before editing implementation code

## Workflow

1. Read `src/tests/pytest.ini`, the target test, shared fixtures, and the smallest relevant implementation path.
2. Reproduce the failure with the narrowest marker, class, or test node.
3. Identify whether the issue is in the test, implementation, fixture, credential boundary, or external service.
4. Make the smallest change that resolves the failure when the user requested a fix; otherwise report the cause without editing.
5. Re-run the narrow target before widening validation.

Apply `../linkmerce-sync-test-fixtures/SKILL.md` when the task adds or synchronizes ETL test coverage, fixture keys, markers, result paths, or test README entries. Keep this SKILL focused on failure reproduction and diagnosis.

## Style Rules

- Prefer direct, local fixes over broad refactors.
- Keep assertions focused on behavior, not incidental details.
- Add a regression test when the bug is real and reproducible.
- Preserve existing test naming and layout unless there is a clear reason to change it.
- Treat missing credentials or unavailable external services as unexecuted validation, not a passing test.
- Write test docstrings and README prose in Korean and exception or error messages in English.

## When Not to Use

- Pure documentation work
- Large refactors unrelated to validation
- Work that does not involve tests or bug reproduction
