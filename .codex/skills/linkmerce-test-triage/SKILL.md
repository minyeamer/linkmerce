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

1. Reproduce the failure or inspect the test target first.
2. Read the smallest relevant implementation path and nearby tests.
3. Identify whether the issue is in the test, the implementation, or the boundary between them.
4. Make the smallest change that resolves the failure.
5. Re-run or describe the validation step that best matches the change.

## Style Rules

- Prefer direct, local fixes over broad refactors.
- Keep assertions focused on behavior, not incidental details.
- Add a regression test when the bug is real and reproducible.
- Preserve existing test naming and layout unless there is a clear reason to change it.

## When Not to Use

- Pure documentation work
- Large refactors unrelated to validation
- Work that does not involve tests or bug reproduction
