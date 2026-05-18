---
name: "linkmerce-docstring-dag-style"
description: "Use when editing docstrings, DAG doc_md, or adjacent narrative docs in LinkMerce. Follow the repository's approved wording, layer boundaries, and section order from .github/instructions/lm.instructions.md."
---

# LinkMerce Docstring and DAG Style

Use this skill when a task involves:

- Docstrings in `src/linkmerce/core/**/*.py`
- Docstrings in `src/linkmerce/api/**/*.py`
- DAG `doc_md` in `airflow/dags/**/*.py`

## Workflow

1. Read the current code and nearby approved wording first.
2. Treat `.github/instructions/lm.instructions.md` as the source of truth for format and phrasing.
3. Preserve the existing layer boundary:
   - `Extractor` only describes source collection or download
   - `Transformer` only describes parsing and table loading
   - `API` only describes the public function surface and returned shapes
   - `DAG` only describes orchestration, credentials, extraction, transform, and load strategy
4. Change only the placeholders that are code-specific.
5. Keep table names, parser names, return shapes, and load strategy aligned with the implementation.
6. If behavior changes, update tests or nearby documentation instead of rewriting the whole narrative.

## Style Rules

- Prefer the current approved template over inventing a new one.
- Keep summaries short and specific.
- Do not blur responsibilities across layers.
- Keep terminology consistent with the code and product UI.
- When multiple tables are involved, name the split explicitly.

## When Not to Use

- Pure logic changes with no narrative update
- Broad refactors unrelated to docstrings or DAG docs
- New product areas that need a different style guide
