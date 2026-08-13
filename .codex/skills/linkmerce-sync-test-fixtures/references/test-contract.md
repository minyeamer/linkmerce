# ETL Test Contract

## Shared surfaces

- `src/tests/test_extract.py`
- `src/tests/test_transform.py`
- `src/tests/conftest.py`
- `src/tests/fixtures.yaml`
- `src/tests/pytest.ini`
- `src/tests/README.md`
- `src/tests/results/`

## Coverage rules

- A newly exposed Extractor requires a focused extract test unless the user explicitly excludes live testing.
- A new Transformer requires an offline transform test using `transformer_harness` and representative extraction data.
- Reuse the nearest domain class, fixture access pattern, marker, dump helper, naming, and ordering.
- Register new markers in `src/tests/pytest.ini`; do not assume root-level pytest configuration.
- Keep secrets in ignored credential or cookie files referenced by fixtures. Never place secret values in tests, result files, SKILL content, or README examples.
- Update the Korean test README when a domain, fixture setup, result hierarchy, or invocation changes.

## Audit severity

For changed or newly added ETL surfaces, missing paired tests, fixtures, or marker registration are errors. For untouched legacy surfaces found by `--all`, report gaps as warnings. A skipped credentialed test is not a failure, but it is also not runtime verification.
