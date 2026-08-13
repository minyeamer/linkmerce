# Extractor, Transformer, and API Contract Matrix

| Surface | Required contract |
| --- | --- |
| Extractor annotation | Actual value returned by extraction tasks |
| Extractor `Returns` | Same type expression as the annotation |
| Transformer Extractor section | Extractor class name only |
| Parser input | Shape produced by the Extractor after task composition |
| Transformer `tables` | Keys used by `models.sql` and physical table values exposed by the API |
| API `raw=True` | Extractor runtime shape |
| API transformed return | DuckDB connection and table contract used by the decorator |

## Shape interpretation

- Inspect `src/linkmerce/common/tasks.py` before interpreting composed request helpers.
- Distinguish one response object from a list produced by repeated requests.
- Distinguish pagination aggregation from per-identifier aggregation.
- Do not add Transformer output types to an Extractor union merely because one API function can return either raw or transformed output.
- Resolve aliases and imported type names before declaring textually different annotations incompatible.

## Manual review boundaries

AST checks cannot prove runtime response shape, conditional decorators, dynamic table mappings, or parser behavior. Mark these cases for manual review and cite the exact class or function. Do not infer permission to execute credentialed APIs.
