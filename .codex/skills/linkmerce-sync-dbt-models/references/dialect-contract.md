# BigQuery to PostgreSQL dbt Dialect Contract

Preserve behavior before syntax. Read the closest current paired model before choosing a translation.

## Formatting

Use Jinja comments at PostgreSQL CTE boundaries so dbt removes source newlines while keeping the model readable:

```sql
WITH{#

#} first_cte AS (
  SELECT ...
),{#

#} second_cte AS (
  SELECT ...
){#

#} SELECT * FROM second_cte
```

Keep the existing leading-comma column style, comment positions, CTE order, and quote style.

## Common translations

| BigQuery | PostgreSQL |
| --- | --- |
| `IF(condition, a, b)` | `CASE WHEN condition THEN a ELSE b END` |
| `SAFE_CAST(value AS type)` | Validate first, commonly with a regex, then cast |
| `CAST(value AS INT64)` | `value::bigint` or an explicit PostgreSQL `CAST` |
| `CAST(value AS STRING)` | `value::text` |
| `SPLIT(value, delimiter)[SAFE_OFFSET(n)]` | `string_to_array(value, delimiter)[n + 1]` or `split_part` |
| `UNNEST(SPLIT(...)) WITH OFFSET` | `CROSS JOIN LATERAL unnest(string_to_array(...)) WITH ORDINALITY` |
| `ARRAY_LENGTH(array)` | `cardinality(array)` |
| `QUALIFY ROW_NUMBER()` | A row-number subquery or `DISTINCT ON` with an equivalent order |
| `ARRAY_TO_STRING(ARRAY(...), delimiter)` | `concat_ws` or filtered `array_to_string` |
| `DATE_SUB` / `DATE_ADD` | Date arithmetic or explicit PostgreSQL intervals |
| `FORMAT_DATETIME` | `to_char` |
| BigQuery regex functions | PostgreSQL regex operators or functions |

BigQuery offsets start at zero and PostgreSQL ordinality starts at one. Adjust every translated comparison and remainder-allocation condition deliberately.

## Numeric behavior

- Cast operands to `numeric` before division when PostgreSQL integer division would lose precision.
- Apply `ROUND(..., scale)` to `numeric`; cast percentile results before two-argument `ROUND`.
- Round before converting calculated values to integer when BigQuery does so.
- Preserve divide-by-zero handling with `NULLIF`.
- Do not flag repository-supported functions such as `DIV` or `starts_with` merely because they also exist in BigQuery.

## Dates and partitions

- Translate BigQuery `incremental` plus `insert_overwrite` models to the repository's PostgreSQL `partitioned_table` materialization.
- Replace `bq_date_partitions` with `pg_date_partitions`.
- Use `pg_batch_start_date()` and `pg_batch_end_date()` inside partitioned models so batched executions use the active batch range.
- Preserve lookback, lookahead, inclusive, and exclusive boundaries exactly.
- Filter the final output back to the requested partition range when source reads use an expanded range.
- Translate `CURRENT_DATE('Asia/Seoul')` with an explicit Asia/Seoul expression rather than the database session timezone.

## Table functions and unions

- Translate BigQuery TVF parameter types to PostgreSQL types, including `string` to `text` and `datetime` to `timestamp`.
- Cast `NULL` fields explicitly when union branches or TVF return inference require stable types.
- Keep parameter names, order, alias, and output column order unchanged.
- BigQuery `EXTRACT(DAYOFWEEK)` returns Sunday as 1; PostgreSQL `EXTRACT(DOW)` returns Sunday as 0. Add one when joining the shared 1-to-7 mapping.

## Final verification

Compile both models with identical variables. Inspect the compiled PostgreSQL SQL, then compare columns, order, row counts, keys, null behavior, and representative aggregates against BigQuery on an authorized target.
