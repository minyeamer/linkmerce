# LinkMerce dbt Golden Sources

Use current files first. Use these commits to resolve patterns only when the current paired implementation does not answer the question.

## Historical commits

- `6a1b25f` — approved PostgreSQL Jinja comment delimiters between CTEs.
- `6d3e801` — aligned numeric division, rounding, and casting between BigQuery and PostgreSQL.
- `2e36136` — paired BigQuery/PostgreSQL table-function implementation for `analytics__total_order`.
- `0cd1f4f` — intermediate sales migration.
- `2d905bf` — daily ads and partitioned materialization migration.
- `28fc5e2` — intermediate stock migration.
- `99e54b1`, `ddd8288`, `3df4121`, `1484f01` — marts ads, product, sales, and stock migrations.

## Current paired models

- `models/intermediate/ads/naver_connect__insight_daily.sql` for paired partitioned models and batch date macros.
- `models/intermediate/stock/core__stock_qty_batch.sql` for PostgreSQL partition replacement.
- `models/marts/sales/analytics__total_order.sql` for large paired TVFs, typed `NULL`, numeric calculations, and Jinja CTE boundaries.
- `models/marts/stock/analytics__stock_cost_mom.sql` for day-of-week mapping and date formatting.

## Precedence

1. Explicit user wording and behavior.
2. Complete current files and their paired counterpart.
3. Repository instructions and this skill.
4. Golden commits.

Never overwrite a current user edit with a historical version solely because the historical version is listed here.
