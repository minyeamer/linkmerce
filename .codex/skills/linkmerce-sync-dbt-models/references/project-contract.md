# LinkMerce Paired dbt Project Contract

## Authority and modes

- Treat `dbt_bigquery/models` as the behavioral source of truth.
- Default to `audit-only`; report gaps without creating or modifying models.
- Enter `create-counterpart` or `synchronize-existing` only after an explicit implementation request.
- A SKILL maintenance request does not authorize changes outside the SKILL directory.

## Pairing

- Require the same relative `.sql` path and filename under `dbt_postgres/models` unless the user explicitly approves an exception.
- Keep model names, configured `schema`, configured `alias`, source and ref dependencies, output column names, output order, and business meaning aligned.
- Permit materialization and physical types to differ only when required by the warehouse.

## Metadata and sources

- Add every implemented model to both `models/models.yml` files in the same logical section.
- Copy Korean descriptions exactly when the business contract is identical.
- Declare referenced source tables in each `models/sources.yml`.
- Keep selectors equivalent in name, purpose, method, graph expansion, and dependency order.

## README inventory

Update `dbt_bigquery/README.md` and `dbt_postgres/README.md` whenever implementation adds, moves, renames, or deletes a model. Include the relative path, role, materialization when relevant, new schema or layer, selectors, and related Airflow Dag path. Use Korean prose and matching section order and terminology.

## Airflow integration

- Use the same selector name in Airflow and both dbt projects.
- For dual warehouse loads, define both dbt TaskGroups.
- Preserve the repository's bottom-variable names and order.
- Use `Dag` for the workflow in prose and `DAG` only for the Python class, import, or constructor.

## Validation

- Run dbt only as `conda run -n dbt dbt ...`.
- Python-only audit scripts may use the `main` environment.
- In audit-only mode, report parse, pairing, metadata, selector, README, and Airflow mismatches without fixing them.
- In an authorized implementation mode, completion requires paired paths, successful parsing, selector compilation, aligned contracts, current README inventories, and dual TaskGroups where applicable.
