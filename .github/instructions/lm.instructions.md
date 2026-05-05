---
name: "LinkMerce Docstrings"
description: "Use when writing or revising docstrings or DAG doc_md in src/linkmerce/core, src/linkmerce/api, or airflow/dags. Prefer the templates below and change only the code-specific parts."
applyTo: src/linkmerce/core/**/*.py, src/linkmerce/api/**/*.py, airflow/dags/**/*.py
---

# LinkMerce Docstring and DAG Patterns

## Source of truth

Use this priority order.

1. Current function or class signature
2. Current adjacent implementation and runtime behavior
3. Matching `core/.../extract.py` class docstring and `extract()` method docstring
4. Matching `core/.../transform.py` class docstring
5. Current approved wording already present in neighboring modules

Do not preserve older AI-generated wording when it conflicts with the current code.

## Layer boundaries

- `Extractor`: queries, collects, generates, or downloads source data
- `Parser` / `ResponseTransformer`: parses one response into `list[dict]`-like rows
- `DuckDBTransformer`: transforms parsed rows and loads DuckDB tables
- `API`: calls Extractor plus Transformer and exposes one external function surface
- `DAG`: calls API functions, controls schedule, credentials, and BigQuery load strategy

Do not blur these layers.

## Default editing rule

When updating an existing docstring or `doc_md`, keep the approved section order and sentence pattern below, then change only the placeholders required by the current code.

- Do not rewrite the whole structure unless the current code uses a different approved pattern.
- Prefer editing the summary, parser names, table names, parameter list, or load strategy over inventing a new format.
- Reuse neighboring approved wording when the same layer and behavior match.

## Template: Extractor class docstring

Use this as the default skeleton for `core/**/extract.py` classes.

```python
"""<what this extractor queries, collects, or downloads>

<Menu/API/Docs/Referer/URL block when the module already uses one>

Attributes
----------
<config, cookies, task options, request timing options>
"""
```

Rules:

- The first line should describe source-data behavior only.
- Keep product metadata blocks when relevant.
- Use `Attributes` for configs, cookies, task options, and request timing options.
- If the class implements a shared workflow, describe the workflow steps explicitly.
- Do not describe DuckDB tables or BigQuery loading here.

## Template: Transformer class docstring

Use this as the default skeleton for `core/**/transform.py` classes.

```python
"""<one-line summary>

- **Extractor**
  - `<ExtractorClass>: <input> -> <output>`

- **Parser** / **Parsers**
  - `<ParserClass>: <input> -> <output>`

- **Table** ( *table_key: table_name* ):
  `table: physical_table`

- **Tables** ( *table_key: table_name (description)* ):
  1. `first: first_table` (description)
  2. `second: second_table` (description)

Parameters
----------
<only when runtime params are actually required>
"""
```

Rules:

- Section order: summary -> `Extractor` -> `Parser` / `Parsers` -> `Table` / `Tables` -> optional `Parameters`
- Single parser values such as `json`, `html`, `excel` should be written as their actual class names
- Table legend format must stay exactly in the current codebase style
- If runtime params are needed, keep this sentence exactly:
  - `**NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.`
- If one response becomes multiple tables, describe the split explicitly rather than using vague normalization language

## Template: Parser class docstring

Use this when documenting parser classes under `transform.py`.

```python
"""<one-line summary>

Parameters
----------
<only if this parser really consumes runtime options>
"""
```

Rules:

- Describe only response parsing behavior.
- Do not claim DuckDB loading or BigQuery loading.
- Input and output wording should match the real parser contract.

## Template: API function docstring

Use this as the default skeleton for `src/linkmerce/api/**/*.py` functions.

```python
"""<one-line summary>

**Table** ( *table_key: table_name* ):
    `table: physical_table`

**Tables** ( *table_key: table_name (description)* ):
    1. `first: first_table` (description)
    2. `second: second_table` (description)

Parameters
----------
<only parameters that actually exist in the signature>

Returns
-------
<return annotation>
    `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
"""
```

Summary patterns:

- Collection API: `... 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.`
- Download API: `... 보고서를 생성 및 다운로드해 DuckDB 테이블에 변환 및 적재한다.`
- Branching API: `... 유형에 맞는 ...를 생성 및 다운로드해 DuckDB 테이블에 변환 및 적재한다.`

Required rules:

- Keep `Table` / `Tables` directly below the summary.
- Document only parameters that exist in the current signature.
- Keep the `connection` description exactly:
  - `사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.`
- Document `return_type`, `extract_options`, `transform_options` when present.
- `request_delay` and `progress` wording should follow matching core wording.

## Template: API Returns block

Start every API `Returns` description with:

- `` `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다. ``

Then use the matching pattern.

- Multi-table APIs with `with_duckdb_connection(tables=...)`
  - `csv`: `{table_key: list[tuple]}`
  - `json`: `{table_key: list[dict]}`
  - `parquet`: `{table_key: Parquet 바이너리}`
- Single-table APIs with `with_duckdb_connection(table=...)`
  - `csv`: `list[tuple]`
  - `json`: `list[dict]`
  - `parquet`: `Parquet 바이너리`
- `none`
  - `모든 과정을 수행한 후 `None`을 반환한다.`

`raw` must describe the real extractor output, not a guessed generic shape.

- Collection API: original `dict`, `list[dict]`, or equivalent source response
- File download API: `{파일명: 엑셀 바이너리}` if the extractor really returns that shape
- Text download API: `TSV 텍스트` or `{보고서 유형: TSV 텍스트}` if that is the real output

## Template: DAG `doc_md`

Use this as the default skeleton for `airflow/dags/**/*.py`.

```python
doc_md = """
# <DAG title>

> <optional notice for trigger relationships or operational cautions>

## 인증(Credentials)
<what config or credentials are read>

## 추출(Extract)
<what business data the DAG fetches>

## 변환(Transform)
<how responses are split or merged into DuckDB tables>

## 적재(Load)
<actual BigQuery load strategy>
"""
```

Rules:

- Credentials must match exactly what the DAG reads from config.
- Extract should describe business data, usually from the invoked API.
- Transform should describe real DuckDB table splitting or merging.
- Load should describe actual BigQuery method: append, merge, overwrite, or mixed.
- If the DAG triggers another DAG, say so in a short notice blockquote.
- If the DAG is trigger-only or manual, say so explicitly.
- If one response becomes multiple tables, use specific split wording such as `A로부터 B와 C를 각각의 테이블로 분리해 적재한다.`

## Terminology rules

- Keep service-specific nouns exactly as the code and product UI use them.
- Preserve the distinction between collection and download.
  - Query/list endpoints: `조회`, `수집`
  - Report/file workflows: `생성 및 다운로드`
- Preserve the distinction between raw response and transformed tables.
- Keep `요청 간` phrasing. Do not rewrite it as `요청 사이의`.
- Use the natural Korean expansion of approved abbreviations when the surrounding code already does so.
  - Example: `PA` -> `매출 성장`, `NCA` -> `신규 구매 고객 확보`

## Prohibited mistakes

- Reusing stale AI-generated docstrings without checking current code
- Documenting parameters that are not in the current signature
- Moving `Table` / `Tables` below `Returns`
- Using generic `raw` descriptions that do not match the real return shape
- Claiming that DAGs directly implement low-level extractor behavior when they actually orchestrate API calls
- Mixing Extractor, Transformer, API, and DAG responsibilities into one summary sentence

## Final checklist

- Is the wording aligned with the current implementation, not older docs?
- Does each layer describe its own responsibility only?
- Are table names, parser names, and parameter names copied from real code?
- Does `raw` describe the true raw output shape?
- Does DAG `doc_md` state the real load strategy and trigger relationship?