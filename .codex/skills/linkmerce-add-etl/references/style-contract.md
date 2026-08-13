# LinkMerce ETL Style Contract

## Contents

1. Authority and preservation
2. Python layout
3. Extractor docstrings
4. Request task documentation
5. Transformer docstrings
6. API docstrings
7. Airflow Dag documentation
8. Naming and comments

## 1. Authority and preservation

- Write docstrings and README prose in Korean. Write exception and error messages in English.
- Write explanatory prose in English. Use Korean only inside examples, templates, or exact repository-facing text that an agent must preserve or copy.
- Read a whole file before editing any part of it.
- Preserve user-authored text unless behavior or an explicit wording request requires a change.
- Treat every Korean noun, spacing choice, code span, list indent, and punctuation mark as intentional.
- Use `.` only for a complete sentence or to separate distinct statements. Do not append it to a label or noun phrase such as `고객 ID`.
- Do not append `이다.` to an established fragment such as `기본값은 `True``.
- Do not replace service terminology with a plausible synonym. Copy the term from the current UI, user request, config, or approved neighboring implementation.
- Do not change a correct docstring merely to make adjacent docstrings visually uniform.

## 2. Python layout

Use this continuation layout for a module-level function:

```python
def function_name(
        required: str,
        optional: int = 1,
        **kwargs,
    ) -> dict:
```

Use the same two-level continuation relative to an indented method:

```python
class Example:
    def method(
            self,
            required: str,
            optional: int = 1,
            **kwargs,
        ) -> dict:
```

For every multiline call, use one space on each side of `=` and a trailing comma:

```python
result = function_name(
    required = value,
    optional = 1,
)
```

For a multiline `dict(...)` constructor, use the same rule:

```python
parser_config = dict(
    dtype = dict,
    scope = "data",
    fields = ["id", "name"],
)
```

For a literal mapping, use `:` because it is not a keyword argument. Keep compact one-line calls and mappings compact when neighboring code does so. The multiline rule does not authorize formatting unrelated lines.

Keep `from __future__ import annotations` first. Put runtime imports before `TYPE_CHECKING`; keep type-only imports under `if TYPE_CHECKING:` when the module follows that pattern.

## 3. Extractor docstrings

Keep the class section order exactly: one-line description, optional operational notice, related locations, then `Attributes`.

Keep related locations in the established `Menu`, `API`, `Docs`, `Referer`, or `URL` order and omit only sections that do not exist.

```python
class Feature(Domain):
    """<source data behavior>하는 클래스.

    <optional notice>

    - **Menu**: <menu>
    - **API**: <endpoint>
    - **Docs**: <official docs>
    - **Referer**: <referer>

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `<argument>` 인자로 <required input>을 반드시 전달해야 한다.

    <attribute>: <type>
        <approved description>
    """
```

Describe source requests only. Do not mention DuckDB, physical warehouse tables, dbt, or Airflow in an Extractor class.

Keep the `extract` method section order exactly: one-line description, optional notice, `Parameters`, then `Returns`. Omit `Parameters` only when no public input other than `self`, `cls`, or internal `**kwargs` exists.

```python
def extract(...) -> dict | list[dict]:
    """<business data>를 <request behavior>해 JSON 형식으로 반환한다.

    Parameters
    ----------
    <parameter>: <annotation>
        <approved description>

    Returns
    -------
    dict | list[dict]
        <business result>. <conditions that select each shape>
    """
```

Copy the return annotation into the line below `Returns` exactly. Explain every shape-changing condition introduced by `Iterable`, date expansion, page expansion, or report-type expansion. Do not include a Transformer return type.

## 4. Request task documentation

Select the task by the actual request dimensions implemented in `src/linkmerce/common/tasks.py`:

| Request topology | Task | Selection rule |
| --- | --- | --- |
| Direct | `Request` | Run one callable or coroutine and optionally parse its result. A direct Extractor method may perform the request without exposing `Request` in `default_options`. |
| Retry | `RequestLoop` | Repeat one request until `condition` succeeds or `max_retries` is exhausted. |
| Expand | `RequestEach` | Run one request for each value or Cartesian-product context. A dictionary context represents one request; a sequence represents repeated requests. |
| Expand and retry | `RequestEachLoop` | Apply `RequestLoop` to every expanded context through `.loop(...)`. |
| Page | `PaginateAll` | Request the first page, obtain the total count through `counter`, and traverse the remaining numbered pages. |
| Expand and page | `RequestEachPages` | Apply `PaginateAll` to every expanded context through `.all_pages(...)`; when an explicit page is supplied, request only that page. |
| Cursor | `CursorAll` | Continue synchronous requests until `get_next_cursor` returns `None`. |
| Expand and cursor | `RequestEachCursor` | Apply `CursorAll` to every expanded context through `.all_cursor(...)`. This task is synchronous only. |

`Task` is the abstract root, while `RunLoop` and `ForEach` are generic execution primitives used to compose the request tasks above. Do not substitute these primitives for an available request-specific task. Use `parse`, `partial`, `expand`, and `concat` only when the selected class implements the method, and preserve the method chaining order used by the Extractor.

Use the exact task class in both `default_options` and the Attributes notice:

```python
**NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

request_delay: float | int | tuple[int, int]
    <request unit>별 요청 간 대기 시간(초). 기본값은 `<default>`
tqdm_options: dict | None
    반복 요청 작업 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
```

For `RequestLoop`, use `재시도 요청 간 대기 시간(초)`. For `PaginateAll`, use `페이지 요청 간 대기 시간(초)` and `페이지 순회 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수`. For `CursorAll`, use `커서 요청 간 대기 시간(초)`.

For a composed task, document each participating option block in execution order:

- `RequestEachLoop`, then `RequestLoop`
- `RequestEachPages`, then `PaginateAll`
- `RequestEachCursor`, then `CursorAll`

Copy the exact option name, default value, Attributes wording, and chaining order from the closest current Extractor. Do not infer them from the class name.

## 5. Transformer docstrings

Keep the section order exactly: one-line description, `Extractor`, `Parser` or `Parsers`, `Table` or `Tables`, then optional `Parameters`.

Put only the class name in `Extractor`. Never add input or output types there.

Use this single-table structure:

```python
class Feature(DuckDBTransformer):
    """<business data>를 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**
        - `Feature`

    - **Parser**
        - `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: provider_feature`

    Parameters
    ----------
    <parameter>: <type>
        <noun phrase without an automatic period>
    """
```

Use this multi-table structure:

```python
class Feature(DuckDBTransformer):
    """<business data>와 <derived data>를 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**
        - `Feature`

    - **Parser**
        - `JsonTransformer: dict -> list[dict]`

    - **Tables** ( *table_key: table_name (description)* ):
        1. `first: provider_first` (<approved description>)
        2. `second: provider_second` (<approved description>)

    Parameters
    ----------
    <parameter>: <type>
        <approved description>
    """
```

Use `Parsers` only when more than one parser actually participates. Use actual parser class names, not the shorthand assigned to `parser`. Keep table keys, internal names, order, and descriptions identical to `tables` and the API decorator.

Document `Parameters` only for values consumed by `params`, parser options, or overridden transform logic. Do not insert stock prose that is absent from the approved local pattern.

## 6. API docstrings

Keep the section order exactly: one-line description, `Table` or `Tables`, `Parameters`, then `Returns`.

Use the same Table/Tables legend, key order, internal table names, and descriptions as the Transformer.

Use established parameter descriptions verbatim where the contract is the same. In particular:

```text
사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
반환 형식. **Returns** 문단을 참고한다.
`Extractor` 초기화 옵션
`Transformer` 초기화 옵션
```

For a repeated request, copy the current approved wording for `request_delay` and `progress`; do not append `이다.`. For example:

```text
키워드별 요청 간 대기 시간(초). 기본값은 `1`
반복 요청 작업의 진행률 출력 여부. 기본값은 `True`
```

Start Returns with this exact sentence:

```text
`return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
```

For one table, document `csv`, `json`, and `parquet` as one table result. For multiple tables, document them as `{table_key: result}`. Document `raw` from the real Extractor return annotation and behavior. Document `none` from the real API behavior. The API return annotation, Returns type line, and every listed shape must agree.

## 7. Airflow Dag documentation

Keep this heading order exactly:

```python
"""
# <title>

## 인증(Credentials)
<actual credential acquisition>

## 추출(Extract)
<business data and date basis>

## 변환(Transform)
<real response parsing and DuckDB table split>

## 적재(Load)
<append, merge, overwrite, partition, and downstream dbt behavior>
"""
```

Do not copy credential wording, table names, or load modes from another Dag without verifying the current implementation and config. For mixed load modes, use separate list items in actual load order. Keep the bottom variable order `etl_result` or `etl_results`, `dbt_date_range`, then `dbt_run` when that pipeline shape applies.

## 8. Naming and comments

- Use `Dag` for an Airflow workflow in prose and `Dags` for the plural. Use `DAG` only for the Python class, its import, or constructor usage.
- Preserve lowercase code identifiers such as `dag` and `dag_id`.
- Use the service's official business term in Korean descriptions.
- Use snake_case for modules, functions, Dag IDs, selectors, table keys, and internal DuckDB table names.
- Use PascalCase for paired Extractor and Transformer classes; alias the Transformer locally as `T` in the public API.
- Use one stable table key from Transformer through API, Dag `sources`, config `tables`/`merge`, and load results.
- Use `schema.table` only for warehouse targets; keep DuckDB internal names unqualified.
- Add comments only when they convey a reason or non-obvious constraint. Preserve existing comments and their punctuation unless behavior changes.
