# dbt 분석 쿼리 포팅 계획

> 대상 원본: `postgres/var/queries/*.sql`
>
> 운영 프로젝트: `dbt_bigquery/`, `dbt_postgres/`

## 1. 프로젝트 분리 원칙

LinkMerce dbt는 BigQuery와 PostgreSQL을 하나의 target 분기로 섞지 않고 물리적으로 분리한다.

- `dbt_bigquery/`: BigQuery 원천 dataset을 읽어 BigQuery 분석 cache table을 만든다.
- `dbt_postgres/`: PostgreSQL 원천 schema를 읽어 PostgreSQL 분석 table을 만든다.
- PostgreSQL 프로젝트에서 BigQuery dataset, BigQuery table function, 원격 BigQuery를 참조하지 않는다.
- BigQuery 프로젝트도 table function wrapper를 호출하지 않고 함수 본문을 dbt SQL model로 구현한다.
- 기존 `dbt/`의 `linkmerce_dbt` 프로젝트는 제거하고 두 프로젝트만 운영 대상으로 둔다.

원본 SQL은 BigQuery table function 형태였지만 dbt 모델은 table function을 호출하지 않는다. 함수 본문을 SQL model로 옮기고, 원천은 `source()`, 분석 모델 간 의존성은 `ref()`로 연결한다.

## 2. 현재 구현 상태

| 영역 | 구현 상태 | 비고 |
|---|---|---|
| BigQuery 모델 | 28개 구현 완료 | 실제 BigQuery 분석 테이블 생성은 추가 실행하지 않는다. |
| PostgreSQL 모델 | 28개 구현 완료 | 전체 모델이 `dbt compile`을 통과한다. |
| Helper view | 7개 분류 완료 | `ad_group_ad`, `meta_object`, `main_product`, `total_order`, `sold_quantity`, `stock_uptime`, `pivot_range` |
| BigQuery 검증 | `google_ads.report` 경로 생성 검증 완료 | `2023-09-06..2026-06-07`, 45,194 rows. |
| PostgreSQL 검증 | 전체 run 1차 수행, 마지막 패치 이후 재실행 필요 | 1차 run은 17개 성공, 8개 실패, 3개 skip이었다. 실패 원인 패치 후 compile 통과. |
| 기존 `dbt/` | 제거 | `dbt_bigquery/`, `dbt_postgres/` 기준으로 대체. |

PostgreSQL 1차 full run 이후 처리한 보정:

- `SELECT * EXCEPT (...)`를 PostgreSQL 명시 컬럼 선택으로 변환했다.
- `UNNEST(SPLIT(...))`를 `LEFT JOIN LATERAL unnest(string_to_array(...))`로 변환했다.
- `PIVOT`/`UNPIVOT`을 조건부 집계와 `UNION ALL`로 변환했다.
- BigQuery `STARTS_WITH`, `REGEXP_EXTRACT`, `SAFE.PARSE_DATE`, `SAFE_CAST` 계열을 PostgreSQL 표현으로 변환했다.
- 자동 변환 중 `2099-*` 리터럴로 굳은 날짜 필터를 dbt vars 기반 매크로로 복구했다.
- PostgreSQL partition scan에서 parallel worker가 lock/shared-memory를 더 많이 쓰지 않도록 model pre-hook으로 parallel append/gather를 껐다.

## 3. PostgreSQL 번역 규칙

| BigQuery 패턴 | PostgreSQL 번역 |
|---|---|
| `QUALIFY ROW_NUMBER() ... = 1` | window 값을 CTE/subquery에서 계산하고 outer `WHERE`로 필터링 |
| `UNNEST([STRUCT(...)])` | `VALUES (...) AS alias(col, ...)` |
| `UNNEST(SPLIT(x, ','))` | `LEFT JOIN LATERAL unnest(string_to_array(x, ',')) AS alias(col) ON TRUE` |
| `SAFE_CAST(expr AS INT64)` | 기본 `CAST(expr AS BIGINT)`, 실패 가능 데이터는 regex guard 또는 `NULLIF(..., '')::BIGINT` |
| `SAFE_OFFSET(n)` | PostgreSQL array의 1-based index로 변환 |
| `IF(cond, a, b)` | `CASE WHEN cond THEN a ELSE b END` |
| `COUNTIF(cond)` | `COUNT(*) FILTER (WHERE cond)` 또는 `COUNT(CASE WHEN cond THEN 1 END)` |
| `DATE_ADD/SUB` | date/timestamp + interval 연산 |
| `DATE_DIFF` | date subtraction 또는 `extract(epoch from ...) / 86400` |
| `GENERATE_ARRAY`, `GENERATE_DATE_ARRAY` | `generate_series()` |
| `SELECT * EXCEPT (...)` | 명시 컬럼 선택 |
| `PIVOT`, `UNPIVOT` | 조건 집계 또는 `UNION ALL` |
| `STRING`, `INT64`, `FLOAT64`, `BOOL`, `DATETIME` | `TEXT`, `BIGINT`, `DOUBLE PRECISION`, `BOOLEAN`, `TIMESTAMP` |

## 4. 모델 범위

PostgreSQL과 BigQuery 양쪽에 같은 모델명/폴더 구조를 둔다.

- `intermediate`: 광고, 주문, 매핑, 원천 보정 모델
- `marts/analytics`: 분석 fact/mart 모델
- `reports/ss_hcenter`: 쇼핑센터 리포트 모델

BigQuery는 비용 보호를 위해 이번 단계에서 추가 생성하지 않는다. PostgreSQL은 전체 모델 검증 대상이다.

한 파일에 여러 table function이 있는 경우, 마지막 function을 최종 산출물로 보고 앞쪽 function은 helper view로 둔다. 리포트 파라미터 helper인 `pivot_range`도 view로 둔다.

## 5. 검증 정책

BigQuery table function 검증은 비용 보호를 위해 `google_ads.report` 하나만 수행한다.

- 기간: `2023-09-06`부터 `2026-06-07`
- BigQuery 처리량: 실행 결과 기준 `google_report` 생성 약 `4.0 MiB`
- BigQuery row count: `45,194`
- PostgreSQL row count: `45,194`

PostgreSQL은 전체 분석 모델을 검증한다.

- 기본 검증 명령은 `dbt run` 전체 실행이다.
- `analysis_models_not_empty` singular test는 전체 모델이 생성되어 있고 최소 1행 이상 조회되는지 확인하는 smoke test다.
- 마지막 원격 full run 이후 SQL 패치를 추가했으므로, 운영 반영 전 `dbt_postgres` 전체 run을 다시 수행해야 한다.

## 6. 후속 최적화

초기 materialization은 최종 산출물은 `table` 또는 adapter별 incremental, 중간 helper는 `view`로 둔다. 전체 PostgreSQL run이 통과한 뒤 아래 순서로 최적화한다.

1. 날짜 파티션 기준 incremental/delete+insert 전략을 모델별로 적용한다.
2. 수동 overwrite master는 PostgreSQL에서 변경 영향 범위를 산출한 뒤 BigQuery에는 해당 범위만 넘긴다.
3. 표시용 dimension과 계산용 dimension을 분리해 `sabangnet.model` 같은 master 변경 영향 범위를 줄인다.
4. PostgreSQL affected range 산출이 안정화된 뒤 ClickHouse 적재 Airflow 작업을 연결한다.
