# dbt 프로젝트 운영 문서

> 대상 프로젝트: `dbt_bigquery/`, `dbt_postgres/`
>
> 원칙: 접속 정보와 서비스 계정 내용은 저장소에 기록하지 않는다.

## 1. 구성 요약

- 기존 `dbt/` `linkmerce_dbt` 프로젝트를 제거했다.
- `dbt_bigquery/`와 `dbt_postgres/`를 운영 프로젝트로 분리했다.
- 두 프로젝트 모두 `postgres/var/queries`의 table function 본문을 dbt SQL model로 구현한다.
- dbt 모델에서 BigQuery/PostgreSQL table function wrapper를 호출하지 않는다.
- 프로젝트 package name은 adapter package와 충돌하지 않도록 `linkmerce_bigquery`, `linkmerce_postgres`로 설정했다.
- profile 이름은 각각 `dbt_bigquery`, `dbt_postgres`를 사용한다.

## 2. Profile

홈 profile은 `/Users/cuz/.dbt/profiles.yml`에 둔다. 저장소에는 예시만 둔다.

- BigQuery 예시: `dbt_bigquery/profiles.example.yml`
- PostgreSQL 예시: `dbt_postgres/profiles.example.yml`

두 프로젝트 모두 `dev`, `prod` target을 사용한다.

- 기본 target은 `dev`다.
- `dev`는 산출 schema/dataset 뒤에 `_dev`를 붙인다.
- `prod`는 기존 산출 schema/dataset 이름을 그대로 사용한다.
- target별 schema 이름 생성은 각 프로젝트의 `macros/generate_schema_name.sql`에서 처리한다.

BigQuery 주요 값:

- `location`: `asia-northeast3`
- `threads`: 개발/검증은 `1` 권장
- `job_execution_timeout_seconds`: 운영은 `1800` 권장

PostgreSQL 주요 값:

- `schema`: 기본은 profile 값이지만 모델별 `schema` config가 있으면 해당 schema를 사용한다.
- 모델 산출물은 `int_*`, `mart_analytics`, `rpt_ss_hcenter` 등 layer/domain schema로 분리한다.
- `dbt_postgres`는 모델 pre-hook에서 `max_parallel_workers_per_gather=0`, `enable_parallel_append=off`를 설정해 큰 partition scan의 shared-memory/lock 부담을 낮춘다.

Helper view:

- `ad_group_ad`, `meta_object`, `main_product`, `total_order`, `sold_quantity`, `stock_uptime`, `pivot_range`는 중간 table function 또는 리포트 helper이므로 view로 materialize한다.

## 3. BigQuery 운영

BigQuery는 비용 보호 때문에 이번 단계에서 구현만 하고 전체 분석 테이블 생성은 실행하지 않는다.

허용된 검증:

```bash
cd dbt_bigquery
dbt run --target dev --select ad_group_ad google_report --vars '{"ds_start_date":"2023-09-06","ds_end_date":"2026-06-07"}'
dbt test --target dev --select analysis_models_not_empty
```

일별 운영 예시:

```bash
cd dbt_bigquery
dbt run --target dev --selector bigquery_daily --vars '{"ds_start_date":"2026-06-07","ds_end_date":"2026-06-07"}'
```

운영 dataset에 반영할 때만 `--target prod`를 명시한다.

selector 예시:

```bash
dbt ls --selector bigquery_all_models
dbt ls --selector bigquery_final_outputs
dbt ls --selector bigquery_helper_views
```

비용 보호 규칙:

- 전체 table function row-count 검증을 실행하지 않는다.
- `disabled_bigquery_update`, `parameterized`, `deprecated` 태그 모델은 기본 정기 실행에서 제외한다.
- 과거 변경분 반영은 PostgreSQL에서 산출한 affected range를 vars로 넘겨 제한 실행한다.
- `bigquery_analysis_tests`는 선택된 모델이 실제 BigQuery에 존재할 때만 실행한다.

## 4. PostgreSQL 운영

전체 모델 실행:

```bash
cd dbt_postgres
dbt run --target dev --vars '{"ds_start_date":"2023-09-06","ds_end_date":"2026-06-07","ds_start_datetime":"2023-09-06 00:00:00","ds_end_datetime":"2026-06-07 23:59:59","stock_start_datetime":"2023-09-06 00:00:00","stock_end_datetime":"2026-06-07 23:59:59","order_start_datetime":"2023-09-06 00:00:00","order_end_datetime":"2026-06-07 23:59:59"}'
```

전체 smoke test:

```bash
cd dbt_postgres
dbt test --target dev --select analysis_models_not_empty --vars '{"ds_start_date":"2023-09-06","ds_end_date":"2026-06-07","ds_start_datetime":"2023-09-06 00:00:00","ds_end_datetime":"2026-06-07 23:59:59","stock_start_datetime":"2023-09-06 00:00:00","stock_end_datetime":"2026-06-07 23:59:59","order_start_datetime":"2023-09-06 00:00:00","order_end_datetime":"2026-06-07 23:59:59"}'
```

운영 schema에 반영할 때만 `--target prod`를 명시한다.

selector 예시:

```bash
dbt ls --selector postgres_all_models
dbt ls --selector postgres_final_outputs
dbt ls --selector postgres_helper_views
dbt ls --selector postgres_analysis_tests
```

구간별 재실행 예시:

```bash
cd dbt_postgres
dbt run --select sbn_product_order smt_product_order total_order product_sales categorical_sales --vars '{"ds_start_date":"2026-06-07","ds_end_date":"2026-06-07","ds_start_datetime":"2026-06-07 00:00:00","ds_end_datetime":"2026-06-07 23:59:59"}'
```

금지사항:

- PostgreSQL 모델에서 BigQuery table function을 호출하지 않는다.
- PostgreSQL 모델에서 BigQuery dataset을 원격 참조하지 않는다.
- BigQuery SQL을 PostgreSQL에 그대로 복사해 실행 가능하다고 간주하지 않는다.
- DB 접속 정보, service account JSON, 원격 연결 문자열을 저장소 파일에 기록하지 않는다.

## 5. 검증 상태

2026-06-07 현재 확인한 결과:

| 프로젝트 | 명령 | 결과 |
|---|---|---|
| `dbt_bigquery` | `dbt run --select ad_group_ad google_report` | `ad_group_ad` 304 rows, `google_report` 45,194 rows |
| `dbt_postgres` | `dbt run --select ad_group_ad google_report` | `ad_group_ad` 304 rows, `google_report` 45,194 rows |
| `dbt_postgres` | 전체 smoke test 추가 | `analysis_models_not_empty` 사용 |
| `dbt_postgres` | 전체 `dbt run` 1차 | 17 PASS, 8 ERROR, 3 SKIP |
| `dbt_postgres` | 1차 실패 후 SQL 패치 | `dbt compile --no-populate-cache` PASS |

1차 전체 run에서 실패한 SQL 타입/문법 문제는 이후 패치했다. 마지막 원격 재실행은 도구 승인 한도에 막혀 수행하지 못했으므로, 운영 반영 전 위의 PostgreSQL 전체 실행 명령을 다시 수행해야 한다.

단일 parity test는 제거했고, 현재는 `analysis_models_not_empty`만 유지한다.

## 6. Partition overwrite incremental marts

The mart/report models tagged `partition_overwrite` are incremental models that refresh an explicit date range.

- `dbt_postgres`: uses `materialized='incremental'`, `incremental_strategy='append'`, and a model pre-hook that deletes the selected range from `{{ this }}` before inserting the new rows.
- `dbt_bigquery`: uses `materialized='incremental'`, `incremental_strategy='insert_overwrite'`, `partition_by`, and a static `partitions` list generated from dbt vars.
- Models without a natural row-level date partition remain `table`: `stock_report`, `stock_uptime`, `mall_product`, `pivot_range`, `pivot_sales`, and `two_sales`.

Initial rebuild after the materialization change:

```bash
cd dbt_postgres
dbt run --target prod --full-refresh --select +tag:partition_overwrite --vars '{"ds_start_date":"2023-09-06","ds_end_date":"2026-06-07","ds_start_datetime":"2023-09-06 00:00:00","ds_end_datetime":"2026-06-07 23:59:59"}'
```

```bash
cd dbt_bigquery
dbt run --target prod --full-refresh --select +tag:partition_overwrite --vars '{"ds_start_date":"2023-09-06","ds_end_date":"2026-06-07","ds_start_datetime":"2023-09-06 00:00:00","ds_end_datetime":"2026-06-07 23:59:59"}'
```

Daily or range-limited refresh:

```bash
dbt run --target prod --select +tag:partition_overwrite --vars '{"ds_start_date":"2026-06-07","ds_end_date":"2026-06-07","ds_start_datetime":"2026-06-07 00:00:00","ds_end_datetime":"2026-06-07 23:59:59"}'
```

Project selectors are also available:

```bash
dbt ls --selector postgres_partition_overwrite
dbt ls --selector bigquery_partition_overwrite
```
