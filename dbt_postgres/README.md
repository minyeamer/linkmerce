# LinkMerce dbt PostgreSQL

> PostgreSQL에서 분석 테이블과 테이블 함수를 생성하는 dbt 프로젝트

## 목차

- [개요](#개요)
- [한눈에 보기](#한눈에-보기)
- [프로젝트 구조](#프로젝트-구조)
- [모델 구성](#모델-구성)
- [Materialization 정책](#materialization-정책)
- [SQL 작성 규칙](#sql-작성-규칙)
- [Profile 설정](#profile-설정)
- [변수](#변수)
- [Selector](#selector)
- [실행 명령](#실행-명령)
- [테스트](#테스트)
- [dbt Docs](#dbt-docs)

## 개요

`dbt_postgres`는 LinkMerce 파이프라인이 PostgreSQL에 적재한 원천 테이블을 읽어,
운영 분석에 사용하는 PostgreSQL 산출물을 만드는 dbt 프로젝트다.

이 프로젝트는 채널별 주문/배송/광고/재고 데이터를 중간 모델로 정리한 뒤,
이를 통합한 일별 매출 모델과 주문수/영업이익 조회용 테이블 함수,
상품/옵션 마스터, 광고 및 재고 보고용 테이블 함수 등을 생성한다.

BigQuery 프로젝트와 달리, PostgreSQL 환경에 맞춰
파티션과 테이블 함수를 생성하기 위한 커스텀 materialization을 사용한다.

현재 모델은 크게 다음 네 영역으로 구성된다.

1. 채널별 매출/광고 intermediate 모델
2. 플랫폼별 재고/판매량 intermediate 모델
3. 매출/광고/재고 분석용 mart 모델
4. 상품/브랜드 및 채널별 상품/옵션 view 모델

## 한눈에 보기

| 항목 | 값 |
| --- | --- |
| 프로젝트 | `linkmerce_postgres` |
| 프로필 | `dbt_postgres` |

| 주요 스키마 | 요약 |
| --- | --- |
| `core` | 대표상품 테이블 및 공용 데이터 관리 |
| `xfm_ads` | 광고 intermediate 모델 |
| `xfm_sales` | 매출/배송 intermediate 모델 |
| `xfm_stock` | 재고 intermediate 모델 |
| `analytics` | 통합 분석 mart 모델 |

## 프로젝트 구조

```bash
dbt_postgres/
├── macros/
│   ├── mapping/
│   ├── generate_schema_name.sql
│   ├── incremental_partitions.sql
│   ├── partitioned_tables.sql
│   └── table_functions.sql
├── models/
│   ├── intermediate/
│   │   ├── ads/
│   │   ├── delivery/
│   │   ├── sales/
│   │   └── stock/
│   ├── marts/
│   │   ├── ads/
│   │   ├── product/
│   │   ├── sales/
│   │   └── stock/
│   ├── models.yml
│   └── sources.yml
├── dbt_project.yml
├── selectors.yml
└── README.md
```

| 파일 | 역할 |
| --- | --- |
| `dbt_project.yml` | 프로젝트 이름, profile, 기본 vars, 모델 기본 설정 |
| `models/sources.yml` | PostgreSQL 원천 테이블 선언 |
| `models/models.yml` | 모델 설명과 문서 메타데이터 |
| `selectors.yml` | Airflow Dag에서의 dbt 후속 실행 범위 또는 수동 실행 범위 모음 |
| `macros/incremental_partitions.sql` | 시작일부터 종료일까지의 날짜 파티션 목록을 계산하는 매크로 |
| `macros/partitioned_tables.sql` | 파티션 테이블을 생성하기 위한 `partitioned_table` materialization 정의 |
| `macros/table_functions.sql` | 테이블 함수를 생성하기 위한 `tvf` materialization 정의 |
| `macros/mapping/*.sql` | 채널별 코드-한글명 매핑 및 상품코드 변환 규칙 등을 정의한 매크로 모음 |

## 모델 구성

모델은 역할에 따라 `intermediate`, `marts` 두 계층으로 나뉜다.

| 계층 | 경로 | 역할 |
| --- | --- | --- |
| `intermediate` | `models/intermediate/` | 채널별 주문/배송/광고/재고 데이터를 요약하는 중간 모델 |
| `marts` | `models/marts/` | 대시보드 또는 엑셀 형태로 보고하기 위한 목적의 최종 산출 모델 |

### intermediate 모델

`intermediate` 계층의 모델을 트리 구조로 안내한다.
(확장자 `.sql`은 생략한다.)

#### intermediate/core

```bash
dbt_postgres/models/intermediate/
├── core__brand_master
└── core__product_master
```

`core__product_master`, `core__brand_master` 모델은 대표상품을
사방넷 상품 및 브랜드 기준으로 필터한 마스터 뷰 테이블을 제공한다.

#### intermediate/ads

플랫폼별 광고 실적을 상품별 광고비로 요약하는 intermediate 모델은 다음과 같다.

```bash
dbt_postgres/models/intermediate/ads/
├── coupang_ads/
│   ├── coupang_ads__campaign_master
│   ├── coupang_ads__adgroup_master
│   └── coupang_ads__insight_daily
├── google_ads/
│   ├── google_ads__campaign_master
│   ├── google_ads__adgroup_master
│   ├── google_ads__ad_master
│   └── google_ads__insight_daily
├── meta_ads/
│   ├── meta_ads__campaign_master
│   ├── meta_ads__adset_master
│   ├── meta_ads__ad_master
│   └── meta_ads__insight_daily
├── searchad/
│   ├── relation__smt_prd_to_ranged_sbn_ids
│   ├── searchad__campaign_master
│   ├── searchad__adgroup_master
│   ├── searchad__ad_master
│   ├── searchad__contract_master
│   ├── searchad__contract_daily
│   └── searchad__insight_daily
├── core__opex_daily
├── dable__report_daily
└── relation__ad_id_to_ranged_sbn_ids
```

이 계층에서는 소재-상품에 대한 1대다 매칭 및 n등분 분배를 통해 상품별 광고비를 계산해
일별 광고 fact 테이블을 생성한다.

캠페인(campaign), 광고그룹(adgroup), 소재(ad) 단위의 `master` 모델은
구글시트와 연동해 담당자가 수동으로 소재-상품을 매칭할 때 활용되며,
그 중 일부는 광고 보고서에 측정 기준을 붙일 때도 참조된다.

플랫폼별 `insight_daily` 모델은 매출 및 광고 분석을 위한 mart 모델에서 공통적으로 사용된다.

#### intermediate/sales

```bash
dbt_postgres/models/intermediate/sales/
├── coupang_rfm/
│   ├── coupang_rfm__order_count
│   └── coupang_rfm__sales_daily
├── sabangnet/
│   ├── sabangnet__order_count
│   └── sabangnet__sales_daily
├── smartstore/
│   ├── relation__smt_opt_to_sbn_ids
│   ├── smartstore__order_count
│   └── smartstore__sales_daily
└── core__sales_daily
```

이 계층에서는 주문 상태 매핑, 상품 매칭 및 세트 상품 분리, 원가 계산, 배송비 보정,
채널별 매핑 규칙을 반영해 일별 매출 fact 테이블을 생성한다.

`core__sales_daily` 모델은 채널별 매출과 광고비, 운영비, 그리고 기타 매출/광고/손익
데이터를 동일한 수준에서 병합해 전사 수준의 일별 매출 fact 테이블을 제공한다.
매출 및 영업이익 분석을 위한 mart 모델에서 공통적으로 사용된다.

채널별 `order_count` 모델은 요약된 일별 매출 fact 테이블에서 집계할 수 없는
주문수량을 집계하기 위해 원천 주문 데이터에 상품 매칭 및 세트 상품 분리만
적용한 일별 주문 fact 테이블을 생성한다.

`coupang_rfm/` 경로의 모델들은 쿠팡 로켓정산의 주간 집계 특성을 고려해,
실행할 날짜 범위를 매주 월요일부터 일요일 단위로 확장한 후,
요약된 결과를 요청 범위로 다시 제한한다.

#### intermediate/delivery

```bash
dbt_postgres/models/intermediate/delivery/
├── cj__invoice
└── cj__invoice_order
```

배송 관련 intermediate 모델은 CJ 내부 플랫폼별 송장 데이터를 병합한 뷰 테이블을 생성하며,
사방넷과 스마트스토어 매출 모델의 배송비 매칭에 사용된다.

별도의 스키마를 갖지 않고 `sales` 모델과 동일한 `xfm_sales` 스키마에 포함된다.

#### intermediate/stock

```bash
dbt_postgres/models/intermediate/stock/
├── sources/
│   ├── cj_eflexs__stock_qty_batch
│   ├── coupang_rfm__stock_qty_batch
│   └── ecount__stock_qty_batch
├── core__sold_qty_30d_daily
├── core__stock_qty_batch
└── core__stock_time_batch
```

이 계층에서는 채널마다 일별/차수별로 수집한 재고 내역을
이카운트 상품 기준으로 매핑 및 집계하여 플랫폼별 재고수량 fact 테이블을 생성한다.

`core__sold_qty_30d_daily` 모델은 `intermediate/sales/` 경로의
`sales_daily` 모델을 참조하여 재고 플랫폼별 판매량(출고량) fact 테이블을 생성한다.

`core__stock_time_batch` 모델은 단일 날짜 및 차수를 지정하는
대시보드에서 조회 범위 내 최종 재고 업데이트 시간을 표시하기 위한 목적을 가진다.

`core` 수준의 통합된 모델은 재고 분석을 위한 mart 모델에서 사용된다.

### marts 모델

`mart` 계층의 모델을 트리 구조로 안내한다.
(확장자 `.sql`은 생략한다.)

#### marts/ads

```bash
dbt_postgres/models/marts/ads/
├── analytics__adreport_daily
├── coupang_ads__report_daily
├── google_ads__report_daily
├── meta_ads__report_daily
└── searchad__report_daily
```

플랫폼별 `report_daily` 모델은 대응되는 플랫폼별 `insight_daily` 및 `master` 모델을
결합해 일별 광고 보고서를 제공하는 테이블 함수를 생성한다.

`analytics__adreport_daily` 모델은 플랫폼별 `report_daily` 모델들을
동일한 수준으로 맞춰서 병합한 테이블 함수를 제공한다.

#### marts/sales

```bash
dbt_postgres/models/marts/sales/
├── analytics__order_count
├── analytics__order_count_mom
├── analytics__profit_base
├── analytics__profit_daily
├── analytics__profit_monthly
├── analytics__profit_mom
└── analytics__sales_target
```

`analytics__profit_base` 모델은 `core__sales_daily` 모델에서
주문 상태 등의 조건에 따라 값을 조정하고 마진금액과 영업이익을 계산하는 테이블 함수를 생성한다.
대시보드에서 참조하는 나머지 `analytics__profit` 모델에서 공통적으로 참조된다.

`analytics__sales_target` 모델은 항상 전일 날짜를 기준으로 전월과 당월의 매출,
그리고 담당자가 지정한 당월의 목표 매출을 비교하기 위한 뷰 테이블을 생성한다.

`analytics__order_count` 모델은 플랫폼별 `order_count` 모델을 동일한 수준에서 병합하고
`core__product_master` 모델로부터 카테고리 등 대표상품의 측정 기준을 연결한 테이블 함수를 생성한다.

`_mom`으로 끝나는 모델들은 대시보드에서 지정된 기간으로부터 n개월 전의 항목들을 비교하기 위해
각각의 측정값을 단일 칼럼으로 UNPIVOT 변환하는 테이블 함수를 제공한다.

#### marts/product

```bash
dbt_postgres/models/marts/product/
├── coupang__option_master
├── sabangnet__product_master
├── sabangnet__option_master
├── smartstore__product_master
└── smartstore__option_master
```

`product_master` 및 `option_master` 모델은 쿠팡, 사방넷, 스마트스토어에 대해
전체 상품 및 옵션 목록을 조회하기 위한 뷰 테이블을 생성한다.

`product_master` 모델에는 유형 또는 상태 코드를 조합한 `sort_key`가 포함되며,
`option_master` 모델은 상품코드를 기준으로 `product_master` 모델을 결합해
`sort_key`를 참조한다.

#### marts/stock

```bash
dbt_postgres/models/marts/stock/
├── analytics__stock_report
├── analytics__stock_report_ds
├── analytics__stock_time_ds
└── analytics__stock_cost_mom
```

`analytics__stock_report` 모델은 재고수량과 30일 판매량을 결합해
이카운트 상품별 판매가능일과 예상소진일을 계산한다.
단일 날짜 및 차수에 대한 조회만 허용하며, 대시보드 및 엑셀 보고서에서 공통으로 참조한다.

`analytics__stock_report_ds` 모델은 `analytics__stock_report` 모델을
조회할 때 날짜를 생략하고 항상 최신 보고서를 조회할 수 있도록 조건문 및 fallback을 추가한
테이블 함수를 제공한다.
`REPORT_BATCH` 매개변수를 오전(10) 또는 오후(20)가 아닌 다른 값을 전달하면
현재 기준 최신 보고서를 반환한다.

`analytics__stock_time_ds` 모델은 `analytics__stock_report_ds` 모델에
대한 플랫폼별 최종 재고 업데이트 시간을 제공한다.
`REPORT_BATCH` 매개변수의 역할은 동일하다.

`analytics__stock_cost_mom` 모델은 지정된 기간으로부터 n개월 전의 항목 중에서
매월 말의 재고비용을 비교하기 위한 테이블 함수를 제공한다.
`analytics__profit_mom` 모델과 동일한 수준에서 조회하기 위해 행 보정을 수행한다.

## Materialization 정책

PostgreSQL 프로젝트는 모델 성격에 따라 `table`, `view`, `partitioned_table`, `tvf`를 사용한다.

| Materialization | 산출 대상 | 예시 |
| --- | --- | --- |
| `table` | 마스터 테이블 또는 복잡한 연산 결과 | `searchad__ad_master` |
| `view` | 단순한 연산 결과 | `core__product_master`, `relation__*` |
| `partitioned_table` | 일별 파티션 테이블 | `*_sales_daily`, `*_insight_daily`, `dable__report_daily` |
| `tvf` | 대시보드 조회용 보고서 | `analytics__profit_*`, `*_report_daily` |

### partitioned_table

`partitioned_tables` 매크로의 커스텀 materialization `partitioned_table`은
BigQuery의 `insert_overwrite` 전략에 대응하는 PostgreSQL 전용 materialization이며
다음과 같이 동작한다.

1. 모델 `config(...)`에 선언한 `partition_by`, `partitions` 설정과 매개변수 `batch_size`를 읽는다.
2. `batch_size > 0`이면 날짜 목록을 배치로 나누고 배치별 staging 테이블을 만든다.
3. 대상 테이블이 없으면 staging 테이블을 복제해 부모 테이블을 생성한다.
4. 대상 파티션이 있다면 비우고 없으면 생성하여 staging 결과를 적재한다.
5. 배치마다 커밋이 발생하며, `batch_size = 0`이면 전체 과정을 한 트랜잭션으로 실행한다.

일별 파티션 테이블을 생성하는 모델은 다음의 공통 설정을 추가한다.

```sql
{{
  config(
    materialized = "partitioned_table",
    partition_by = {
      "field": "...",
      "data_type": "date",
      "granularity": "day"
    },
    partitions = pg_date_partitions('ds_start_date', 'ds_end_date')
  )
}}
```

`partitions` 설정을 생략하면 전체 기간을 비우고 적재한다.

#### 배치 실행

`batch_size`는 한 트랜잭션에서 갱신할 최대 일 수다.

생략 시 기본값 `0`은 전체 범위를 하나의 트랜잭션으로 실행한다.

`batch_size`가 1보다 크다면 staging 테이블 생성, 파티션 생성 또는 비우기,
staging 결과 적재, 그리고 커밋까지의 과정을 나눠서 수행한다.

- 이미 커밋된 이전 배치는 다음 배치가 실패해도 롤백하지 않고 유지된다.
- `partitions` 설정을 생략한 경우 `batch_size = 0`으로 작동한다.

모델을 정의할 때 조회 시작일과 종료일은 `pg_batch_start_date`, `pg_batch_end_date`
매크로를 사용해 표현한다.

- `batch_size > 0`이면 배치마다 부분 파티션 시작일 및 종료일로 치환된다.
- `batch_size = 0`이면 매개변수 `ds_start_date`, `ds_end_date`로 치환된다.

```sql
WHERE ymd BETWEEN {{ pg_batch_start_date() }} AND {{ pg_batch_end_date() }}
```

```bash
dbt run --project-dir dbt_postgres --selector sales \
  --vars '{ds_start_date: "2026-06-01", ds_end_date: "2026-06-30", batch_size: 7}'
```

### 테이블 함수

`materializations` 매크로의 커스텀 materialization `tvf`는 다음과 같이 동작한다.

1. 모델 `config(meta={'params': [...]})`에 선언한 파라미터 목록을 읽는다.
2. 모델 SQL 본문을 감싼다.
3. `CREATE OR REPLACE FUNCTION ... RETURNS TABLE(...)` 구문을 실행해 테이블 함수를 만든다.

매개변수가 없을 경우 `params`를 생략할 수 있다.

## SQL 작성 규칙

### Jinja 주석으로 줄바꿈 제거

모델 파일은 CTE별로 읽기 좋게 여러 줄로 작성하되, 컴파일 결과에서는 토큰 사이에 안전한 공백만
남기기 위해 줄바꿈 경계에 Jinja 주석을 사용한다.

```sql
WITH{#

#} first_cte AS (
  SELECT ...
),{#

#} next_cte AS (
  SELECT ...
){#

#} SELECT * FROM next_cte
```

이 표현은 dbt 렌더링 후 주석과 그 안의 줄바꿈을 제거하므로,
컴파일된 SQL은 하나의 연결된 쿼리로 표현된다.

## Profile 설정

실제 접속 정보는 저장소에 두지 않는다.
로컬 실행은 `~/.dbt/profiles.yml`의 `dbt_postgres` profile을 사용한다.

현재 프로젝트는 target을 `dev`, `prod` 2가지로 나누어 사용한다.
기본 target은 `dev`로 두고, 운영 데이터 세트에 반영이 필요할 때만 `--target prod` 옵션을 명시한다.

권장 설정:

| 항목 | 값 |
| --- | --- |
| `type` | `postgres` |
| `host`, `port`, `user`, `password`, `dbname` | DB 연결 정보 |
| `threads` | 개발/검증은 `1` |

## 변수

`dbt_postgres` 프로젝트에서 사용하는 변수의 기본값은 `dbt_project.yml`에 선언되어 있다.

- `ds_start_date`
- `ds_end_date`
- `batch_size`

두 가지 날짜 변수와 `batch_size`는 partitioned_table의 교체 대상인 날짜 파티션 범위를 가리킨다.
변수의 기본값은, 전역 파싱 시 변수를 전달하지 않으면 발생하는 오류를 회피하기 위한 목적으로
먼 미래의 날짜를 지정했을 뿐이고, 관련 모델 실행 시 반드시 시작일과 종료일에 대한 변수를 제공해야 한다.

대표 예시는 다음과 같다.

```bash
dbt run --project-dir dbt_postgres --selector sales \
  --vars '{ds_start_date: "2026-06-01", ds_end_date: "2026-06-30", batch_size: 7}'
```

## Selector

`selectors.yml`에는 여러 개의 연관성 있는 모델을 일괄 실행하기 위한 selector를 정의한다.

소스 테이블에 데이터를 적재하는 Airflow Dag 실행 후 연관된 dbt 모델을
순차적으로 실행하기 위한 목적으로 `dag_id`와 동일한 selector를 정의해 사용한다.

Airflow 경로 내에서 다음과 같은 Dag에서 괄호 안의 `dag_id`와 같은 명칭의 selector를
사용하고 있다.

```bash
airflow/dags/
├── cj/
│   ├── eflexs_stock.py (cj_eflexs_stock)
│   └── loisparcel_invoice.py (cj_loisparcel_invoice)
├── coupang/
│   └── downstream/
│       ├── adreport.py (coupang_adreport)
│       ├── campaign.py (coupang_campaign)
│       ├── inventory.py (coupang_inventory)
│       └── rocket_sales.py (coupang_rocket_sales)
├── ecount/
│   ├── inventory.py (ecount_inventory)
│   └── product.py (ecount_product)
├── gsheets/
│   ├── sync_gsheets__ads_master.py (sync_gsheets__ads_master)
│   ├── sync_gsheets__expense.py (sync_gsheets__expense)
│   ├── sync_gsheets__extra_ads.py (sync_gsheets__extra_ads)
│   ├── sync_gsheets__extra_sales.py (sync_gsheets__extra_sales)
│   ├── sync_gsheets__opex.py (sync_gsheets__opex)
│   └── sync_gsheets__order_status.py (sync_gsheets__order_status)
├── sabangnet/
│   ├── invoice.py (sabangnet_invoice)
│   ├── order.py (sabangnet_order)
│   └── product.py (sabangnet_product)
├── searchad/
│   ├── contract.py (searchad_contract)
│   ├── master_gfa.py (searchad_master_gfa)
│   ├── master_sad.py (searchad_master_sad)
│   ├── report_gfa.py (searchad_report_gfa)
│   └── report_sad.py (searchad_report_sad)
├── smartstore/
│   ├── invoice.py (smartstore_invoice)
│   └── order.py (smartstore_order)
├── dable_ads.py (dable_ads)
├── google_ads.py (google_ads)
└── meta_ads.py (meta_ads)
```

로컬에서 전체 매출/광고 등을 일괄 갱신하기 위한 사용자 정의 selector도 제공된다.

| selector | 역할 |
| --- | --- |
| `coupang_master` | 쿠팡 옵션 목록 업데이트 |
| `sabangnet_master` | 사방넷 상품/옵션 목록 업데이트 |
| `smartstore_master` | 스마트스토어 상품/옵션 목록 업데이트 |
| `order_count` | 일별 주문 내역 업데이트 |
| `sales` | 일별 매출 보고서 업데이트 |
| `profit` | 일별 영업이익 보고서 업데이트 |
| `stock` | 재고수량 및 기간 판매량 업데이트 |
| `stock_report` | 재고-소비기한 보고서 업데이트 |

dbt 모델을 실행할 때 selector를 다음과 같이 지정할 수 있다.

```bash
dbt run --project-dir dbt_postgres --selector stock_report
```

## 실행 명령

컴파일만 확인할 때:

```bash
dbt compile --project-dir dbt_postgres --no-populate-cache
```

selector를 사용해 모델을 실행할 때:

```bash
dbt run --project-dir dbt_postgres --selector stock_report
```

selector를 사용하지 않고 특정 모델을 실행할 때:

```bash
dbt run --select path:models/marts/product/analytics__stock_report.sql
```

partitioned_table 모델을 실행하면서 파티션 범위를 전달할 때:

```bash
dbt run --project-dir dbt_postgres --selector sales \
  --vars '{ds_start_date: "2026-06-01", ds_end_date: "2026-06-02"}'
```

partitioned_table 모델을 넓은 파티션 범위로 실행하면서 배치 사이즈를 전달할 때:

```bash
dbt run --project-dir dbt_postgres --selector sales \
  --vars '{ds_start_date: "2021-01-01", ds_end_date: "2026-06-30", batch_size: 365}'
```

운영 환경에서 모델을 실행할 때:

```bash
dbt run --project-dir dbt_postgres --selector profit --target prod
```

## 테스트

현재 `dbt_postgres/tests/`에는 실행 중인 분석 테스트가 없다. `.gitkeep`만 유지하고 있다.

따라서 현재 검증은 보통 다음 방식으로 수행한다.

1. `dbt compile`로 SQL 컴파일 확인
2. 필요한 selector만 `dbt run`
3. 결과 테이블 또는 테이블 함수를 BigQuery에서 직접 조회

## dbt Docs

lineage graph와 모델 설명을 확인할 때는 docs artifact를 생성한 뒤 로컬 서버를 띄운다.

```bash
dbt docs generate --project-dir dbt_postgres --empty-catalog --no-populate-cache
dbt docs serve --project-dir dbt_postgres --port 8092
```

`--empty-catalog`는 catalog 조회 비용을 줄이고,
lineage 확인에는 `source()`와 `ref()` 관계가 들어 있는 manifest만으로도 충분하다.
