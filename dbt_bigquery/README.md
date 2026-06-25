# LinkMerce dbt BigQuery

> BigQuery 원천 dataset에서 BigQuery 분석 table과 table function을 생성하는 dbt 프로젝트

## 목차

- [개요](#개요)
- [한눈에 보기](#한눈에-보기)
- [프로젝트 구조](#프로젝트-구조)
- [모델 구성](#모델-구성)
- [Materialization 정책](#materialization-정책)
- [Profile 설정](#profile-설정)
- [변수](#변수)
- [Selector](#selector)
- [실행 명령](#실행-명령)
- [테스트](#테스트)
- [dbt Docs](#dbt-docs)
- [운영 참고사항](#운영-참고사항)

## 개요

`dbt_bigquery/`는 LinkMerce가 BigQuery에 적재한 원천 dataset를 읽어, 운영 분석에 사용하는 BigQuery 산출물을 만드는 dbt 프로젝트다.

이 프로젝트는 채널별 주문·배송·광고 데이터를 중간 모델로 정리한 뒤, 이를 통합한 일별 매출 mart와 주문수/수익성 조회용 sales table function, 채널별 광고 보고용 table function을 생성한다. PostgreSQL 프로젝트와 달리 BigQuery 환경에 맞춘 partition overwrite incremental 전략과 custom table function materialization을 사용한다.

현재 모델은 크게 3가지 역할로 나뉜다.

1. 채널별 sales intermediate 모델
2. 광고 매체별 ads intermediate 모델
3. 최종 분석 mart 및 BigQuery table function

## 한눈에 보기

| 항목 | 값 |
| --- | --- |
| dbt project name | `linkmerce_bigquery` |
| profile | `dbt_bigquery` |
| adapter | `dbt-bigquery` |
| 모델 수 | 40개 |
| 원천 참조 | BigQuery dataset `source()` |
| 모델 참조 | dbt `ref()` |
| 주요 산출 schema | `core`, `xfm_sales`, `xfm_ads`, `analytics`, 각 매체 전용 schema |

## 프로젝트 구조

```bash
dbt_bigquery/
├── macros/
│   ├── generate_schema_name.sql
│   ├── incremental_partitions.sql
│   ├── materializations.sql
│   └── mapping/
├── models/
│   ├── intermediate/
│   │   ├── ads/
│   │   ├── delivery/
│   │   └── sales/
│   ├── marts/
│   │   ├── ads/
│   │   └── sales/
│   ├── models.yml
│   └── sources.yml
├── dbt_project.yml
├── selectors.yml
└── README.md
```

주요 파일의 역할은 다음과 같다.

| 파일 | 역할 |
| --- | --- |
| `dbt_project.yml` | 프로젝트 이름, profile, 기본 vars, 모델 기본 설정 |
| `models/sources.yml` | BigQuery 원천 dataset/table 선언 |
| `models/models.yml` | 모델 설명과 docs metadata |
| `selectors.yml` | 도메인별 실행 범위를 정의하는 selector 모음 |
| `macros/incremental_partitions.sql` | 날짜 범위로 partition 목록과 주간 경계를 계산하는 매크로 |
| `macros/materializations.sql` | BigQuery table function 생성을 위한 custom `tvf` materialization |
| `macros/mapping/*.sql` | 채널별 주문 상태, 매핑, 비용 계산 등에 사용하는 공통 SQL 매크로 |

## 모델 구성

모델은 역할에 따라 `intermediate`와 `marts` 두 계층으로 나눈다.

| 계층 | 경로 | 역할 |
| --- | --- | --- |
| Intermediate | `models/intermediate/` | 채널별 주문/광고/배송 데이터를 분석용 형태로 정리하는 중간 모델 |
| Mart | `models/marts/` | 운영 분석이나 보고에 직접 사용하는 최종 산출 모델 |

### Intermediate 모델

#### 1. Core

- `core__product_master`
- `core__opex_daily`

`core__product_master`는 상품 기준 마스터 차원을 제공하고, `core__opex_daily`는 운영비를 일자 단위로 정리해 최종 매출 mart에서 사용한다.

#### 2. Delivery

- `cj__invoice`
- `cj__invoice_order`

배송 관련 intermediate 모델은 CJ 송장 데이터를 정규화해 Sabangnet, Smartstore 매출 모델의 배송비 매칭에 사용한다.

#### 3. Sales

채널별 주문 데이터를 정리하는 intermediate 모델은 다음과 같다.

- `sabangnet`
  - `sabangnet__order_count`
  - `sabangnet__sales_daily`
- `smartstore`
  - `relation__smt_opt_to_sbn_ids`
  - `smartstore__order_count`
  - `smartstore__sales_daily`
- `coupang_rfm`
  - `coupang_rfm__order_count`
  - `coupang_rfm__sales_daily`

이 계층에서는 주문 상태 매핑, 번들 상품 전개, 공급원가 계산, 배송비 보정, 채널별 매핑 규칙을 반영해 일별 fact를 만든다.

특히 `coupang_rfm__sales_daily`와 `coupang_rfm__order_count`는 쿠팡 로켓정산 원천의 주간 집계 특성을 고려해, 입력한 날짜 범위를 주간 경계로 확장해 partition filter를 적용한 뒤 최종 결과만 요청 범위로 다시 제한한다.

#### 4. Ads

광고 intermediate 모델은 매체별 차원과 일별 실적 fact를 준비한다.

- 공통 매핑
  - `relation__ad_id_to_ranged_sbn_ids`
  - `relation__smt_prd_to_ranged_sbn_ids`
- `coupang_ads`
  - `coupang_ads__campaign_master`
  - `coupang_ads__adgroup_master`
  - `coupang_ads__insight_daily`
- `google_ads`
  - `google_ads__campaign_master`
  - `google_ads__adgroup_master`
  - `google_ads__ad_master`
  - `google_ads__insight_daily`
- `meta_ads`
  - `meta_ads__campaign_master`
  - `meta_ads__adset_master`
  - `meta_ads__ad_master`
  - `meta_ads__insight_daily`
- `searchad`
  - `searchad__campaign_master`
  - `searchad__adgroup_master`
  - `searchad__ad_master`
  - `searchad__contract_master`
  - `searchad__contract_daily`
  - `searchad__insight_daily`

이 계층은 광고 ID나 상품 매핑 테이블을 날짜 범위 기준으로 풀어내고, 각 매체의 원천 실적을 상품 기준 fact로 정리한다.

### Mart 모델

#### 1. Sales marts

- `analytics__sales_daily`
- `analytics__order_count`
- `analytics__profit_daily`
- `analytics__profit_monthly`
- `analytics__profit_mom`

`analytics__sales_daily`는 Sabangnet, Smartstore, Coupang RFM 매출과 광고비, 운영비를 합친 통합 일별 mart다.

`analytics__order_count`는 채널별 주문건수를 통합하고 상품/쇼핑몰 마스터를 결합해
주문수 조회에 사용하는 BigQuery table function이다.

`analytics__profit_daily`와 `analytics__profit_monthly`는 통합 매출 mart를 기반으로
상품 마스터와 결합해 일별/월별 수익성 조회에 사용하는 BigQuery table function이다.

`analytics__profit_mom`은 지정 기간 결과와 이전 월 구간을 함께 비교할 수 있도록
metric 단위로 펼친 BigQuery table function이다.

#### 2. Ads marts

- `coupang_ads__report_daily`
- `google_ads__report_daily`
- `meta_ads__report_daily`
- `searchad__report_daily`

광고 mart는 모두 BigQuery table function 형태로 제공되며, intermediate fact와 차원 모델을 조합해 보고용 결과를 만든다.

## Materialization 정책

BigQuery 프로젝트는 모델 성격에 따라 `view`, `table`, `incremental`, `tvf`를 함께 사용한다.

| 구분 | 예시 모델 | Materialization |
| --- | --- | --- |
| Helper view | `core__product_master`, `cj__invoice`, `relation__ad_id_to_ranged_sbn_ids` | `view` |
| 기준성 table | `core__opex_daily`, 각종 `*_master`, `searchad__contract_daily`, `relation__smt_opt_to_sbn_ids` | `table` |
| 일별 fact | `*_sales_daily`, `*_order_count`, `*_insight_daily`, `analytics__sales_daily` | `incremental` |
| 최종 보고 함수 | `analytics__order_count`, `analytics__profit_*`, `*_report_daily` | `tvf` |

### Partition overwrite incremental

일별 fact 계열 모델은 주로 다음 규칙을 따른다.

- `materialized='incremental'`
- `incremental_strategy='insert_overwrite'`
- `partition_by` 설정
- `require_partition_filter=true`

partition 목록은 `incremental_partitions.sql`의 매크로로 생성한다.

| 매크로 | 역할 |
| --- | --- |
| `bq_date_partitions()` | 날짜 partition 목록 생성 |
| `bq_datetime_partitions()` | datetime partition 목록 생성 |
| `bq_week_start_date()` | 입력 시작일을 같은 주의 월요일로 확장 |
| `bq_week_end_date()` | 입력 종료일을 같은 주의 일요일로 확장 |

`bq_week_start_date()`와 `bq_week_end_date()`는 Coupang rocket settlement 원천(`sales`, `shipping`)에 대해 파티션 필터를 유지하면서 주간 집계 범위를 읽기 위해 사용한다.

### BigQuery table function

`materializations.sql`의 custom `tvf` materialization은 다음과 같이 동작한다.

1. 모델 `config(params=...)`에 선언한 파라미터 목록을 읽는다.
2. 모델 SQL 본문을 감싼다.
3. `create or replace table function ... as (...)` 구문으로 BigQuery table function을 만든다.

## Profile 설정

실제 접속 정보는 저장소에 두지 않는다. 로컬 실행은 `~/.dbt/profiles.yml`의 `dbt_bigquery` profile을 사용한다.

현재 프로젝트는 보통 `dev`, `prod` target을 나누어 사용한다. 기본 target은 `dev`로 두고, 운영 dataset 반영이 필요할 때만 `--target prod`를 명시한다.

권장 설정:

| 항목 | 값 |
| --- | --- |
| `type` | `bigquery` |
| `method` | `service-account` |
| `threads` | 개발/검증은 `1` |
| `location` | 실제 BigQuery dataset이 있는 리전 |
| `keyfile` | 로컬 서비스 계정 JSON 경로 |

BigQuery 프로젝트 ID에 하이픈이 포함될 수 있으므로 `dbt_project.yml`에서는 quoting을 모두 켜두었다.

```yaml
quoting:
  database: true
  schema: true
  identifier: true
```

## 변수

기본 vars는 `dbt_project.yml`에 다음 두 값이 선언되어 있다.

- `ds_start_date`
- `ds_end_date`

하지만 실제 실행에서는 대부분 명시적으로 값을 넘겨주는 쪽을 전제로 한다.

또한 일부 incremental 모델은 SQL 안에서 다음 변수를 함께 기대한다.

- `ds_start_datetime`
- `ds_end_datetime`

BigQuery table function 파라미터는 dbt vars와 별개로 정의한다.
현재 sales TVF는 기본적으로 `DS_START_DATE`, `DS_END_DATE`를 사용하고,
`analytics__profit_mom`은 `DS_INTERVAL_MONTH`를 추가로 받는다.

대표 예시는 다음과 같다.

```bash
dbt run --project-dir dbt_bigquery --selector sales \
  --vars '{"ds_start_date":"2026-06-01","ds_end_date":"2026-06-07"}'
```

datetime 기준 모델까지 함께 돌릴 때는 datetime 변수도 같이 넘긴다.

```bash
dbt run --project-dir dbt_bigquery --selector all_models \
  --vars '{"ds_start_date":"2026-06-01","ds_end_date":"2026-06-07","ds_start_datetime":"2026-06-01 00:00:00","ds_end_datetime":"2026-06-07 23:59:59"}'
```

## Selector

`selectors.yml`은 도메인별 실행 범위를 정의한다.

| Selector | 역할 |
| --- | --- |
| `all_models` | 전체 모델 선택 |
| `intermediate` | 전체 intermediate 모델 선택 |
| `marts` | 전체 mart 모델 선택 |
| `core` | 공통 상품/운영비 모델 선택 |
| `delivery` | 배송 helper 모델 선택 |
| `sales_intermediate` | 채널별 sales intermediate 선택 |
| `sales_marts` | 최종 sales mart 선택 |
| `sales` | 배송 helper + sales intermediate + sales mart 전체 선택 |
| `ads_intermediate` | 광고 intermediate 전체 선택 |
| `ads_marts` | 광고 table function 선택 |
| `ads` | 광고 intermediate + 광고 mart 전체 선택 |
| `searchad` | Searchad 관련 모델만 선택 |
| `google_ads` | Google Ads 관련 모델만 선택 |
| `meta_ads` | Meta Ads 관련 모델만 선택 |
| `coupang_ads` | Coupang Ads 관련 모델만 선택 |
| `sabangnet` | Sabangnet sales 모델만 선택 |
| `smartstore` | Smartstore sales 모델만 선택 |
| `coupang_rfm` | Coupang rocket settlement sales 모델만 선택 |

selector 확인 예시:

```bash
dbt ls --project-dir dbt_bigquery --selector sales
dbt ls --project-dir dbt_bigquery --selector ads
dbt ls --project-dir dbt_bigquery --selector coupang_rfm
```

## 실행 명령

컴파일만 확인할 때:

```bash
dbt compile --project-dir dbt_bigquery --no-populate-cache
```

sales 계열 모델을 실행할 때:

```bash
dbt run --project-dir dbt_bigquery --selector sales \
  --vars '{"ds_start_date":"2026-06-01","ds_end_date":"2026-06-07"}'
```

ads 계열 모델을 실행할 때:

```bash
dbt run --project-dir dbt_bigquery --selector ads \
  --vars '{"ds_start_date":"2026-06-01","ds_end_date":"2026-06-07"}'
```

특정 채널만 실행할 때:

```bash
dbt run --project-dir dbt_bigquery --selector coupang_rfm \
  --vars '{"ds_start_date":"2026-06-01","ds_end_date":"2026-06-07"}'
```

incremental 모델을 전체 재구축할 때:

```bash
dbt run --project-dir dbt_bigquery --full-refresh --selector sales \
  --vars '{"ds_start_date":"2026-06-01","ds_end_date":"2026-06-07"}'
```

운영 dataset에 반영할 때만 `--target prod`를 명시한다.

## 테스트

현재 `dbt_bigquery/tests/`에는 실행 중인 분석 테스트가 없다. `.gitkeep`만 유지하고 있다.

따라서 현재 검증은 보통 다음 방식으로 수행한다.

1. `dbt compile`로 SQL 컴파일 확인
2. 필요한 selector만 `dbt run`
3. 결과 table 또는 table function을 BigQuery에서 직접 조회

## dbt Docs

lineage graph와 모델 설명을 확인할 때는 docs artifact를 생성한 뒤 로컬 서버를 띄운다.

```bash
dbt docs generate --project-dir dbt_bigquery --empty-catalog --no-populate-cache
dbt docs serve --project-dir dbt_bigquery --port 8082
```

`--empty-catalog`는 catalog 조회 비용을 줄이고, lineage 확인에는 `source()`와 `ref()` 관계가 들어 있는 manifest만으로도 충분하다.

## 운영 참고사항

- BigQuery 원천 dataset을 직접 `source()`로 참조한다.
- 채널별 intermediate 모델을 거쳐 최종 mart와 table function을 만든다.
- 최종 보고 산출물 중 일부는 view나 table이 아니라 BigQuery table function이다.
- `selectors.yml`은 프로젝트 단위로 읽히므로, 저장소 루트에서 실행하더라도 `--project-dir dbt_bigquery`를 주면 이 프로젝트의 selector만 사용한다.
- `coupang_rfm` 계열은 원천의 주간 집계 특성을 고려해 주간 경계 기반 partition filter를 사용한다.
- BigQuery 비용을 줄이기 위해 전체 실행보다는 selector와 날짜 범위를 명시한 부분 실행을 기본 운영 방식으로 삼는다.
