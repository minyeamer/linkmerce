# PostgreSQL 파티셔닝 및 클라이언트 모듈 설계 문서

> **대상 버전**: PostgreSQL 13  
> **프로젝트**: linkmerce (BigQuery → PostgreSQL 마이그레이션)  
> **작성일**: 2026-07

---

## 목차

1. [배경 및 동기](#1-배경-및-동기)
2. [BRIN 인덱스의 한계](#2-brin-인덱스의-한계)
3. [선언적 파티셔닝 설계](#3-선언적-파티셔닝-설계)
4. [PostgreSQL 클라이언트 모듈](#4-postgresql-클라이언트-모듈)
5. [BigQuery ↔ PostgreSQL 매핑 테이블](#5-bigquery--postgresql-매핑-테이블)
6. [참고 링크](#6-참고-링크)

---

## 1. 배경 및 동기

BigQuery의 원천 테이블을 PostgreSQL로 복제하는 과정에서, 초기에는 **BRIN 인덱스**로 BigQuery의 파티션 프루닝을 대체하려 했다. 그러나 BRIN은 블록 범위 요약 기반의 **손실(lossy) 인덱스**로, BigQuery의 **결정론적(deterministic) 파티션 프루닝**과 동일한 수준의 성능을 보장하지 못한다.

### 핵심 차이

| 특성 | BigQuery 파티션 | BRIN 인덱스 | PG 선언적 파티션 |
|------|----------------|-------------|-----------------|
| 데이터 격리 | 물리적 분리 | 없음 (동일 힙) | 물리적 분리 |
| 프루닝 방식 | 정확(exact) | 근사(lossy) | 정확(exact) |
| INSERT 시 동작 | 자동 라우팅 | 인덱스 갱신 | 자동 라우팅 |
| DELETE/TRUNCATE | 파티션 단위 가능 | 불가 | 파티션 단위 가능 |
| MERGE/UPSERT | 네이티브 MERGE | 무관 | INSERT ON CONFLICT |

이러한 분석을 바탕으로, **BRIN 단독 전략을 폐기**하고 PostgreSQL의 **선언적 파티셔닝(Declarative Partitioning)**으로 전환하였다.

---

## 2. BRIN 인덱스의 한계

BRIN(Block Range Index)은 물리적으로 정렬된 대용량 테이블에서 범위 검색을 최적화하는 인덱스이다. 그러나 다음 한계가 있다:

1. **손실 필터링**: BRIN은 블록 범위의 min/max 값만 저장한다. 특정 날짜를 조회할 때 해당 블록 전체를 스캔해야 하며, False Positive가 발생한다.
2. **물리 정렬 의존성**: INSERT 순서가 날짜 순서와 불일치하면 BRIN 효율이 급격히 저하된다.
3. **파티션 관리 불가**: BRIN은 인덱스일 뿐, 파티션 단위 TRUNCATE/DROP, 파티션별 통계 수집 등이 불가능하다.
4. **INSERT ON CONFLICT(UPSERT)와의 비호환**: BRIN은 UPSERT 워크로드에서 추가적인 이점을 제공하지 않는다.

---

## 3. 선언적 파티셔닝 설계

### 3.1 전략 개요

- **BigQuery DATE 파티션** → PostgreSQL `PARTITION BY RANGE (date_column)`
- **BigQuery DATE(TIMESTAMP) 파티션** → PostgreSQL `PARTITION BY RANGE (timestamp_column)`
  - TIMESTAMP 칼럼에 직접 RANGE 파티셔닝 적용 (월별 범위)
  - `WHERE ts >= '2025-01-01' AND ts < '2025-02-01'` 형태의 조건으로 파티션 프루닝 작동
- **BigQuery 클러스터링** → PostgreSQL B-tree 인덱스
- **PK 규칙**: 파티셔닝된 테이블의 PK는 반드시 파티션 키를 포함해야 한다 (PostgreSQL 제약)

### 3.2 파티션 관리

`init.sql`은 **부모 테이블(파티션 정의)**만 생성한다. 자식 파티션(월별)은 `PostgresClient.ensure_monthly_partitions()`를 통해 ETL 파이프라인에서 동적으로 생성한다.

```sql
-- 부모 테이블 (init.sql)
CREATE TABLE smartstore.order (
    order_id BIGINT NOT NULL,
    channel_seq BIGINT NOT NULL,
    payment_dt TIMESTAMP NOT NULL,
    PRIMARY KEY (payment_dt, order_id)
) PARTITION BY RANGE (payment_dt);

-- 자식 파티션 (PostgresClient가 자동 생성)
CREATE TABLE smartstore.order_y2025m01
    PARTITION OF smartstore.order
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
```

```python
# Python에서의 파티션 생성
pg = PostgresClient(dsn="postgresql://...")
pg.ensure_monthly_partitions("smartstore.order", "2024-01-01", "2026-12-31")
```

### 3.3 파티셔닝 적용 테이블 목록

#### DATE 칼럼 파티션 (직접 RANGE)

| PG 테이블 | 파티션 키 | BigQuery 원본 |
|-----------|----------|--------------|
| `cj_eflexs.invoice` | `pickup_date` | `cjlogistics.eflexs_invoice` |
| `cj_eflexs.invoice_order` | `order_date` | `cjlogistics.eflexs_order` |
| `cj_loisparcel.invoice` | `register_date` | `cjlogistics.loisparcel` |
| `coupang_ads.adreport_pa` | `ymd` | `coupang.adreport_pa` |
| `coupang_ads.adreport_nca` | `ymd` | `coupang.adreport_nca` |
| `coupang.rocket_sales` | `sales_date` | `coupang.rocket_sales` |
| `coupang.rocket_shipping` | `sales_date` | `coupang.rocket_shipping` |
| `ecount.cost` | `end_date` | `ecount.cost` |
| `google.insight` | `ymd` | `google.insight` |
| `meta.insights` | `ymd` | `meta.insights` |
| `sabangnet.order_status` | `order_date` | `sabangnet.order_status` |
| `searchad.report` | `ymd` | `searchad.report` |
| `searchad.contract` | `contract_end_date` | `searchad.contract` |
| `searchad_gfa.report` | `ymd` | `searchad.report_gfa` |
| `ss_hcenter.pageview` | `ymd` | `nscenter.pageview` |
| `ss_hcenter.sales` | `payment_date` | `nscenter.sales` |
| `ss_bizdata.marketing_channel` | `ymd` | `smt_bizdata.marketing_channel` |

#### TIMESTAMP 칼럼 파티션 (RANGE with 월별 범위)

| PG 테이블 | 파티션 키 | BigQuery 원본 |
|-----------|----------|--------------|
| `sabangnet.order` | `order_dt` | `sabangnet.order` |
| `sabangnet.order_invoice` | `order_dt` | `sabangnet.order_invoice` |
| `sabangnet.order_dispatch` | `register_dt` | `sabangnet.order_dispatch` |
| `searchad.rank` | `created_at` | `searchad.rank` |
| `naver.rank` | `created_at` | `nshopping.rank` |
| `smartstore.order` | `payment_dt` | `smartstore.order` |
| `smartstore.order_delivery` | `payment_dt` | `smartstore.order_delivery` |
| `smartstore.order_detail` | `payment_dt` | `smartstore.order_detail` |
| `smartstore.order_status` | `payment_dt` | `smartstore.order_status` |
| `ss_hcenter.pid_cid` | `created_at` | `nscenter.pid_cid` |
| `ss_hcenter.stock` | `created_at` | `nscenter.stock` |
| `ss_hcenter.price` | `created_at` | `nscenter.price` |

#### PK 변경 사항 (파티션 키 포함 필수)

다음 테이블은 기존 PK에 파티션 키가 포함되어 있지 않아 PK를 변경하였다:

| 테이블 | 기존 PK | 변경 후 PK |
|--------|---------|-----------|
| `cj_eflexs.invoice` | `(invoice_no)` | `(pickup_date, invoice_no)` |
| `cj_eflexs.invoice_order` | `(invoice_no, order_id)` | `(order_date, invoice_no, order_id)` |
| `cj_loisparcel.invoice` | `(invoice_no)` | `(register_date, invoice_no)` |
| `ecount.cost` | `(id)` | `(end_date, id)` |
| `searchad.contract` | `(contract_id)` | `(contract_end_date, contract_id)` |
| `sabangnet.order` | `(order_seq)` | `(order_dt, order_seq)` |
| `sabangnet.order_invoice` | `(order_seq)` | `(order_dt, order_seq)` |
| `sabangnet.order_dispatch` | `(order_seq)` | `(register_dt, order_seq)` |
| `ss_hcenter.pid_cid` | `(product_id)` | `(created_at, product_id)` |
| `ss_hcenter.price` | `(product_id)` | `(created_at, product_id)` |
| `ss_hcenter.stock` | `(product_id, created_at)` | `(created_at, product_id)` |

---

## 4. PostgreSQL 클라이언트 모듈

### 4.1 개요

`extensions/postgres.py`의 `PostgresClient`는 `Connection` 추상 클래스를 상속하며, `BigQueryClient`와 동등한 인터페이스를 제공한다. PostgreSQL 13의 제약에 맞춰 `MERGE` 대신 `INSERT ... ON CONFLICT`(UPSERT)를 사용한다.

### 4.2 BigQuery → PostgreSQL 메서드 매핑

| BigQuery (`BigQueryClient`) | PostgreSQL (`PostgresClient`) | 비고 |
|----------------------------|------------------------------|------|
| `execute()` | `execute()` | 자동 커밋 옵션 추가 |
| `fetch_all_to_csv()` | `fetch_all_to_csv()` | 동일 |
| `fetch_all_to_json()` | `fetch_all_to_json()` | 동일 |
| `table_exists()` | `table_exists()` | `information_schema` 조회 |
| `table_has_rows()` | `table_has_rows()` | 동일 |
| `get_columns()` | `get_columns()` | GENERATED 칼럼 제외 |
| `get_schema()` | `get_primary_key()` | PG 고유: PK 칼럼 조회 |
| `load_table_from_json()` | `insert_rows()` | `execute_batch` 사용 |
| `load_table_from_file()` | `copy_from_csv()` | `COPY FROM STDIN` |
| — | `copy_to_csv()` | `COPY TO STDOUT` |
| `merge_into_table()` | `upsert()` | `INSERT ON CONFLICT` |
| `merge_into_table()` (table→table) | `upsert_from_staging()` | 스테이징→타겟 UPSERT |
| `overwrite_table_from_duckdb()` | `overwrite_from_duckdb()` | DELETE + COPY (트랜잭션) |
| `load_table_from_duckdb()` | `load_from_duckdb()` | DuckDB→CSV→COPY |
| `merge_into_table_from_duckdb()` | `upsert_from_duckdb()` | 스테이징 테이블 패턴 |
| — | `ensure_partition()` | 파티션 관리 (PG 고유) |
| — | `ensure_monthly_partitions()` | 기간별 파티션 일괄 생성 |
| — | `get_partitions()` | 파티션 목록 조회 |
| — | `is_partitioned()` | 파티셔닝 여부 확인 |
| `expr_now()` | `expr_now()` | `CURRENT_TIMESTAMP` / `TO_CHAR` |
| `expr_today()` | `expr_today()` | `CURRENT_DATE` / `TO_CHAR` |

### 4.3 UPSERT (`INSERT ON CONFLICT`) 상세

PostgreSQL 13은 SQL 표준 `MERGE` 문을 지원하지 않는다 (PostgreSQL 15에서 추가). 대신 `INSERT ... ON CONFLICT ... DO UPDATE SET` 패턴을 사용한다.

#### matched 파라미터 매핑

BigQuery의 `_merge_update`에서 사용하는 `matched` 딕셔너리가 PostgreSQL의 `ON CONFLICT DO UPDATE SET`으로 변환된다:

| matched 값 | BigQuery 표현 | PostgreSQL 표현 |
|-----------|--------------|----------------|
| `":replace_all:"` | `T.col = S.col` (전체) | `col = EXCLUDED.col` (전체) |
| `":do_nothing:"` | WHEN MATCHED 생략 | `DO NOTHING` |
| `"replace"` | `T.col = S.col` | `col = EXCLUDED.col` |
| `"ignore"` | `T.col = T.col` | `col = table.col` |
| `"greatest"` | `GREATEST(S.col, T.col)` | `GREATEST(EXCLUDED.col, table.col)` |
| `"least"` | `LEAST(S.col, T.col)` | `LEAST(EXCLUDED.col, table.col)` |
| `"source_first"` | `COALESCE(S.col, T.col)` | `COALESCE(EXCLUDED.col, table.col)` |
| `"target_first"` | `COALESCE(T.col, S.col)` | `COALESCE(table.col, EXCLUDED.col)` |

#### 사용 예시

```python
pg = PostgresClient(dsn="postgresql://airflow:airflow@localhost:5431/airflow")

# 전체 교체 (기본)
pg.upsert("searchad.report", data, conflict_columns=["ymd", "customer_id", "ad_id", "media_code", "pc_mobile_type"])

# 충돌 시 무시
pg.upsert("naver.keyword", data, conflict_columns=["keyword", "keyword_group"], matched=":do_nothing:")

# 칼럼별 전략
pg.upsert("ss_hcenter.sales", data,
    conflict_columns=["payment_date", "product_id"],
    matched={
        "click_count": "greatest",       # 더 큰 값 유지
        "payment_count": "replace",       # 새 값으로 교체
        "payment_amount": "replace",
    })
```

### 4.4 DuckDB → PostgreSQL 적재 패턴

DuckDB에서 PostgreSQL로 데이터를 적재할 때, DuckDB의 DataFrame을 CSV로 변환 후 `COPY FROM STDIN`을 사용한다:

```
DuckDB Table → pandas DataFrame → CSV StringIO → COPY FROM STDIN → PG Table
```

UPSERT가 필요한 경우 스테이징 패턴을 사용한다:

```
DuckDB Table → COPY → PG Temp Table (staging)
                            ↓
              INSERT INTO target SELECT FROM staging
              ON CONFLICT (...) DO UPDATE SET ...
```

---

## 5. BigQuery ↔ PostgreSQL 매핑 테이블

### 데이터셋 매핑

| BigQuery 데이터셋 | PostgreSQL 스키마 |
|------------------|------------------|
| `cjlogistics` | `cj_eflexs`, `cj_loisparcel` |
| `coupang` | `coupang`, `coupang_ads` |
| `ecount` | `ecount` |
| `google` | `google` |
| `meta` | `meta` |
| `nscenter` | `ss_hcenter` |
| `nshopping` | `naver` |
| `sabangnet` | `sabangnet` |
| `searchad` | `searchad`, `searchad_gfa` |
| `smartstore` | `smartstore` |
| `smt_bizdata` | `ss_bizdata` |
| `relation` | `relation` |

---

## 6. 참고 링크

- [PostgreSQL 13: Table Partitioning](https://www.postgresql.org/docs/13/ddl-partitioning.html) — 선언적 파티셔닝 공식 문서
- [PostgreSQL 13: INSERT ON CONFLICT](https://www.postgresql.org/docs/13/sql-insert.html#SQL-ON-CONFLICT) — UPSERT 구문
- [PostgreSQL 13: BRIN Indexes](https://www.postgresql.org/docs/13/brin-intro.html) — BRIN 인덱스 동작 원리
- [PostgreSQL 15: MERGE Statement](https://www.postgresql.org/docs/15/sql-merge.html) — PG 15부터 지원되는 MERGE (참고)
- [psycopg2: COPY](https://www.psycopg.org/docs/cursor.html#cursor.copy_expert) — `copy_expert` API
- [DuckDB: PostgreSQL Extension](https://duckdb.org/docs/extensions/postgres.html) — DuckDB↔PG 직접 연결 (향후 활용 가능)
