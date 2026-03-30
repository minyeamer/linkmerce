# linkmerce 핵심 패키지

> PyPI 배포 패키지 — 이커머스 데이터 수집, 변환, 적재를 위한 ETL 프레임워크

---

## 목차

- [linkmerce 핵심 패키지](#linkmerce-핵심-패키지)
  - [목차](#목차)
  - [디렉토리 구조](#디렉토리-구조)
  - [common/ — 공통 베이스 클래스](#common--공통-베이스-클래스)
    - [Extractor (`extract.py`)](#extractor-extractpy)
    - [Transformer (`transform.py`)](#transformer-transformpy)
    - [DuckDBConnection (`load.py`)](#duckdbconnection-loadpy)
    - [Task 시스템 (`tasks.py`)](#task-시스템-taskspy)
    - [Models (`models.py`)](#models-modelspy)
  - [core/ — ETL 모듈](#core--etl-모듈)
    - [3파일 구조](#3파일-구조)
    - [모듈 목록](#모듈-목록)
  - [api/ — 통합 API](#api--통합-api)
    - [플랫폼별 API 함수](#플랫폼별-api-함수)
      - [쿠팡 (`coupang/`)](#쿠팡-coupang)
      - [스마트스토어 (`smartstore/`)](#스마트스토어-smartstore)
      - [검색광고 (`searchad/`)](#검색광고-searchad)
      - [네이버 (`naver/`)](#네이버-naver)
      - [기타 플랫폼](#기타-플랫폼)
    - [공통 파라미터](#공통-파라미터)
  - [extensions/ — 외부 연동](#extensions--외부-연동)
    - [BigQueryClient (`bigquery.py`)](#bigqueryclient-bigquerypy)
    - [WorksheetClient (`gsheets.py`)](#worksheetclient-gsheetspy)
  - [utils/ — 유틸리티](#utils--유틸리티)

---

## 디렉토리 구조

```
linkmerce/
├── __init__.py
├── common/                 # 공통 베이스 클래스
│   ├── extract.py          #   Extractor — HTTP 요청, 세션, 페이지네이션
│   ├── transform.py        #   Transformer — JSON/HTML/DB 데이터 변환
│   ├── load.py             #   Connection, DuckDBConnection — DB 커넥션 및 SQL
│   ├── tasks.py            #   Task 체계 — Request, ForEach, Paginate, Cursor
│   ├── models.py           #   Models — .sql 파일 파서
│   ├── api.py              #   API 런타임 — extract/transform 조합 실행
│   └── exceptions.py       #   예외 클래스
│
├── core/                   # ETL 모듈 (플랫폼별)
│   ├── cj/eflexs/          #   CJ대한통운 eFlexs
│   ├── coupang/             #   쿠팡 (advertising, wing)
│   ├── ecount/              #   이카운트 ERP
│   ├── google/              #   Google Ads
│   ├── meta/                #   Meta (Facebook/Instagram)
│   ├── naver/               #   네이버 (메인 검색, 오픈 API)
│   ├── sabangnet/           #   사방넷
│   ├── searchad/            #   네이버 검색광고
│   └── smartstore/          #   네이버 스마트스토어
│
├── api/                    # 통합 API (core를 조합한 실행 진입점)
│   ├── config.py           #   설정 로더 (schemas.json, config.yaml)
│   ├── cj/eflexs.py
│   ├── coupang/             #   advertising.py, wing.py
│   ├── ecount/api.py
│   ├── google/api.py
│   ├── meta/api.py
│   ├── naver/               #   main.py, openapi.py
│   ├── sabangnet/admin.py
│   ├── searchad/            #   api.py, gfa.py, manage.py
│   └── smartstore/          #   api.py, brand.py, center.py
│
├── extensions/             # 외부 서비스 연동
│   ├── bigquery.py         #   BigQueryClient — DuckDB → BigQuery 적재
│   └── gsheets.py          #   WorksheetClient — Google Sheets 연동
│
└── utils/                  # 유틸리티 함수
    ├── cast.py             #   안전한 형변환 (safe_float, safe_int)
    ├── date.py             #   날짜 유틸 (date_range, YearMonth)
    ├── excel.py            #   엑셀 생성 (조건부 서식, 피벗)
    ├── graphql.py          #   GraphQL 쿼리 빌더
    ├── headers.py          #   HTTP 헤더 빌더
    ├── jinja.py            #   Jinja2 템플릿 렌더링
    ├── map.py              #   중첩 딕셔너리 접근, 변환
    ├── parse.py            #   HTML 파싱 (BeautifulSoup)
    ├── progress.py         #   비동기 실행 + tqdm 진행률
    └── regex.py            #   정규식 유틸
```

---

## common/ — 공통 베이스 클래스

### Extractor (`extract.py`)

HTTP API 호출을 담당하는 추상 베이스 클래스. 동기/비동기 요청, 세션 관리, 재시도 로직을 제공한다.

**클래스 계층:**

```
Client
├── BaseSessionClient          # 세션, 헤더, 파라미터, 바디 관리
│   ├── RequestSessionClient   # requests 라이브러리 (동기)
│   ├── AiohttpSessionClient   # aiohttp 라이브러리 (비동기)
│   └── SessionClient          # 동기+비동기 통합
├── TaskClient                 # Task 팩토리 (request, for_each, paginate 등)
├── Extractor                  # SessionClient + TaskClient 조합
└── LoginHandler               # 로그인 전용 Extractor
```

**주요 기능:**
- **세션 관리**: `per_request`(매 요청마다 새 세션) 또는 공유 세션 모드
- **쿠키 관리**: 문자열 기반 쿠키 설정/추출
- **요청 빌더**: params, data, json, headers 자동 구성
- **응답 파싱**: JSON, HTML(BeautifulSoup), 바이너리 자동 처리
- **데코레이터**: `@with_session`, `@with_token` — 세션 및 인증 라이프사이클 관리

**TaskClient가 제공하는 실행 패턴:**

| 메서드 | Task 클래스 | 설명 |
|--------|-------------|------|
| `request()` | `Request` | 단일 요청 |
| `request_loop()` | `RequestLoop` | 조건 충족까지 재시도 |
| `for_each()` | `ForEach` | 배열 순회 실행 |
| `request_each()` | `RequestEach` | 배열 순회 + 요청 |
| `request_each_loop()` | `RequestEachLoop` | 배열 순회 + 재시도 |
| `paginate_all()` | `PaginateAll` | 전체 페이지 순회 |
| `request_each_pages()` | `RequestEachPages` | 배열 × 페이지 순회 |
| `cursor_all()` | `CursorAll` | 커서 기반 전체 순회 |
| `request_each_cursor()` | `RequestEachCursor` | 배열 × 커서 순회 |

---

### Transformer (`transform.py`)

추출된 데이터를 변환하는 추상 베이스 클래스.

**클래스 계층:**

```
Transformer
├── JsonTransformer        # JSON 응답 파싱 (경로 탐색, 유효성 검증)
├── HtmlTransformer        # HTML 파싱 (CSS 셀렉터, 매핑)
└── DBTransformer          # DB 기반 변환 (모델, 쿼리, 테이블 관리)
    └── DuckDBTransformer  # DuckDB 특화 (SQL 실행, 데이터 삽입)
```

**JsonTransformer** — API 응답에서 필요한 데이터를 추출:
- `path`: JSON 중첩 경로 탐색 (예: `["data", "items"]`)
- `dtype`: 기대 타입 검증 (`dict` 또는 `list`)
- `parse()`: 커스텀 파싱 로직 구현

**DuckDBTransformer** — SQL 기반 데이터 변환:
- `queries`: 사용할 SQL 쿼리 키 목록 (예: `["create", "select", "insert"]`)
- `set_tables()`: 논리 테이블명 → 물리 테이블명 매핑
- `set_models()`: `models.sql` 파일 로드
- `insert_into_table()`: JSON → DuckDB 테이블 삽입 (SELECT 변환 포함)
- `create_table()`: DDL 실행

---

### DuckDBConnection (`load.py`)

인메모리 DuckDB 커넥션을 관리하고 SQL을 실행하는 클래스. 모든 데이터 변환의 중간 저장소 역할을 한다.

**주요 메서드:**

| 카테고리 | 메서드 | 설명 |
|----------|--------|------|
| **실행** | `execute()`, `execute_many()` | SQL 실행 |
| **조회** | `fetch_all()`, `fetch_one()` | 결과 반환 (csv/json/parquet) |
| **테이블** | `create_table()`, `drop_table()`, `count_table()` | DDL 및 메타 |
| **삽입** | `insert_json()`, `insert_csv()`, `insert_parquet()` | 다양한 포맷 삽입 |
| **표현식** | `expr_cast()`, `expr_value()`, `expr_now()` | SQL 표현식 빌더 |
| **내보내기** | `to_csv()`, `to_json()`, `to_parquet()` | 파일 포맷 변환 |

**컨텍스트 매니저로 사용:**

```python
with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
    conn.execute("CREATE TABLE data AS SELECT 1 AS id")
    result = conn.fetch_all("json", "SELECT * FROM data")
    parquet_bytes = conn.to_parquet("data")
```

---

### Task 시스템 (`tasks.py`)

데이터 수집 작업의 **실행 패턴**을 추상화한 클래스 체계. `Extractor` 의 `TaskClient` 가 이 클래스들을 인스턴스화한다.

```
Task
├── Request                  # 단일 요청 + 파서
├── RunLoop                  # 조건 루프 + 지수 백오프 재시도
│   ├── RequestLoop          # RunLoop + Request
│   └── CursorAll            # 커서 기반 전체 순회
└── ForEach                  # 배열 순회 (동기/비동기, tqdm)
    ├── RequestEach          # ForEach + Request
    │   ├── RequestEachLoop  # RequestEach + 재시도
    │   ├── RequestEachPages # RequestEach + 페이지네이션
    │   └── RequestEachCursor# RequestEach + 커서
    └── PaginateAll          # 페이지 순회
```

**주요 기능:**
- **재시도**: 지수 백오프(incremental), 고정 딜레이, 랜덤 범위 지원
- **병렬 실행**: `max_concurrent` 파라미터로 비동기 동시성 제어
- **진행률**: tqdm 통합으로 진행 상황 실시간 표시
- **에러 핸들링**: `raise_errors`, `ignored_errors` 로 세밀한 제어

---

### Models (`models.py`)

`models.sql` 파일을 파싱하여 라벨별 SQL 쿼리를 딕셔너리로 반환하는 파서.

```sql
-- 파일: models.sql
-- Entity: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    id INTEGER PRIMARY KEY,
    name VARCHAR
);

-- Entity: select
SELECT id, name FROM {{ array }};

-- Entity: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
```

```python
models = Models("path/to/models.sql")
models.get("create")   # → "CREATE TABLE IF NOT EXISTS {{ table }} (...)"
models.get("select")   # → "SELECT id, name FROM {{ array }}"
```

---

## core/ — ETL 모듈

### 3파일 구조

모든 데이터 수집 작업은 동일한 패턴으로 구성된다:

```
core/{플랫폼}/{서비스}/{작업}/
├── extract.py      # Extractor 서브클래스 — API 호출, 파라미터, 페이지네이션
├── transform.py    # DuckDBTransformer 서브클래스 — JSON 파싱 + SQL 변환
└── models.sql      # SQL 모델 — CREATE TABLE, SELECT 변환, INSERT
```

**extract.py 패턴:**

```python
class MarketingChannelList(PartnerCenter):
    """스마트스토어 마케팅 채널 데이터 추출"""
    
    @PartnerCenter.with_session
    def extract(self, channel_seq, start_date, end_date, **kwargs):
        return self.paginate_all(
            func=self.request_json,
            page_size=500,
            max_page_size=500,
        ).run()
```

**transform.py 패턴:**

```python
class MarketingChannel(DuckDBTransformer):
    queries = ["create", "select", "insert"]
    
    def set_tables(self, tables=None):
        base = dict(data="data")
        super().set_tables(dict(base, **(tables or {})))
    
    def transform(self, obj, **kwargs):
        items = MarketingChannelItem().transform(obj)
        if items:
            return self.insert_into_table(items, key="insert", ...)
```

**models.sql 패턴:**

```sql
-- MarketingChannel: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    channel_seq VARCHAR,
    date DATE,
    source VARCHAR,
    visits INTEGER
);

-- MarketingChannelItem: select
SELECT
    $channel_seq AS channel_seq,
    TRY_CAST(date AS DATE) AS date,
    source,
    TRY_CAST(visits AS INTEGER) AS visits
FROM {{ array }};

-- MarketingChannel: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
```

**SQL 템플릿 문법:**

| 문법 | 설명 |
|------|------|
| `{{ table }}` | 대상 테이블명 (`set_tables()`로 설정) |
| `{{ array }}` | 입력 JSON 배열 |
| `{{ values }}` | SELECT 쿼리 결과 |
| `$param_name` | 런타임 파라미터 치환 |

---

### 모듈 목록

총 **27개 ETL 모듈**:

| 플랫폼 | 서비스 | 모듈 | 수집 데이터 |
|--------|--------|------|------------|
| **CJ대한통운** | eflexs | `stock/` | 재고 현황 |
| **쿠팡** | advertising | `adreport/` | 광고 성과 리포트 (PA, NCA) |
| | wing | `product/` | 상품 옵션, 재고 |
| | wing | `settlement/` | 로켓 정산 (매출, 배송) |
| **이카운트** | api | `inventory/` | ERP 재고 |
| | api | `product/` | ERP 상품 마스터 |
| **Google** | api | `ads/` | 캠페인, 광고그룹, 광고, 에셋, 인사이트 |
| **Meta** | api | `ads/` | 캠페인, 광고세트, 광고, 인사이트 |
| **네이버** | main | `search/` | 메인 검색 결과, 섹션별 순위 |
| | openapi | `search/` | 쇼핑 검색, 블로그, 뉴스, 카페 |
| **사방넷** | admin | `order/` | 주문, 배송, 송장 |
| | admin | `product/` | 상품, 옵션, 매핑 |
| **검색광고** | api | `adreport/` | 광고 성과 리포트 |
| | api | `contract/` | 시간계약, 브랜드뉴 계약 |
| | api | `keyword/` | 키워드 순위 |
| | manage | `adreport/` | 관리 리포트 |
| | manage | `exposure/` | 노출 진단, 순위 |
| | gfa | `adreport/` | GFA 광고 리포트 |
| **스마트스토어** | api | `bizdata/` | 마케팅 채널 |
| | api | `order/` | 주문, 상품주문, 배송, 클레임 |
| | api | `product/` | 상품, 옵션 |
| | brand | `catalog/` | 브랜드 카탈로그 |
| | brand | `pageview/` | 페이지뷰 (기기별, URL별, 상품별) |
| | brand | `sales/` | 매출 (스토어, 카테고리, 상품) |

---

## api/ — 통합 API

`core/`의 Extract + Transform을 조합하여 하나의 함수로 노출하는 진입점.

### 플랫폼별 API 함수

#### 쿠팡 (`coupang/`)

**advertising.py** — 광고 플랫폼:
| 함수 | 파라미터 | 설명 |
|------|----------|------|
| `login()` | userid, passwd, domain | 로그인 (쿠키 반환) |
| `campaign()` | cookies, goal_type, vendor_id | 캠페인, 광고그룹 계층 |
| `creative()` | cookies, campaign_ids | 크리에이티브 데이터 |
| `product_adreport()` | cookies, start_date, end_date | PA 광고 성과 |
| `new_customer_adreport()` | cookies, start_date, end_date | NCA 광고 성과 |

**wing.py** — 커머스 플랫폼:
| 함수 | 파라미터 | 설명 |
|------|----------|------|
| `login()` | userid, passwd, domain | 로그인 |
| `product_option()` | cookies, is_deleted | 전체 상품 옵션 |
| `product_detail()` | cookies, vendor_inventory_id | 개별 상품 상세 |
| `product_download()` | cookies, request_type, fields | 상품 대량 다운로드 |
| `rocket_inventory()` | cookies, vendor_id | 로켓 재고 |
| `rocket_settlement()` | cookies, start_date, end_date, date_type | 정산 데이터 |
| `rocket_settlement_download()` | cookies, start_date, end_date | 정산 파일 다운로드 |

#### 스마트스토어 (`smartstore/`)

**api.py** — Commerce API:
| 함수 | 파라미터 | 설명 |
|------|----------|------|
| `product()` | client_id, client_secret, search_keyword | 상품 목록 |
| `option()` | client_id, client_secret, product_id | 상품 옵션 |
| `product_option()` | client_id, client_secret, connection | 상품+옵션 통합 |
| `order()` | client_id, client_secret, start_date, end_date | 주문 상세 |
| `order_status()` | client_id, client_secret, start_date, end_date | 주문 상태 이력 |
| `marketing_channel()` | client_id, client_secret, channel_seq | 마케팅 채널 |

**brand.py** — 브랜드센터:
| 함수 | 파라미터 | 설명 |
|------|----------|------|
| `login()` | userid, passwd, channel_seq | 로그인 |
| `brand_catalog()` | cookies, brand_ids | 브랜드 카탈로그 |
| `brand_price()` | cookies, brand_ids, mall_seq | 가격 + 상품 |
| `product_catalog()` | cookies, brand_ids, mall_seq | 카탈로그 상품 |
| `page_view()` | cookies, aggregate_by, mall_seq | 페이지뷰 |
| `store_sales()` | cookies, mall_seq, start_date, end_date | 스토어 매출 |
| `category_sales()` | cookies, mall_seq | 카테고리별 매출 |
| `product_sales()` | cookies, mall_seq | 상품별 매출 |
| `aggregated_sales()` | cookies, mall_seq | 통합 매출 |

#### 검색광고 (`searchad/`)

**api.py** — 공식 API:
| 함수 | 파라미터 | 설명 |
|------|----------|------|
| `campaign()` | api_key, secret_key, customer_id | 캠페인 마스터 |
| `adgroup()` | api_key, secret_key, customer_id | 광고그룹 |
| `ad()` | api_key, secret_key, customer_id | 광고 |
| `contract()` | api_key, secret_key, customer_id | 계약 정보 |
| `keyword()` | api_key, secret_key, customer_id, keywords | 키워드 순위 |

**manage.py** — 관리 패널:
| 함수 | 파라미터 | 설명 |
|------|----------|------|
| `adreport()` | customer_id, cookies, report_id | 커스텀 리포트 |
| `daily_report()` | customer_id, cookies, report_id | 일별 성과 |
| `diagnose_exposure()` | customer_id, cookies, keyword | 노출 진단 |
| `rank_exposure()` | customer_id, cookies, keyword | 순위 분석 |

**gfa.py** — GFA 패널:
| 함수 | 파라미터 | 설명 |
|------|----------|------|
| `campaign()` | account_no, cookies | 캠페인 |
| `adset()` | account_no, cookies | 광고세트 |
| `creative()` | account_no, cookies | 크리에이티브 |
| `campaign_report()` | account_no, cookies, start_date, end_date | 캠페인 리포트 |
| `creative_report()` | account_no, cookies, start_date, end_date | 크리에이티브 리포트 |

#### 네이버 (`naver/`)

**main.py** — 메인 검색 (크롤링):
| 함수 | 파라미터 | 설명 |
|------|----------|------|
| `search()` | query, mobile, cookies | 통합 검색 결과 |
| `search_cafe()` | query, mobile | 카페 검색 |
| `cafe_article()` | url, domain | 카페 글 내용 |
| `search_cafe_plus()` | connection, query | 카페 검색 + 글 수집 |

**openapi.py** — 공식 오픈 API:
| 함수 | 파라미터 | 설명 |
|------|----------|------|
| `search_blog()` | client_id, client_secret, query | 블로그 검색 |
| `search_news()` | client_id, client_secret, query | 뉴스 검색 |
| `search_shop()` | client_id, client_secret, query | 쇼핑 검색 |
| `rank_shop()` | client_id, client_secret, query | 쇼핑 순위 |
| `search_cafe()` | client_id, client_secret, query | 카페 검색 |

#### 기타 플랫폼

**사방넷** (`sabangnet/admin.py`):
- `login()`, `order()`, `order_download()`, `order_status()`, `product()`, `option()`, `product_mapping()`, `sku_mapping()`, `option_mapping()`

**CJ대한통운** (`cj/eflexs.py`):
- `stock()` — eFlexs 재고 현황

**이카운트** (`ecount/api.py`):
- `product()`, `inventory()` — ERP 상품/재고

**Google Ads** (`google/api.py`):
- `campaign()`, `adgroup()`, `ad()`, `insight()`, `asset()`, `asset_view()`

**Meta Ads** (`meta/api.py`):
- `campaigns()`, `adsets()`, `ads()`, `insights()`

---

### 공통 파라미터

모든 API 함수에서 사용 가능한 공통 파라미터:

| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| `connection` | `DuckDBConnection` | `None` | 전달 시 DuckDB에 결과 저장 |
| `tables` | `dict` | - | 논리 테이블명 → 물리 테이블명 매핑 |
| `return_type` | `"csv" \| "json" \| "parquet" \| "raw" \| "none"` | `"json"` | 반환 포맷 |
| `progress` | `bool` | `True` | tqdm 진행률 표시 |
| `request_delay` | `float` | `0` | 요청 간 지연 (초) |
| `max_concurrent` | `int` | `None` | 비동기 최대 동시성 |
| `extract_options` | `dict` | `{}` | Extractor 추가 설정 |
| `transform_options` | `dict` | `{}` | Transformer 추가 설정 |

---

## extensions/ — 외부 연동

### BigQueryClient (`bigquery.py`)

DuckDB에서 변환된 데이터를 Google BigQuery에 적재하는 클라이언트.

**적재 전략:**

| 메서드 | 전략 | 설명 |
|--------|------|------|
| `load_table_from_duckdb()` | append / empty / truncate | 단순 적재 (append), 비어있을 때만 (empty), 전체 교체 (truncate) |
| `merge_into_table_from_duckdb()` | merge | MERGE 문 — 키 매칭 시 UPDATE, 미매칭 시 INSERT |
| `overwrite_table_from_duckdb()` | overwrite | WHERE 조건 기반 삭제 후 삽입 |
| `copy_table()` | copy | DuckDB 테이블을 그대로 복사 |

**주요 기능:**
- 날짜 범위 기반 파티션 적재
- 동시 업데이트 충돌 자동 재시도 (5회, 랜덤 백오프)
- `schemas.json` 기반 스키마 자동 적용
- 서비스 계정 인증 (JSON 문자열/파일/딕셔너리)

### WorksheetClient (`gsheets.py`)

Google Sheets API를 통한 데이터 읽기/쓰기.

| 메서드 | 설명 |
|--------|------|
| `get_all_records()` | 시트 전체 읽기 (날짜/불리언/퍼센트 자동 변환) |
| `update_worksheet()` | 특정 셀 범위 업데이트 |
| `overwrite_worksheet()` | 시트 덮어쓰기 (헤더 매칭) |
| `upsert_worksheet()` | 키 기반 업서트 |

---

## utils/ — 유틸리티

| 모듈 | 주요 함수/클래스 | 설명 |
|------|------------------|------|
| `cast.py` | `safe_float()`, `safe_int()` | 쉼표 포함 문자열 안전 변환 |
| `date.py` | `date_range()`, `date_split()`, `YearMonth` | 날짜 범위 생성, 분할, 연월 클래스 |
| `excel.py` | `csv2excel()`, `json2excel()` | 엑셀 파일 생성 (조건부 서식, 필터, 병합 셀) |
| `graphql.py` | `GraphQLOperation`, `GraphQLSelection` | GraphQL 쿼리 빌더 |
| `headers.py` | `build_headers()` | Chrome UA 기반 HTTP 헤더 생성 |
| `jinja.py` | `render_string()` | Jinja2 템플릿 문자열 렌더링 |
| `map.py` | `hier_get()`, `list_apply()`, `to_csv()` | 중첩 딕셔너리 접근, 리스트 변환 |
| `parse.py` | `clean_html()`, `hier_select()` | HTML 정제, 계층적 선택자 |
| `progress.py` | `gather()`, `gather_async()` | 비동기 실행 + tqdm 진행률 |
| `regex.py` | `regexp_match()`, `regexp_extract()` | 정규식 매칭/추출 |
