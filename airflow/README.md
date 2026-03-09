# Airflow 워크플로우

> Apache Airflow 3.1.3 기반 이커머스 데이터 ETL 스케줄링

---

## 목차

- [Airflow 워크플로우](#airflow-워크플로우)
  - [목차](#목차)
  - [개요](#개요)
  - [구조](#구조)
  - [실행](#실행)
  - [DAG 아키텍처 패턴](#dag-아키텍처-패턴)
    - [표준 패턴 — Partial + Expand](#표준-패턴--partial--expand)
    - [고급 패턴](#고급-패턴)
  - [DAG 목록](#dag-목록)
    - [인증](#인증)
    - [쿠팡](#쿠팡)
    - [네이버 검색/순위](#네이버-검색순위)
    - [스마트스토어](#스마트스토어)
    - [검색광고](#검색광고)
    - [사방넷](#사방넷)
    - [Google Ads / Meta Ads](#google-ads--meta-ads)
    - [CJ대한통운 / 이카운트](#cj대한통운--이카운트)
  - [스케줄 타임라인](#스케줄-타임라인)
  - [플러그인](#플러그인)
    - [`variables.py`](#variablespy)
  - [동시성 제어 (Pool)](#동시성-제어-pool)
  - [환경설정](#환경설정)
    - [Docker Compose](#docker-compose)
    - [추가 패키지](#추가-패키지)
    - [BigQuery 적재 전략](#bigquery-적재-전략)

---

## 개요

27개 DAG가 이커머스 플랫폼의 데이터를 주기적으로 수집하여 Google BigQuery에 적재한다.

**실행 흐름:**

```
[Airflow Scheduler]
        │
        ▼
┌──────────────────┐     ┌────────────────────┐     ┌──────────────────┐
│  read_variables  │     │ read_credentials   │     │  (Airflow Pool)  │
│  (config.yaml)   │     │ (credentials.yaml) │     │  동시성 제어        │
└───────┬──────────┘     └───────┬────────────┘     └──────────────────┘
        │                        │
        ▼                        ▼
┌───────────────────────────────────────────────┐
│  ETL Task (.partial().expand())               │
│                                               │
│  ┌─────────┐  ┌───────────┐  ┌────────────┐   │
│  │ Extract │→ │ DuckDB    │→ │ BigQuery   │   │
│  │ (API)   │  │ Transform │  │ Load       │   │
│  └─────────┘  └───────────┘  └────────────┘   │
│                                               │
│  return {params, counts, status}              │
└───────────────────────────────────────────────┘
```

---

## 구조

```
airflow/
├── dags/                   # DAG 정의 파일 (27개)
│   ├── all_login.py        #   통합 로그인
│   ├── coupang_*.py        #   쿠팡 (4개)
│   ├── naver_*.py          #   네이버 (8개)
│   ├── smartstore_*.py     #   스마트스토어 (4개)
│   ├── searchad_*.py       #   검색광고 (3개)
│   ├── sabangnet_*.py      #   사방넷 (3개)
│   ├── google_ads.py       #   Google Ads
│   ├── meta_ads.py         #   Meta Ads
│   ├── ecount_stock_report.py  # 이카운트 재고
│   └── cj_loisparcel_invoice.py # CJ 송장
│
├── plugins/                # Airflow 플러그인
│   ├── variables.py        #   변수, 인증, 설정 로더
│   └── linkmerce/          #   패키지 심볼릭 링크
│
├── config/
│   └── airflow.cfg         # Airflow 설정
│
├── files/env/              # 환경 파일 (마운트)
├── logs/                   # 실행 로그
│
├── docker-compose.yaml     # Docker Compose 정의
├── exec_api.sh             # API 컨테이너 접속
├── exec_db.sh              # DB 컨테이너 접속
└── init.sh                 # 초기 설정 스크립트
```

---

## 실행

```bash
cd airflow
docker compose up airflow-init && docker compose up -d
```

**개별 DAG 테스트:**

```bash
# API 컨테이너 접속
./exec_api.sh

# DAG 트리거
airflow dags trigger ${dag_id}
```

---

## DAG 아키텍처 패턴

### 표준 패턴 — Partial + Expand

대부분의 DAG가 따르는 패턴. 인증 정보별로 Dynamic Task Mapping을 활용해 병렬 실행한다.

```python
with DAG(dag_id="smartstore_bizdata", schedule="10 8 * * *", ...):

    PATH = ["smartstore", "api", "smartstore_bizdata"]

    @task
    def read_variables() -> dict:
        from variables import read
        return read(PATH, tables=True, service_account=True)

    @task
    def read_credentials() -> list:
        from variables import read
        return read(PATH, credentials=True)["credentials"]

    @task(map_index_template="{{ credentials['channel_seq'] }}")
    def etl_task(credentials: dict, variables: dict, **kwargs) -> dict:
        from variables import get_execution_date
        date = get_execution_date(kwargs, subdays=1)
        return main(**credentials, date=date, **variables)

    def main(client_id, client_secret, channel_seq, date, service_account, tables, **kwargs):
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.smartstore.api import marketing_channel
        from linkmerce.extensions.bigquery import BigQueryClient

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            marketing_channel(
                client_id=client_id, client_secret=client_secret,
                channel_seq=channel_seq, start_date=date, end_date=date,
                connection=conn, progress=False, return_type="none",
            )
            with BigQueryClient(service_account) as client:
                return dict(
                    params=dict(channel_seq=channel_seq, date=date),
                    counts=dict(data=conn.count_table("data")),
                    status=dict(data=client.load_table_from_duckdb(
                        connection=conn, source_table="data",
                        target_table=tables["marketing_channel"],
                    )),
                )

    etl_task.partial(variables=read_variables()).expand(credentials=read_credentials())
```

### 고급 패턴

| 패턴 | 사용 DAG | 설명 |
|------|----------|------|
| **TaskGroup** | `ecount_stock_report` | 복합 워크플로우를 그룹으로 조직화 (CJ → 쿠팡 → 이카운트 → 리포트) |
| **BranchPythonOperator** | `sabangnet_order`, `naver_*_search` | 조건 분기 실행 |
| **DAG 트리거** | `naver_rank_shop` → `naver_product_catalog` | DAG 간 체이닝 |
| **MultipleCronTriggerTimetable** | `smartstore_invoice`, `sabangnet_invoice` | 하루 여러 번 실행 |
| **Slack 알림** | `naver_cafe_search`, `ecount_stock_report` | 엑셀 리포트 Slack 파일 전송 |

---

## DAG 목록

### 인증

| DAG | 스케줄 | 설명 |
|-----|--------|------|
| `all_login` | 매일 01:00 | 네이버 쇼핑 파트너센터 + 검색광고 로그인 쿠키 갱신 |
| `coupang_login` | - | 크론탭으로 이관됨 (05:41 광고, 09:01 윙) |

### 쿠팡

| DAG | 스케줄 | 설명 |
|-----|--------|------|
| `coupang_adreport` | 매일 05:50 | 벤더별 PA/NCA 광고 보고서 → `merge` |
| `coupang_campaign` | 평일 05:55 | 캠페인, 광고그룹, 크리에이티브 마스터 데이터 → `load` |
| `coupang_option` | 매일 09:20 | 상품 옵션 전체 → `overwrite` |
| `coupang_rocket_sales` | 매일 09:10 | 주간 정산 (매출, 배송), 월-일 집계 → `merge` |

### 네이버 검색/순위

| DAG | 스케줄 | 설명 |
|-----|--------|------|
| `naver_rank_ad` | 매시간 06:00-18:00 | 검색광고 키워드 순위 (100개 단위 청크) |
| `naver_rank_shop` | 매시간 06:00-18:00 | 쇼핑 검색 순위 (3페이지) → 카탈로그 DAG 트리거 |
| `naver_product_catalog` | 수동/트리거 | 카탈로그 매칭 업데이트 |
| `naver_brand_price` | 매일 00:01 | 경쟁사 상품 가격 모니터링 → `merge` |
| `naver_brand_sales` | 매일 08:00 | 경쟁사 상품 매출 → `merge` |
| `naver_brand_pageview` | 매일 10:00 | 경쟁사 상품 페이지뷰 → `merge` |
| `naver_cafe_search` | 10분 간격 (평일 08:00-10:00) | 카페 검색 + 글 수집 → 엑셀 Slack 전송 |
| `naver_main_search` | 10분 간격 (평일 08:00-10:00) | 메인 검색 섹션별 순위 → 엑셀 Slack 전송 |

### 스마트스토어

| DAG | 스케줄 | 설명 |
|-----|--------|------|
| `smartstore_product` | 평일 23:30 | 상품, 옵션 카탈로그 → `load` |
| `smartstore_order` | 매일 08:30 | 주문 상세 (주문, 상품주문, 배송, 옵션, 상태) → `merge` |
| `smartstore_invoice` | 03:00, 10:30, 15:00 | 송장 내역 업데이트 (PAYED/DISPATCHED 필터) → `merge` |
| `smartstore_bizdata` | 평일 08:10 | 마케팅 채널 API 데이터 → `load` |

### 검색광고

| DAG | 스케줄 | 설명 |
|-----|--------|------|
| `searchad_contract` | 매일 05:30 | 브랜드 + 신제품 광고 계약 내역 → `merge` |
| `searchad_master` | 평일 23:40 | API + GFA 마스터 데이터 (캠페인, 광고그룹, 광고) |
| `searchad_report` | 매일 05:40 | 일별 다차원 보고서 → `merge` |

### 사방넷

| DAG | 스케줄 | 설명 |
|-----|--------|------|
| `sabangnet_order` | 매일 23:30 | 주문, 출고, 옵션 다운로드 → `merge` |
| `sabangnet_invoice` | 평일 10:30, 14:30, 23:50 | 송장 상태 (주문일/집하일/배달일 필터) → `merge` |
| `sabangnet_product` | 평일 23:20 | 상품, 옵션, 매핑 전체 추출 → `overwrite` |

### Google Ads / Meta Ads

| DAG | 스케줄 | 설명 |
|-----|--------|------|
| `google_ads` | 매일 07:50 | 캠페인, 광고그룹, 광고, 에셋 마스터 + 30일 인사이트 |
| `meta_ads` | 매일 07:40 | 캠페인, 광고세트, 광고 마스터 + 일별 인사이트 |

### CJ대한통운 / 이카운트

| DAG | 스케줄 | 설명 |
|-----|--------|------|
| `cj_loisparcel_invoice` | 매일 02:00 | Playwright 브라우저 자동화, 2FA 이메일 인증 → 송장 추출 |
| `ecount_stock_report` | 수동 트리거 | 4단계 TaskGroup: CJ 재고 → 쿠팡 재고 → 이카운트 재고 → 엑셀 리포트 |

---

## 스케줄 타임라인

```
00:00 ┬── naver_brand_price (00:01)
      │
01:00 ├── all_login (01:00)
      │
02:00 ├── cj_loisparcel_invoice (02:00)
      │
03:00 ├── smartstore_invoice ① (03:00)
      │
05:00 ├── searchad_contract (05:30)
      ├── searchad_report (05:40)
      ├── coupang_adreport (05:50)
      ├── coupang_campaign (05:55, 평일)
      │
06:00 ├── naver_rank_ad ↻ 매시간 (06:00-18:00)
      ├── naver_rank_shop ↻ 매시간 (06:00-18:00)
      │
07:00 ├── meta_ads (07:40)
      ├── google_ads (07:50)
      │
08:00 ├── naver_brand_sales (08:00)
      ├── naver_cafe_search ↻ 10분 (08:00-10:00, 평일)
      ├── naver_main_search ↻ 10분 (08:00-10:00, 평일)
      ├── smartstore_bizdata (08:10, 평일)
      ├── smartstore_order (08:30)
      │
09:00 ├── coupang_rocket_sales (09:10)
      ├── coupang_option (09:20)
      │
10:00 ├── naver_brand_pageview (10:00)
      ├── smartstore_invoice ② (10:30)
      ├── sabangnet_invoice ① (10:30, 평일)
      │
14:00 ├── sabangnet_invoice ② (14:30, 평일)
      │
15:00 ├── smartstore_invoice ③ (15:00)
      │
23:00 ├── sabangnet_product (23:20, 평일)
      ├── sabangnet_order (23:30)
      ├── smartstore_product (23:30, 평일)
      ├── searchad_master (23:40, 평일)
      └── sabangnet_invoice ③ (23:50, 평일)
```

---

## 플러그인

### `variables.py`

모든 DAG에서 사용하는 설정, 인증 정보 로더.

**주요 함수:**

| 함수 | 설명 |
|------|------|
| `read(path, **kwargs)` | YAML 계층 경로로 설정 로드. `tables`, `credentials`, `service_account`, `sheets` 옵션 |
| `get_execution_date(kwargs, subdays=1)` | DAG 컨텍스트에서 실행일 추출 (기본 1일 전) |
| `format_date(datetime, fmt)` | 날짜 포맷 (pendulum) |
| `split_by_credentials(creds, shuffle)` | 인증 정보 리스트를 개별 딕셔너리로 분리 |

**설정 로드 예시:**

```python
# config.yaml 경로: ["depth1", "depth2", "depth3"]
variables = read(
    ["depth1", "depth2", "depth3"],
    tables=True,            # → tables: {"data": "example.table", ...}
    service_account=True,   # → service_account: {...}
    credentials=True,       # → credentials: [{cookies}, ...]
)
```

---

## 동시성 제어 (Pool)

| Pool | 용도 | 제한 |
|------|------|------|
| `login_pool` | 브라우저 로그인 직렬화 | 1 |
| `coupang_pool` | 쿠팡 벤더별 병렬 | 벤더 수 |
| `nsearch_pool` | 네이버 검색 동시성 제어 | 제한적 |
| `sabangnet_pool` | 사방넷 요청 직렬화 | 1 |

---

## 환경설정

### Docker Compose

- **이미지**: Apache Airflow 3.1.3 (Python 3.12, 한국어 로케일)
- **서비스**: webserver, scheduler, worker, triggerer, postgres, redis
- **볼륨 마운트**:
  - `./dags` → `/opt/airflow/dags`
  - `./plugins` → `/opt/airflow/plugins`
  - `./files/env` → `/opt/airflow/env` (설정 파일)
  - `./logs` → `/opt/airflow/logs`

### 추가 패키지

```
apache-airflow-providers-slack    # Slack 알림
google-cloud-bigquery             # BigQuery 적재
gspread                           # Google Sheets
pyarrow                           # Parquet 처리 (BigQuery 의존성)
playwright                        # 브라우저 자동화
linkmerce                         # 핵심 ETL 패키지
```

### BigQuery 적재 전략

DAG별로 데이터 특성에 맞는 적재 전략을 사용한다:

| 전략 | 메서드 | 사용 DAG 예시 |
|------|--------|--------------|
| **load** (append) | `load_table_from_duckdb()` | `smartstore_product`, `coupang_campaign` |
| **merge** (upsert) | `merge_into_table_from_duckdb()` | `smartstore_order`, `coupang_adreport`, `searchad_report` |
| **overwrite** (조건부 삭제+삽입) | `overwrite_table_from_duckdb()` | `coupang_option`, `sabangnet_product` |
