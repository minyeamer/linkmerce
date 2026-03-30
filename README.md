
# LinkMerce

**이커머스 데이터 수집 · 변환 · 적재 통합 프레임워크**

> 다양한 이커머스 플랫폼의 API 및 웹 스크래핑을 통해 데이터를 수집하고, DuckDB 기반 SQL 변환을 거쳐 Google BigQuery에 적재하는 Python ETL 프레임워크

---

## 목차

- [LinkMerce](#linkmerce)
  - [목차](#목차)
  - [개요](#개요)
  - [아키텍처](#아키텍처)
  - [프로젝트 구조](#프로젝트-구조)
  - [핵심 패키지 (`src/linkmerce/`)](#핵심-패키지-srclinkmerce)
    - [ETL 파이프라인 구조](#etl-파이프라인-구조)
    - [지원 플랫폼 및 API](#지원-플랫폼-및-api)
  - [스케줄링 (`airflow/`)](#스케줄링-airflow)
  - [크론탭 (`crontab/`)](#크론탭-crontab)
  - [수동 실행 UI (`streamlit/`)](#수동-실행-ui-streamlit)
  - [설치](#설치)
    - [PyPI 패키지](#pypi-패키지)
    - [Airflow 환경](#airflow-환경)
  - [실행](#실행)

---

## 개요

LinkMerce는 이커머스 운영에 필요한 데이터를 **9개 플랫폼**으로부터 자동 수집하여 BigQuery 데이터 웨어하우스에 통합하는 프레임워크이다. 핵심 기능은 PyPI 패키지(`linkmerce`)로 배포하고, Apache Airflow로 스케줄링하며, 브라우저 자동화가 필요한 작업은 크론탭으로, 담당자 수동 실행은 Streamlit 웹 UI로 처리한다.

**데이터 처리 흐름:**

```
[이커머스 API / 웹] → Extract → DuckDB (SQL Transform) → BigQuery (Load)
```

1. **Extract** — HTTP API 호출 또는 웹 스크래핑으로 원천 데이터 수집
2. **Transform** — DuckDB 인메모리 DB에서 SQL로 데이터 정제·변환
3. **Load** — DuckDB에서 BigQuery로 적재 (append / merge / overwrite)

---

## 아키텍처

```
┌─────────────────────────────────────────────────────────┐
│                    스케줄링 & 실행                         │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │   Airflow    │  │   Crontab    │  │   Streamlit   │  │
│  │  (27 DAGs)   │  │ (브라우저 로그인)│  │  (수동 실행 UI) │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬────────┘  │
└─────────┼─────────────────┼─────────────────┼───────────┘
          │                 │                 │
          ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────┐
│               linkmerce 핵심 패키지 (PyPI)                 │
│                                                         │
│  ┌──────────┐    ┌─────────────┐    ┌────────────────┐  │
│  │   api/   │───▶│    core/    │───▶│    common/     │  │
│  │ (통합 API)│    │ (Extract+   │    │ (Extractor,    │  │
│  │          │    │ Transform)  │    │  Transformer,  │  │
│  │          │    │             │    │  Connection)   │  │
│  └──────────┘    └─────────────┘    └────────────────┘  │
│                                                         │
│  ┌──────────────┐              ┌─────────────────────┐  │
│  │ extensions/  │              │       utils/        │  │
│  │ (BigQuery,   │              │ (날짜, 엑셀, 파싱 등)   │  │
│  │  Sheets)     │              │                     │  │
│  └──────────────┘              └─────────────────────┘  │
└─────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────┐
│              외부 서비스                                   │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────┐   │
│  │  DuckDB  │  │ BigQuery │  │  Sheets  │  │ Slack  │   │
│  │ (인메모리) │  │  (DW)    │  │ (리포트)   │  │ (알림)  │   │
│  └──────────┘  └──────────┘  └──────────┘  └────────┘   │
└─────────────────────────────────────────────────────────┘
```

---

## 프로젝트 구조

```
linkmerce/
├── src/linkmerce/          # 핵심 패키지 (PyPI 배포)
│   ├── api/                #   통합 API — core/의 Extract+Transform을 조합한 실행 진입점
│   ├── core/               #   ETL 모듈 — 플랫폼별 extract.py, transform.py, models.sql
│   ├── common/             #   공통 베이스 — Extractor, Transformer, DuckDBConnection
│   ├── extensions/         #   외부 연동 — BigQuery, Google Sheets
│   └── utils/              #   유틸리티 — 날짜, 엑셀, 파싱, 정규식 등
│
├── airflow/                # Airflow 스케줄링
│   ├── dags/               #   27개 DAG 정의
│   ├── plugins/            #   변수 로더, linkmerce 패키지 심볼릭 링크
│   └── config/             #   airflow.cfg
│
├── crontab/                # 크론탭 (브라우저 자동화 로그인)
│   ├── coupang_login.py    #   쿠팡 로그인 스크립트
│   └── *.sh                #   셸 래퍼 (cron 등록용)
│
├── streamlit/              # 수동 실행 UI
│   └── app.py              #   Airflow DAG 트리거 웹 인터페이스
│
├── src/env/                # 환경설정 (비공개)
│   ├── config.yaml         #   테이블 매핑, 스키마 경로
│   ├── credentials.yaml    #   플랫폼별 인증 정보
│   ├── schemas.json        #   BigQuery 테이블 스키마 정의
│   └── service_account.json#   GCP 서비스 계정
│
├── pyproject.toml          # 패키지 메타데이터 (v0.6.8)
└── Dockerfile              # Airflow 워커 이미지
```

> 각 하위 디렉토리의 상세 문서: [`src/linkmerce/README.md`](src/linkmerce/README.md) · [`airflow/README.md`](airflow/README.md)

---

## 핵심 패키지 (`src/linkmerce/`)

### ETL 파이프라인 구조

모든 데이터 수집 작업은 `core/` 하위에 **동일한 3파일 구조**로 구성된다:

```
core/{플랫폼}/{서비스}/{작업}/
├── extract.py      # 데이터 추출 (API 호출, 웹 스크래핑)
├── transform.py    # 데이터 변환 (DuckDB SQL)
└── models.sql      # SQL 모델 정의 (CREATE, SELECT, INSERT)
```

`api/` 모듈이 이 세 파일을 조합하여 하나의 실행 가능한 함수로 노출한다:

```python
from linkmerce.common.load import DuckDBConnection
from linkmerce.api.smartstore.api import marketing_channel

with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
    marketing_channel(
        client_id="...", client_secret="...", channel_seq="...",
        start_date="2025-01-01", end_date="2025-01-31",
        connection=conn,
    )
    # conn 내부의 DuckDB 테이블에 변환된 데이터가 적재됨
    # → BigQuery 등 외부 시스템으로 로드
```

**핵심 클래스:**

| 클래스 | 모듈 | 역할 |
|--------|------|------|
| `Extractor` | `common/extract.py` | HTTP 세션 관리, 동기/비동기 요청, 페이지네이션, 재시도 |
| `Transformer` | `common/transform.py` | JSON/HTML 파싱, DuckDB SQL 변환, 테이블 생성·삽입 |
| `DuckDBConnection` | `common/load.py` | 인메모리 DuckDB 커넥션, SQL 실행, 표현식 빌더 |
| `BigQueryClient` | `extensions/bigquery.py` | DuckDB → BigQuery 적재 (append/merge/overwrite) |

### 지원 플랫폼 및 API

총 **9개 플랫폼**, **27개 ETL 모듈** 지원:

| 플랫폼 | 모듈 경로 | 주요 API | 데이터 |
|--------|-----------|----------|--------|
| **쿠팡** | `coupang/` | Advertising, Wing | 광고 리포트, 캠페인, 상품 옵션, 로켓 정산 |
| **네이버 스마트스토어** | `smartstore/` | Commerce API, 브랜드센터, 파트너센터 | 주문, 상품, 매출, 페이지뷰, 마케팅 채널 |
| **네이버 검색광고** | `searchad/` | API, GFA, 관리 | 광고 리포트, 계약, 키워드 순위, 노출 진단 |
| **네이버 검색** | `naver/` | 메인 검색, 오픈 API | 검색 결과, 카페 글, 쇼핑 순위 |
| **사방넷** | `sabangnet/` | Admin | 주문, 상품, 옵션, 송장 |
| **CJ 대한통운** | `cj/` | eFlexs, 로이스파셀 | 재고, 송장 |
| **이카운트** | `ecount/` | ERP API | 상품, 재고 |
| **Google** | `google/` | Google Ads API | 캠페인, 광고그룹, 광고, 인사이트 |
| **Meta** | `meta/` | Marketing API | 캠페인, 광고세트, 광고, 인사이트 |

---

## 스케줄링 (`airflow/`)

Docker 기반 Apache Airflow 3.1.3에서 **27개 DAG**를 운영한다.

**표준 DAG 실행 패턴:**

```python
# 1. 변수·인증 정보 로드
read_variables() → read_credentials()
# 2. 인증 정보별 병렬 실행 (Dynamic Task Mapping)
etl_task.partial(variables=vars).expand(credentials=creds)
# 3. ETL 실행: Extract → DuckDB Transform → BigQuery Load
# 4. 결과 반환: {params, counts, status}
```

주요 DAG 스케줄:

| 시간대 | DAG | 주기 |
|--------|-----|------|
| 00:01 | 브랜드 가격 | 매일 |
| 01:00 | 통합 로그인 (스마트스토어, 검색광고) | 매일 |
| 02:00 | CJ 로이스파셀 송장 | 매일 |
| 05:30–05:55 | 검색광고 계약·리포트, 쿠팡 광고 리포트 | 매일 |
| 06:00–18:00 | 네이버 광고/쇼핑 순위 | 매시간 |
| 07:40–08:30 | Meta·Google 광고, 스마트스토어 주문·비즈데이터 | 매일 |
| 09:10–09:20 | 쿠팡 로켓 정산·상품 옵션 | 매일 |
| 23:20–23:50 | 사방넷 상품·송장, 스마트스토어 상품 | 평일 |

> 상세: [`airflow/README.md`](airflow/README.md)

---

## 크론탭 (`crontab/`)

Airflow 워커에서 실행할 수 없는 **브라우저 자동화 로그인**을 크론탭으로 처리한다.

| 스크립트 | 시간 | 목적 |
|----------|------|------|
| `coupang_login_advertising.sh` | 05:41 | 쿠팡 광고 도메인 쿠키 갱신 |
| `coupang_login_wing.sh` | 09:01 | 쿠팡 윙 도메인 쿠키 갱신 |

`coupang_login.py` 가 실제 로그인 로직을 수행하고, 셸 스크립트가 크론에서 이를 호출한다.

---

## 수동 실행 UI (`streamlit/`)

사내 담당자가 브라우저에서 Airflow DAG를 수동 트리거할 수 있는 Streamlit 웹 애플리케이션이다.

- 날짜/시간 범위 입력
- Airflow REST API를 통한 DAG 실행
- 실행 상태 실시간 모니터링 (running, success, failed 등)

```bash
cd streamlit
docker compose up -d
```

---

## 설치

### PyPI 패키지

```bash
pip install linkmerce
```

- Python >= 3.10
- 주요 의존성: `aiohttp`, `duckdb`, `requests`, `bs4`, `openpyxl`, `jinja2`
- BigQuery/Sheets 연동 시 별도 설치 필요: `google-cloud-bigquery`, `gspread`

### Airflow 환경

```bash
cd airflow
docker compose up airflow-init && docker compose up -d
```

- 기반 이미지: Apache Airflow 3.1.3 (Python 3.12)
- 추가 패키지: `playwright`, `apache-airflow-providers-slack`, `pyarrow`

---

## 실행

```python
# 단독 사용 예시: 스마트스토어 마케팅 채널 데이터 수집
from linkmerce.common.load import DuckDBConnection
from linkmerce.api.smartstore.api import marketing_channel
from linkmerce.extensions.bigquery import BigQueryClient

with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
    marketing_channel(
        client_id="YOUR_CLIENT_ID",
        client_secret="YOUR_CLIENT_SECRET",
        channel_seq="CHANNEL_SEQ",
        start_date="2025-01-01",
        end_date="2025-01-31",
        connection=conn,
    )

    with BigQueryClient(service_account) as bq:
        bq.load_table_from_duckdb(
            connection=conn,
            source_table="data",
            target_table="project.dataset.marketing_channel",
        )
```
