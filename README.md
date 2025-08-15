
# LinkMerce

**E-commerce API 통합 관리 플랫폼**

---

## 목차

- [LinkMerce](#linkmerce)
  - [목차](#목차)
  - [소개](#소개)
  - [설치 및 사용법](#설치-및-사용법)
    - [PyPI 패키지](#pypi-패키지)
  - [구성](#구성)
    - [ETL 모듈 및 구조](#etl-모듈-및-구조)
      - [요청 작업별 ETL 구조 (`core/`)](#요청-작업별-etl-구조-core)
  - [확장 모듈](#확장-모듈)
  - [Airflow 워크플로우](#airflow-워크플로우)
## 소개

LinkMerce는 다양한 이커머스 API를 통합 관리할 수 있는 Python 기반 플랫폼입니다. 
API 연동, 데이터 적재, ETL, 스케줄링을 지원하며, PyPI 패키지와 Airflow 워크플로우로 구성되어 있습니다.

---

## 설치 및 사용법

### PyPI 패키지

1. Python 환경 준비 (>=3.10)
2. 패키지 설치:  
     `pip install linkmerce`
3. 환경설정 파일(`src/env/`) 및 예제 참고

---

## 구성

### ETL 모듈 및 구조

- **Extract (추출)**
    - `extract.py`의 `Extractor` 클래스는 외부 API, DB, 파일 등 다양한 소스에서 데이터를 동기/비동기로 추출하는 기능을 제공합니다.
    - 세션 관리, 요청 파라미터, 변수 관리, 파싱 로직을 포함하며, 실제 데이터 추출은 `extract` 또는 `extract_async` 메서드를 통해 구현됩니다.
    - 예시: REST API에서 상품 정보를 받아오는 커스텀 Extractor 구현 가능

- **Transform (변환)**
    - `transform.py`의 `Transformer` 및 하위 클래스(`JsonTransformer`, `DBTransformer` 등)는 추출된 데이터를 원하는 형태로 변환합니다.
    - JSON, DB 결과 등 다양한 입력을 받아 파싱, 필터링, 타입 변환, 구조 변경 등 데이터 가공을 담당합니다.
    - 예시: API 응답 JSON을 표준 데이터셋으로 변환, DB 결과를 DataFrame 등으로 가공

- **Load (적재)**
    - `load.py`의 `Connection` 및 관련 함수/클래스는 변환된 데이터를 DB, 파일, 외부 시스템에 적재하는 기능을 제공합니다.
    - DuckDB, BigQuery 등 다양한 데이터 웨어하우스 연동을 지원하며, SQL 실행, 파일 저장(csv/json/parquet), 커넥션 관리 등 포함
    - 예시: 변환된 데이터를 DuckDB에 저장, BigQuery로 업로드, CSV/JSON 파일로 내보내기

각 모듈은 추상 클래스와 메서드로 구성되어 있어, 실제 사용 환경에 맞게 커스텀 구현이 가능합니다.

#### 요청 작업별 ETL 구조 (`core/`)

- 각 비즈니스/데이터 작업 단위별로 `core/` 디렉토리 내에 아래와 같은 파일 구조를 갖습니다:
    - `extract.py`: 해당 작업에 필요한 데이터 추출 로직(예: API 호출, DB 조회 등)을 정의합니다.
    - `transform.py`: 추출된 데이터를 가공/정제/필터링하는 변환 로직을 정의합니다.
    - `models.sql`: 데이터 적재 및 조회에 필요한 SQL 모델(테이블/뷰/쿼리 등)을 정의합니다.

- 이들 파일은 서로 다음과 같이 연결되어 동작합니다:
    1. **extract.py**에서 원천 데이터를 수집
    2. **transform.py**에서 수집된 데이터를 비즈니스 목적에 맞게 변환
    3. **models.sql**에 정의된 스키마/쿼리를 활용해 데이터를 저장하거나 추가 가공

- **API 통합 사용 방식**
    - `api/` 모듈에서는 각 작업별로 `core/`의 extract, transform, models를 불러와 하나의 파이프라인으로 통합합니다.
    - 예를 들어, 특정 상품의 랭킹 정보를 가져오는 API는 `core/rank_shop/extract.py`로 데이터 추출, `transform.py`로 변환, `models.sql`로 적재 및 조회를 수행합니다.
    - 이를 통해 코드의 재사용성과 유지보수성을 높이고, 각 작업별 ETL 로직을 명확하게 분리할 수 있습니다.

이 구조는 확장성과 모듈화에 최적화되어 있어, 새로운 데이터 작업 추가 시에도 일관된 방식으로 ETL 파이프라인을 설계할 수 있습니다.

---

## 확장 모듈

- **BigQuery 연동**: 확장 모듈(`extensions/bigquery.py`)을 통해 Google BigQuery에 데이터를 적재하거나 조회할 수 있습니다.
- **Google Sheets 연동**: 확장 모듈(`extensions/sheets.py`)을 통해 Google Sheets API를 활용한 데이터 연동이 가능합니다.
- 기타 외부 시스템 연동도 확장 모듈 구조로 손쉽게 추가할 수 있습니다.
- 확장 모듈에 대한 의존성은 명시되어 있지 않습니다.

---

## Airflow 워크플로우

- 이커머스 데이터 ETL 및 스케줄링을 위한 Airflow DAG 및 관련 스크립트/설정
- 주요 구성:
    - DAGs: `airflow/dags/` (예: naver_brand_price, naver_brand_sales_first, naver_product_catalog 등)
    - 설정: `airflow/config/airflow.cfg`, `docker-compose.yaml`
    - 실행 스크립트: `exec.sh`, `init.sh`
- 주요 기능:
    - 네이버 스마트스토어/검색광고/랭킹/브랜드 데이터 ETL
    - BigQuery, DuckDB 등 데이터 웨어하우스 연동
    - 스케줄링 및 트리거 기반 데이터 파이프라인
- 실행 예시:
    ```bash
    cd airflow
    docker compose up airflow-init && docker compose up -d
    ```
- DAG 예시:
    - 매일 자정 브랜드 가격 데이터 적재: `naver_brand_price`
    - 매일 오전 브랜드 매출 데이터 적재: `naver_brand_sales_first`
    - 시간별 광고/비광고 순위 데이터 적재: `naver_rank_ad`, `naver_rank_shop`
