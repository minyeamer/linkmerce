# LinkMerce

**이커머스 데이터 수집, 변환, 적재 통합 프레임워크**

> 다양한 이커머스 API와 웹문서 응답을 수집하고 DuckDB 기반 변환을 거쳐,   
> BigQuery, PostgreSQL, Google Sheets 등 외부 시스템에 데이터를 적재하는 워크플로우를 관리하기 위한 프로젝트를 안내한다.

## 목차

- [프로젝트 개요](#프로젝트-개요)
- [핵심 아키텍처](#핵심-아키텍처)
- [표준 ETL 모듈 구조](#표준-etl-모듈-구조)
- [Airflow 서비스 구조](#airflow-서비스-구조)
- [Airflow 작업 스케줄링](#airflow-작업-스케줄링)
- [PostgreSQL 적재 환경](#postgresql-적재-환경)
- [Streamlit UI](#streamlit-ui)
- [빠른 시작](#빠른-시작)
- [예시 코드](#예시-코드)

## 프로젝트 개요

LinkMerce 프로젝트는 이커머스 플랫폼으로부터 쇼핑몰 운영에 필요한 데이터를 수집하기 위한 목적을 가진다.

Python 스크래핑 로직을 구현한 PyPI 패키지가 프로젝트의 중심이 되며,   
작업 스케줄링을 처리하기 위해 Apache Airflow를 적극적으로 활용한다.

현재 `linkmerce` 패키지 버전은 `1.0.6`이다.

프로젝트에서 주목할 부분은 다음 5가지다.

1. `src/linkmerce`: 웹 스크래핑 및 외부 시스템과의 데이터 연동을 지원하는 핵심 Python 패키지
2. `src/tests`: Python 패키지의 동작을 검증하고 중간 실행 결과를 저장하는 테스트 모음
3. `airflow`: 작업 스케줄링, 오케스트레이션, Playwright 기반 브라우저 자동화가 들어 있는 DAG 모음
4. `postgres`: 로컬 PostgreSQL 18 적재 환경과 초기 스키마, 파티션, Parquet 확장을 관리하는 실행 환경
5. `streamlit`: 쇼핑몰 운영 담당자가 비정기적인 데이터 수집을 위해 DAG을 수동으로 트리거할 때 사용하는 UI

linkmerce 패키지가 지원하는 이커머스 관련 플랫폼의 수집 범위는 다음과 같다.

| 플랫폼 구분 | 수집 범위 |
| --- | --- |
| CJ대한통운 eFLEXs | 재고 |
| 쿠팡 광고센터 | 광고 |
| 쿠팡 판매자센터 | 상품, 매출 |
| 이카운트 API | 상품, 재고 |
| 구글 API | 광고 |
| 메타 API | 광고 |
| 네이버 메인 | 검색 |
| 네이버 오픈 API | 검색 |
| 사방넷 시스템 | 주문, 상품 |
| 네이버 검색광고 API | 광고 보고서, 광고 계약, 검색량 |
| 네이버 광고주센터 (검색광고) | 광고 보고서 |
| 네이버 성과형 디스플레이 광고 | 광고 보고서, 광고 순위 |
| 네이버 커머스 API | 주문, 상품, 통계 |
| 네이버 쇼핑파트너센터 | 매출, 방문 통계, 카탈로그/상품 |

## 핵심 아키텍처

linkmerce 패키지를 이해하기 위해 주목해야 할 것은 `Extractor` → `Transformer` 연결이다.

linkmerce 패키지는 ETL 프로세스를 [ _추출(Extract), 변환(Transform), 적재(Load)_ ] 3가지 부분으로 구분한다.   
`Extractor`와 `Transformer`는 각각 추출과 변환 역할을 담당한다.

1. `Extractor`는 동기 또는 비동기 HTTP 세션을 활용한 HTTP 요청을 담당한다.   
    일부 작업은 매개변수 목록에 대해 반복 요청하는데, `Task`를 활용해 이러한 동작을 추상화한다.
2. `Transformer`는 HTTP 응답 결과를 JSON 형식으로 파싱하고, DuckDB 테이블의 스키마에 맞게 변환해 적재한다.   
    DuckDB 연결을 통해 BigQuery, PostgreSQL, Google Sheets 같은 외부 시스템에 데이터를 적재하는 확장 기능을 별도로 제공한다.

지금까지의 설명을 아래 표로 정리할 수 있다.

| 계층 구분 | 책임 | 구현 경로 |
| --- | --- | --- |
| `Extractor` | HTTP 세션 관리, 요청 메시지 빌드 | `src/linkmerce/common/extract.py` |
| `Task` | 반복 요청, 재시도, 페이지네이션 | `src/linkmerce/common/tasks.py` |
| `ResponseTransformer` | 응답 결과 파싱, JSON 형식으로 변환 | `src/linkmerce/common/transform.py` |
| `DuckDBTransformer` | DuckDB 테이블 생성 및 적재 | `src/linkmerce/common/transform.py` |
| `DuckDBConnection` | DuckDB 연결 관리, CRUD 작업 지원 | `src/linkmerce/common/load.py` |
| `API Endpoint` | `Extractor`와 `Transformer` 연결 | `src/linkmerce/api/common.py` |
| `Extensions` | DuckDB 테이블을 외부 시스템과 연동 | `src/linkmerce/extensions/*.py` |

계층 구분에 따른 워크플로우는 또한 다음의 흐름도로 정리해볼 수도 있다.

```bash
[API Endpoint] (api/...)
    ↓   사용자 파라미터 전달
Extractor (core/.../extract.py)
    ↓   HTTP 요청 및 응답 수신
ResponseTransformer (core/.../transform.py)
    ↓   JSON 형식으로 변환
DuckDBTransformer (core/.../transform.py)
    ↓   DuckDB 테이블에 적재
DuckDBConnection (common/load.py)
    ↓   외부 시스템에 적재
BigQueryClient / PostgresClient / WorksheetClient (extensions/...)
```

## 표준 ETL 모듈 구조

ETL 프로세스를 플랫폼, 호스트명, 카테고리로 구성된 3단계 하위 경로로 구분한다.

```bash
core/{platform}/{hostname}/{category}/
├── extract.py
├── transform.py
└── models.sql
```

이 구조는 하나의 카테고리 안에서 책임을 명확히 분리한다.

- `extract.py`: HTTP 요청 방식 구현
- `transform.py`: 응답 파싱 및 DuckDB 적재 방식 구현
- `models.sql`: CREATE, INSERT 등 SQL 쿼리문 설계

이 패턴 덕분에 플랫폼 또는 호스트별로 구현이 달라도, 실행 방식과 테스트 방식은 비교적 일관되게 유지된다.

ETL 프로세스를 실행할 때는 편의성을 위해 `core/` 모듈을 순서대로 호출하지 않고, 하나의 API 함수를 호출한다.

linkmerce 패키지와 관련된 `src/linkmerce/` 경로에 대한 상세 설명은
별도의 [문서](https://github.com/minyeamer/linkmerce/blob/main/src/README.md)를 참고한다.

## 테스트 안내

`src/tests` 경로에선 단일 `Extractor` 또는 `Transformer` 단위로 정상 동작하는지 테스트를 담당한다.

- `test_extract.py`: `Extractor.extract()` 실행 결과 저장
- `test_transform.py`: `DuckDBTransformer.parse()` 및 `bulk_insert()` 실행 결과 저장
- `conftest.py`: 공용 리소스 Fixture 정의, `Transformer` 테스트를 지원하는 `TransformerHarness` 제공
- `results/`: 각 테스트의 실행 결과가 저장되는 경로. Git 버전 관리에서는 제외된다.

테스트와 관련된 `src/linkmerce/tests/` 경로에 대한 상세 설명은
별도의 [문서](https://github.com/minyeamer/linkmerce/blob/main/src/tests/README.md)를 참고한다.

## Airflow 서비스 구조

Airflow 시스템을 구성하는 서비스 목록은 `docker-compose.yaml` 설정에서 정의한다.   
운영 환경에서 Airflow Celery Executor 구성을 기준으로 다음 서비스들이 실행되고 있다.

- `postgres`
- `redis`
- `playwright`
- `airflow-apiserver`
- `airflow-scheduler`
- `airflow-dag-processor`
- `airflow-worker`
- `airflow-triggerer`
- `airflow-init`

나머지는 Apache Airflow에서 기본으로 정의한 서비스들이지만, `playwright` 서비스를 예외적으로 추가했다.   
일부 작업에서는 `playwright` 서비스를 활용해 브라우저 렌더링을 활용한 웹 스크래핑을 수행한다.

Docker Compose 실행은 다음 명령어 또는 `init.sh` 스크립트를 실행한다.

```bash
cd airflow
docker compose up airflow-init
docker compose up -d
```

## Airflow 작업 스케줄링

linkmerce 패키지를 활용하는 Airflow DAG은 공통적으로 다음 흐름을 가진다.

1. `airflow_utils` 플러그인의 `read_config()` 또는 `read_credentials()` 함수로 설정과 인증 정보를 불러온다.
2. `linkmerce.api.*` 함수를 호출해 추출(extract)과 변환(transform) 과정을 수행한다.
3. `DuckDBConnection`을 통해 테이블에 적재된 API 실행 결과를 불러올 수 있다.
4. `dual_load` 플러그인으로 DuckDB 테이블을 PostgreSQL에 먼저 적재한 뒤 BigQuery에도 적재한다.
5. Task 결과를 `{params: {...}, results: {...}}` 딕셔너리로 반환한다.

이러한 흐름을 DAG으로 구현할 경우 다음과 같은 코드로 나타낼 수 있다.

```python
with DAG(dag_id="...") as dag:

    PATH = "platform.hostname.category"

    @task(task_id="...")
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)

    @task(task_id="...")
    def read_credentials() -> list:
        from airflow_utils import read_config
        return read_config(PATH, credentials=True)["credentials"]

    @task(task_id="...", map_index_template="{{ credentials['id'] }}")
    def etl_task(credentials: dict, configs: dict, **kwargs) -> dict:
        return main(**credentials, **configs)

    def main(tables: dict[str, str], **kwargs) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.platform.hostname import example_api
        from dual_load import load_table_from_duckdb

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            example_api(**kwargs, connection=conn)

            return {
                "params": {},
                "results": {
                    tables["table"]: load_table_from_duckdb(
                        connection = conn,
                        source_table = "table",
                        target_table = tables["table"],
                    ),
                },
            }

    (etl_task
    .partial(configs=read_configs())
    .expand(credentials=read_credentials()))
```

Airflow와 관련된 `airflow/` 경로에 대한 상세 설명은
별도의 [문서](https://github.com/minyeamer/linkmerce/blob/main/airflow/README.md)를 참고한다.

## PostgreSQL 적재 환경

`postgres/` 경로는 Airflow MetaDB와 별개로 ETL 결과를 적재하기 위한 로컬 PostgreSQL 18 환경을 제공한다.

구성 요소는 다음과 같다.

- `Dockerfile`: PostgreSQL 18, `pg_partman`, Apache Arrow / Parquet 런타임, 자체 `parquet_io` 확장을 포함한 이미지 빌드
- `init.sql`: 플랫폼별 스키마와 테이블, 일별 파티션 초기화
- `partman_maintenance.sql`: 운영 중 미래의 파티션을 생성하기 위한 유지보수 SQL
- `resources/bq_schemas.json`: PostgreSQL `init.sql` 기준으로 생성한 BigQuery 스키마 참고 파일
- `resources/parquet_io.md`: `parquet_io` 확장의 SQL 인터페이스와 타입 변환 정책 설명

Airflow DAG은 대부분 `dual_load` 플러그인을 통해 PostgreSQL 적재를 먼저 수행하고, 성공하면 BigQuery 적재를 이어서 수행한다.
PostgreSQL은 더 엄격한 기본키와 타입 제약을 검증하는 1차 적재 대상으로 사용하고, BigQuery는 기존 분석 테이블 적재 대상으로 유지한다.

PostgreSQL 실행 환경에 대한 상세 설명은
별도의 [문서](https://github.com/minyeamer/linkmerce/blob/main/postgres/README.md)를 참고한다.

## Streamlit UI

`streamlit/app.py`는 Airflow REST API를 호출하여 수동으로 DAG을 트리거하기 위한 운영용 UI다.

Airflow UI에 접근하여 직접 DAG을 트리거할 수 없는 쇼핑몰 운영 담당자가   
비정기적인 데이터 수집 작업(_사방넷 주문 ETL만 지원_)에서 DAG을 제어할 수 있게 간단한 UI을 제공한다.

Airflow와는 별도의 Docker Compose로 단일 Streamlit 서비스를 관리하며,   
로컬 네트워크에서 서버 아이피와 포트를 조합한 주소로 접근한다.

Docker Compose 실행은 다음 명령어를 사용한다.

```bash
cd streamlit
docker compose up -d
```

## 빠른 시작

### 1. 패키지 개발 환경

```bash
pip install -e .
```

패키지 메타데이터는 `pyproject.toml`에 정의되어 있다.

### 2. PostgreSQL 로컬 실행

```bash
cd postgres
./build.sh
docker compose up -d
```

### 3. Airflow 로컬 실행

```bash
cd airflow
docker compose up airflow-init
docker compose up -d
```

Airflow에서 dual load를 실행하려면 `postgres`, `bigquery` Airflow Connection을 먼저 등록해야 한다.

### 4. 테스트 실행

```bash
pytest src/tests/test_extract.py -m extract -v -s
pytest src/tests/test_transform.py -m transform -v -s
pytest src/tests/test_load.py -m load -v -s
```

## 예시 코드

```python
from linkmerce.api.smartstore.api import marketing_channel
from linkmerce.common.load import DuckDBConnection

with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
    rows = marketing_channel(
        client_id="...",
        client_secret="...",
        channel_seq="...",
        start_date="2025-01-01",
        end_date="2025-01-31",
        connection=conn,
        return_type="json",
    )

print(len(rows))
```

이 호출은 API 함수를 통해 `Extractor`와 `DuckDBTransformer`를 연쇄적으로 실행하고,   
결과를 DuckDB 테이블에 적재한 뒤, 테이블 행을 조회하여 `list[dict]` 형식으로 반환한다.
