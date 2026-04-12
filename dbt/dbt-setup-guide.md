# dbt 셋업 가이드

이 문서는 현재 Airflow + DuckDB + BigQuery 파이프라인에 **로컬 PostgreSQL 이중 적재**와 **dbt 분석 레이어**를 추가하는 과정을 설명한다.

---

## 아키텍처 개요

```
[Airflow DAG]
    │
    ├── Extract (API)
    │       │
    │       ▼
    ├── Transform (DuckDB 임시 테이블)
    │       │
    │       ├──▶ BigQuery (운영 원천 테이블)
    │       └──▶ PostgreSQL (로컬 백업 원천 테이블)  ← 신규
    │
    └── (완료)

[dbt] ← 이 가이드의 주제
    │
    ├── Source: PostgreSQL (원천 테이블 참조)
    ├── Models: PostgreSQL (분석 테이블 생성)
    └── Sync: BigQuery (분석 테이블 동기화) ← Airflow에서 수행 권장
```

> **BigQuery 분석 테이블 동기화에 대하여**
> dbt는 하나의 모델을 하나의 데이터베이스에서만 실행하므로 PostgreSQL → BigQuery 간 직접 동기화를 지원하지 않는다. dbt 모델을 BigQuery에서 직접 실행하거나, Airflow DAG을 통해 dbt 실행 후 PostgreSQL → BigQuery로 COPY하는 방식을 권장한다. ([Airflow 연동 방법 참고](#5-airflow와-dbt-연동-선택-사항))

---

## 1. 패키지 설치 및 환경 관리

현재 환경에는 **Apache Airflow 3.x**가 설치되어 있습니다. dbt 설치 시 유발되는 `protobuf`, `grpcio`, `blinker` 버전 변경은 Airflow의 안정성에 치명적일 수 있으므로 **가상환경 분리**를 강력히 권장합니다.

### dbt 전용 가상환경 (권장)

```bash
# dbt 폴더 내에서 전용 환경 생성
cd dbt
python -m venv venv
source venv/bin/activate

# 전용 환경에 dbt 설치
pip install dbt-postgres
```

### 메인 환경 설치 시 (의존성 충돌 해결)

만약 단일 환경에서 관리해야 한다면, 아래 패키지들을 동시에 명시하여 버전을 강제로 맞추어야 합니다.

```bash
pip install "blinker>=1.9.0" "protobuf<5.0" "grpcio>=1.71.0" dbt-postgres
```

| 패키지명 | 목적 | 비고 |
| :--- | :--- | :--- |
| **dbt-postgres** | dbt로 로컬 PostgreSQL 제어 | **핵심 (dbt-core 포함)** |
| **psycopg2-binary** | Airflow/Python에서 Postgres 접속 | DB 드라이버 |
| **astronomer-cosmos** | Airflow에서 dbt 모델을 렌더링 | 분리 환경 시 설정 복잡 |
| **sqlfluff** | SQL 코드 스타일 통일 및 자동 수정 | 필수 도구 |

### Airflow → PostgreSQL 적재 (Airflow 환경)

Airflow DAG에서 DuckDB 데이터를 PostgreSQL에 쓰기 위한 드라이버가 필요하다.

```bash
# Python PostgreSQL 드라이버 (Airflow 환경에 설치)
pip install psycopg2-binary
```

---

## 2. 로컬 PostgreSQL 설치 및 설정

### 설치 (macOS)

```bash
brew install postgresql@16
brew services start postgresql@16
```

### 데이터베이스 및 유저 생성

```bash
psql postgres
```

```sql
-- 데이터베이스 생성
CREATE DATABASE linkmerce;

-- 전용 유저 생성 (비밀번호는 변경할 것)
CREATE USER linkmerce_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE linkmerce TO linkmerce_user;
```

---

## 3. Airflow DAG에 PostgreSQL 이중 적재 추가

DuckDB에서 PostgreSQL로 데이터를 올리는 방법은 두 가지이다.

### 방법 A: DuckDB postgres 확장 사용 (권장)

DuckDB 0.9+ 에서는 별도 드라이버 없이 PostgreSQL에 직접 쓸 수 있다.

```python
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL postgres; LOAD postgres;")

# PostgreSQL에 테이블 직접 적재
conn.execute("""
    ATTACH 'host=localhost port=5432 dbname=linkmerce user=linkmerce_user password=your_password'
    AS pg (TYPE postgres);
""")
conn.execute("CREATE OR REPLACE TABLE pg.raw.marketing_channel AS SELECT * FROM marketing_channel;")
```

기존 `DuckDBConnection` 기반 패턴에 맞게 확장하려면 `linkmerce/extensions/` 아래에 `PostgreSQLClient`를 추가하는 방식을 검토한다.

### 방법 B: psycopg2 사용

```python
import psycopg2
import psycopg2.extras

conn_pg = psycopg2.connect(
    host="localhost", port=5432,
    dbname="linkmerce", user="linkmerce_user", password="your_password"
)

# DuckDB에서 데이터를 pandas로 읽어 PostgreSQL에 삽입
df = duckdb_conn.execute("SELECT * FROM marketing_channel").df()
with conn_pg.cursor() as cur:
    psycopg2.extras.execute_values(cur, "INSERT INTO raw.marketing_channel VALUES %s", df.values.tolist())
conn_pg.commit()
```

---

## 4. dbt 프로젝트 초기화

### 프로젝트 생성

```bash
cd /path/to/linkmerce/dbt

# dbt 프로젝트 초기화 (현재 디렉토리에 생성)
dbt init linkmerce_dbt
mv linkmerce_dbt/* . && rmdir linkmerce_dbt
```

초기화 중 데이터베이스 선택 메시지가 나오면 `postgres`를 선택한다.

### 생성되는 폴더 구조

```
dbt/
├── dbt_project.yml        # 프로젝트 메인 설정
├── profiles.yml           # DB 접속 정보 (이 파일은 .gitignore에 추가)
├── models/
│   ├── staging/           # 원천 테이블 → 정규화 (stg_*)
│   ├── intermediate/      # 중간 변환 (int_*)
│   └── marts/             # 최종 분석 테이블 (비즈니스 로직)
├── seeds/                 # CSV → 테이블 (코드 테이블 등)
├── macros/                # 재사용 SQL 함수
├── tests/                 # 커스텀 데이터 품질 테스트
└── snapshots/             # SCD(천천히 변하는 차원) 테이블
```

### `profiles.yml` 작성

`profiles.yml`은 **프로젝트 루트(`dbt/`) 또는 `~/.dbt/`** 에 위치해야 한다. 프로젝트 내부에 두는 경우 `.gitignore`에 반드시 추가한다.

```yaml
# dbt/profiles.yml
linkmerce_dbt:
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      dbname: linkmerce
      user: linkmerce_user
      password: your_password
      schema: analytics       # dbt 모델이 생성될 스키마
      threads: 4
    prod:
      type: postgres
      host: localhost
      port: 5432
      dbname: linkmerce
      user: linkmerce_user
      password: "{{ env_var('DBT_POSTGRES_PASSWORD') }}"
      schema: analytics
      threads: 4
  target: dev
```

> `prod` 환경에서는 비밀번호를 환경변수로 관리한다.

### `dbt_project.yml` 수정

```yaml
name: 'linkmerce_dbt'
version: '1.0.0'
profile: 'linkmerce_dbt'   # profiles.yml의 키와 일치해야 함

model-paths: ["models"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]
test-paths: ["tests"]

models:
  linkmerce_dbt:
    staging:
      +schema: staging
      +materialized: view      # 스테이징은 뷰로 관리
    intermediate:
      +schema: intermediate
      +materialized: ephemeral # 중간 단계는 CTE로 인라인 처리
    marts:
      +schema: analytics
      +materialized: table     # 최종 분석 테이블은 실체화
```

### 연결 확인

```bash
cd dbt
dbt debug --profiles-dir .   # profiles.yml이 dbt/ 폴더에 있는 경우
```

---

## 5. Airflow와 dbt 연동 (선택 사항)

Airflow에서 dbt를 실행하면 데이터 수집 완료 → 분석 테이블 자동 갱신 → BigQuery 동기화 흐름을 자동화할 수 있다.

### 패키지 설치

```bash
pip install astronomer-cosmos    # Airflow + dbt 통합 (권장)
# 또는
pip install apache-airflow-providers-dbt-cloud  # dbt Cloud를 사용하는 경우
```

### Airflow DAG에서 dbt 실행 예시 (`astronomer-cosmos` 없이)

```python
from airflow.operators.bash import BashOperator

dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="cd /path/to/linkmerce/dbt && dbt run --profiles-dir .",
)

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="cd /path/to/linkmerce/dbt && dbt test --profiles-dir .",
)

dbt_run >> dbt_test
```

### BigQuery 동기화 (dbt 실행 후 Airflow에서 처리)

dbt가 PostgreSQL에 분석 테이블을 생성한 뒤, 별도 Airflow 태스크에서 PostgreSQL → BigQuery로 동기화한다.

```python
@task(task_id="sync_analytics_to_bigquery")
def sync_analytics_to_bigquery(configs: dict) -> None:
    import psycopg2
    import pandas as pd
    from linkmerce.extensions.bigquery import BigQueryClient

    conn_pg = psycopg2.connect(...)
    df = pd.read_sql("SELECT * FROM analytics.mart_sales", conn_pg)

    with BigQueryClient(configs["service_account"]) as client:
        client.load_dataframe(df, target_table=configs["tables"]["mart_sales"])
```

---

## 6. .gitignore 추가

```gitignore
# dbt
dbt/profiles.yml
dbt/target/
dbt/dbt_packages/
dbt/logs/
```

---

## 명령어 빠른 참고

| 명령어 | 설명 |
|--------|------|
| `dbt debug` | DB 연결 확인 |
| `dbt run` | 모든 모델 실행 |
| `dbt run -s staging` | staging 폴더 모델만 실행 |
| `dbt test` | 데이터 품질 테스트 |
| `dbt docs generate` | 문서 생성 |
| `dbt docs serve` | 문서 서버 실행 (localhost:8080) |
| `dbt deps` | `packages.yml`에 정의된 패키지 설치 |
