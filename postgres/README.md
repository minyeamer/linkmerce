# PostgreSQL 적재 환경 안내

> `postgres/` 경로의 실행 환경, 초기 스키마, 파티션 유지보수, `parquet_io` 확장을 설명한다.

## 목차

- [개요](#개요)
- [한눈에 보기](#한눈에-보기)
- [디렉터리 구조](#디렉터리-구조)
- [로컬 실행 환경](#로컬-실행-환경)
- [초기 스키마](#초기-스키마)
- [파티션 관리](#파티션-관리)
- [parquet_io 확장](#parquet_io-확장)
- [Python 연동](#python-연동)
- [운영 참고사항](#운영-참고사항)

## 개요

`postgres/` 디렉터리는 LinkMerce 적재용 PostgreSQL 18 환경을 관리한다.

여기서는 다음 내용을 다룬다.

1. PostgreSQL 18 컨테이너 빌드와 실행
2. `init.sql` 기반 스키마와 테이블 초기화
3. `pg_partman` 기반 일별 파티션 관리
4. `parquet_io` 확장 빌드와 사용 방식

## 한눈에 보기

- **베이스 이미지**: `postgres:18`
- **컨테이너명**: `linkmerce-postgres`
- **기본 포트**: `5432`
- **기본 데이터베이스**: `linkmerce`
- **초기화 파일**: `init.sql`
- **파티션 확장**: `pg_partman`
- **자체 제작 확장**: `parquet_io`

## 디렉터리 구조

```bash
postgres/
├── extension/
│   ├── Makefile
│   ├── parquet_io.control
│   ├── parquet_io--1.0.sql
│   └── parquet_io.cpp
├── .env
├── Dockerfile
├── README.md
├── build.sh
├── docker-compose.yaml
├── init.sql
├── partman_maintenance.sql
└── resources/
    ├── bq_schemas.json
    ├── exec.sh
    └── parquet_io.md
```

주요 파일의 역할은 다음과 같다.

| 파일 | 역할 |
| --- | --- |
| `Dockerfile` | PostgreSQL 18 이미지에 `pg_partman`, Arrow 런타임, `parquet_io` 확장을 설치 |
| `docker-compose.yaml` | PostgreSQL 단일 서비스를 실행하고 데이터 디렉터리와 포트를 연결 |
| `.env` | 이미지명, 사용자, 비밀번호, 데이터베이스, 포트, 데이터 경로 설정 |
| `init.sql` | 최초 데이터베이스 생성 시 스키마, 테이블, 확장, 일별 파티션 초기화 |
| `partman_maintenance.sql` | 운영 중 `pg_partman` 파티션 유지보수를 수동 실행 |
| `extension/` | `parquet_io` 확장의 C++ 구현 및 PostgreSQL 설치 파일 |
| `resources/bq_schemas.json` | `init.sql` 기준으로 생성한 BigQuery 스키마 참고 파일 |
| `resources/exec.sh` | 실행 중인 컨테이너에 `psql`로 접속하는 보조 스크립트 |
| `resources/parquet_io.md` | `parquet_io` 내부 구조와 유지보수 방법을 설명하는 기술 문서 |

## 로컬 실행 환경

`Dockerfile`은 빌드 이미지와 런타임 이미지를 분리한다.

1. 빌드 단계에서 `postgresql-server-dev-18`, `libarrow-dev`, `libparquet-dev`를 설치한다.
2. `extension/Makefile`을 실행하여 `parquet_io.so`와 확장 SQL 파일을 PostgreSQL 경로에 설치한다.
3. 런타임 단계에는 실행에 필요한 Arrow / Parquet 라이브러리와 `postgresql-18-partman`만 설치한다.
4. `init.sql`을 `/docker-entrypoint-initdb.d/`에 복사하여 최초 초기화를 수행한다.

이미지를 빌드하고 컨테이너를 실행할 때는 다음 명령어를 사용한다.

```bash
cd postgres
./build.sh
docker compose up -d
```

실행 중인 PostgreSQL에 `psql`로 접속할 때는 `resources/exec.sh`를 사용한다.

```bash
./resources/exec.sh
```

`docker-compose.yaml`은 데이터 디렉터리만 컨테이너에 마운트한다.

```bash
${POSTGRES_DATA_DIR:-./data} -> /var/lib/postgresql
```

호스트 프로젝트 디렉터리는 컨테이너에 자동으로 마운트하지 않는다.   
따라서 SQL에서 파일 경로를 직접 전달하는 경우 해당 파일은 PostgreSQL 서버가 접근할 수 있는 위치에 있어야 한다.

## 초기 스키마

`init.sql`은 데이터베이스가 처음 생성될 때 한 번 실행된다.

플랫폼별 스키마를 만들고, ETL 결과를 적재할 테이블을 `CREATE TABLE IF NOT EXISTS` 방식으로 초기화한다.

| 스키마 | 데이터 범위 |
| --- | --- |
| `analytics` | 통합 분석 데이터 |
| `cj_eflexs`, `cj_loisparcel` | CJ대한통운 재고 및 배송 데이터 |
| `coupang`, `coupang_ads`, `coupang_rfm` | 쿠팡 상품, 광고, 로켓그로스 데이터 |
| `ecount` | 이카운트 상품 및 재고 데이터 |
| `google_ads`, `meta_ads` | 구글 및 메타 광고 데이터 |
| `naver_shp` | 네이버 쇼핑 검색 데이터 |
| `relation` | 외부 시스템 간 식별자 매핑 |
| `sabangnet` | 사방넷 주문 및 상품 데이터 |
| `searchad` | 네이버 검색광고 및 GFA 데이터 |
| `smartstore` | 스마트스토어 주문, 상품, 통계 데이터 |
| `ss_hcenter` | 네이버 쇼핑파트너센터 데이터 |
| `partman` | `pg_partman` 관리 객체 |
| `test` | 적재 기능 검증용 테이블 |

기존 데이터 디렉터리를 유지한 채 `init.sql`을 수정하면 자동으로 다시 실행되지 않는다.   
기존 인스턴스에 스키마 변경을 반영할 때는 필요한 SQL을 별도로 실행해야 한다.

## 파티션 관리

시계열 데이터가 누적되는 테이블은 `PARTITION BY RANGE`로 선언한다.   
예를 들어 주문일, 수집일시, 보고서 기준일 컬럼을 기준으로 일별 파티션을 사용한다.

`init.sql` 하단의 `public.bootstrap_daily_partitions()` 함수는 다음 순서로 초기 파티션을 준비한다.

1. 대상 부모 테이블에 기존 파티션이 있는지 확인한다.
2. 파티션이 없다면 `partman.create_parent()`로 부모 테이블을 등록한다.
3. 지정된 시작일부터 현재 날짜와 `premake` 범위까지 일별 파티션을 생성한다.
4. `automatic_maintenance`, `infinite_time_partitions`, `premake` 설정을 갱신한다.

운영 중 미래 파티션을 준비할 때는 다음 SQL을 실행한다.

```bash
cd postgres
docker exec -i linkmerce-postgres \
  psql -U linkmerce -d linkmerce \
  < partman_maintenance.sql
```

`partman_maintenance.sql`은 아래 프로시저를 호출한다.

```sql
CALL partman.run_maintenance_proc(
  p_wait := 0,
  p_analyze := false,
  p_jobmon := false
);
```

## parquet_io 확장

`parquet_io`는 Apache Arrow / Parquet C++ 라이브러리를 사용한 PostgreSQL 확장이다.

| SQL 함수 | 역할 |
| --- | --- |
| `parquet_create(...)` | Parquet 스키마를 읽어 빈 PostgreSQL 테이블 생성 |
| `parquet_read(...)` | Parquet 행을 기존 PostgreSQL 테이블에 삽입 |
| `parquet_write(...)` | SQL 조회 결과를 Parquet 파일 또는 `BYTEA`로 출력 |

각 함수는 서버 파일 경로와 `BYTEA` 전달 방식을 구분하는 오버로드를 제공한다.

```sql
SELECT parquet_create('/tmp/accounts.parquet', 'test.accounts', 'replace');
SELECT parquet_read('/tmp/accounts.parquet', 'test.accounts');
SELECT parquet_write('SELECT * FROM test.accounts', '/tmp/accounts.parquet');
```

파일 경로 방식은 PostgreSQL 서버 프로세스가 접근할 수 있는 경로를 사용한다.   
상대경로는 서버 프로세스의 현재 작업 디렉터리를 기준으로 탐색하며, 파일이 없으면 오류가 발생한다.

클라이언트 파일을 전달하거나 Parquet 바이트를 직접 다룰 때는 `BYTEA` 오버로드를 사용한다.

```sql
SELECT parquet_create($1::BYTEA, 'test.accounts', 'replace');
SELECT parquet_read($1::BYTEA, 'test.accounts');
SELECT parquet_write('SELECT * FROM test.accounts');
```

구현 세부사항은 [parquet_io 기술 문서](resources/parquet_io.md)를 참고한다.

## Python 연동

`src/linkmerce/extensions/postgres.py`의 `PostgresClient`는 CSV, JSON, Parquet 형식의 생성, 삽입, 추출을 지원한다.

Parquet 파일 경로를 Python 메서드에 넘기면 클라이언트 프로세스가 파일을 읽고 `BYTEA`로 PostgreSQL에 전달한다.   
따라서 PostgreSQL이 Docker 컨테이너나 원격 서버에 있어도 클라이언트가 접근 가능한 파일을 적재할 수 있다.

```python
from linkmerce.extensions.postgres import PostgresClient

with PostgresClient("postgresql://...") as client:
    client.create_table_from_parquet(
        "test.accounts",
        "accounts.parquet",
        option="replace",
        commit=True,
    )

    client.fetch_all_to_parquet(
        "SELECT * FROM test.accounts",
        save_to="accounts_export.parquet",
    )
```

`create_table_from_parquet()`는 `parquet_create()`로 테이블을 만든 뒤,   
같은 트랜잭션에서 `parquet_read()`를 호출하여 Parquet 행까지 적재한다.

### DuckDB 연동

DuckDB 테이블을 PostgreSQL로 직접 넘기는 적재 메서드도 제공한다.

| 메서드 | 동작 |
| --- | --- |
| `load_table_from_duckdb` | DuckDB 소스 테이블 행을 대상 테이블에 삽입 |
| `overwrite_table_from_duckdb` | 스테이징 테이블을 만든 뒤 대상 범위를 삭제하고 다시 삽입 |
| `upsert_table_from_duckdb` | 스테이징 테이블을 만든 뒤 `INSERT ... ON CONFLICT`로 병합 |
| `create_partitions` | `pg_partman`으로 부모 테이블의 파티션 생성 |

DuckDB 연동 적재는 DuckDB `postgres` 확장을 사용해 PostgreSQL을 `db` 데이터베이스로 attach한 뒤 실행한다.
`install_extension=True` 옵션을 사용하면 실행 시점에 DuckDB 확장을 설치하고 로드한다.

## 운영 참고사항

- `init.sql`은 새 데이터 디렉터리를 초기화할 때만 자동 실행된다.
- `parquet_io` 구현을 변경하면 `./build.sh`로 이미지를 다시 빌드하고 컨테이너를 재생성해야 한다.
- 기존 데이터베이스에서 확장 SQL 함수 정의가 바뀌면 `DROP EXTENSION parquet_io; CREATE EXTENSION parquet_io;`로 다시 등록한다.
- `DROP EXTENSION`은 확장 함수 정의를 제거하므로 운영 적용 전에 의존 객체를 확인한다.
- 파일 경로 기반 Parquet 함수는 서버 파일시스템을 사용하고, `BYTEA` 기반 함수는 DB 연결을 통해 데이터를 전달한다.
- `parquet_io`는 Parquet 입력 컬럼과 PostgreSQL 테이블 컬럼을 이름으로 매칭한다.
