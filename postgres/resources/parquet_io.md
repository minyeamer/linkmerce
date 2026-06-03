# parquet_io 확장 기술 문서

> Apache Arrow / Parquet C++ 라이브러리를 사용하여 PostgreSQL과 Parquet 사이의 스키마 생성, 행 적재, 조회 결과 출력을 처리하는 자체 제작 확장

## 목차

- [개요](#개요)
- [파일 구조](#파일-구조)
- [SQL 인터페이스](#sql-인터페이스)
- [입력 방식](#입력-방식)
- [내부 구조](#내부-구조)
- [타입 변환](#타입-변환)
- [함수별 동작](#함수별-동작)
- [트랜잭션](#트랜잭션)
- [빌드 및 등록](#빌드-및-등록)
- [유지보수 참고사항](#유지보수-참고사항)

## 개요

`parquet_io`는 PostgreSQL 서버 안에서 실행되는 C/C++ 확장이다.
PostgreSQL SPI(Server Programming Interface)와 Apache Arrow / Parquet C++ 라이브러리를 연결한다.

확장은 다음 3가지 공개 SQL 함수를 제공한다.

| SQL 함수 | 역할 | 주요 반환값 |
| --- | --- | --- |
| `parquet_create` | Parquet 스키마를 읽어 빈 테이블 생성 | 생성한 컬럼 수 |
| `parquet_read` | Parquet 행을 기존 테이블에 삽입 | 삽입한 행 수 |
| `parquet_write` | SQL 조회 결과를 Parquet로 직렬화 | 기록한 행 수 또는 Parquet 바이트 |

`parquet_create()`는 데이터 행을 삽입하지 않는다.   
테이블 생성 후 데이터를 적재하려면 `parquet_read()`를 별도로 호출한다.

## 파일 구조

```bash
postgres/extension/
├── Makefile
├── parquet_io.control
├── parquet_io--1.0.sql
└── parquet_io.cpp
```

| 파일 | 역할 |
| --- | --- |
| `parquet_io.cpp` | PostgreSQL C 엔트리포인트, Arrow 변환, Parquet 읽기와 쓰기 구현 |
| `parquet_io--1.0.sql` | SQL 함수명과 C 엔트리포인트 연결 |
| `parquet_io.control` | 확장 버전과 공유 라이브러리 경로 정의 |
| `Makefile` | PGXS 기반 C++ 컴파일 및 설치 설정 |

## SQL 인터페이스

공개 SQL 함수는 입력 형태에 따라 PostgreSQL 오버로드로 분기된다.

| SQL 호출 | C 엔트리포인트 | 반환 타입 |
| --- | --- | --- |
| `parquet_create(TEXT, TEXT, TEXT)` | `parquet_create_file` | `BIGINT` |
| `parquet_create(BYTEA, TEXT, TEXT)` | `parquet_create_byte` | `BIGINT` |
| `parquet_read(TEXT, TEXT)` | `parquet_read_file` | `BIGINT` |
| `parquet_read(BYTEA, TEXT)` | `parquet_read_byte` | `BIGINT` |
| `parquet_write(TEXT, TEXT)` | `parquet_write_file` | `BIGINT` |
| `parquet_write(TEXT)` | `parquet_write_byte` | `BYTEA` |

첫 번째 인자가 SQL 문자열 리터럴이면 PostgreSQL이 `TEXT` 오버로드를 선택한다.

```sql
SELECT parquet_create('/tmp/accounts.parquet', 'test.accounts', 'replace');
SELECT parquet_read('/tmp/accounts.parquet', 'test.accounts');
SELECT parquet_write('SELECT * FROM test.accounts', '/tmp/accounts.parquet');
```

클라이언트가 바이트를 바인딩하면 PostgreSQL이 `BYTEA` 오버로드를 선택한다.

```sql
SELECT parquet_create($1::BYTEA, 'test.accounts', 'replace');
SELECT parquet_read($1::BYTEA, 'test.accounts');
SELECT parquet_write('SELECT * FROM test.accounts');
```

Python 클라이언트에서는 prepared statement의 오버로드 선택이 모호해지지 않도록
`%s::BYTEA`, `%s::TEXT`처럼 명시적 캐스팅을 붙여 호출한다.

## 입력 방식

### 파일 경로

`*_file` 엔트리포인트는 전달받은 경로를 그대로 Arrow 파일 API에 넘긴다.

```cpp
arrow::io::ReadableFile::Open(source_path)
arrow::io::FileOutputStream::Open(target_path)
```

경로 해석은 PostgreSQL 서버 프로세스의 파일시스템을 기준으로 한다.

- 절대경로는 서버의 절대경로다.
- 상대경로는 서버 프로세스의 현재 작업 디렉터리를 기준으로 한다.
- 파일이 존재하지 않거나 권한이 없으면 Arrow의 `IOError`가 PostgreSQL 오류로 전달된다.
- Docker 환경에서도 호스트 경로를 자동으로 마운트하거나 변환하지 않는다.

### 바이트

`*_byte` 엔트리포인트는 PostgreSQL `BYTEA`를 Arrow 메모리 스트림으로 연결한다.

```cpp
arrow::Buffer::Wrap(...)
arrow::io::BufferReader(...)
arrow::io::BufferOutputStream(...)
```

이 방식은 클라이언트 파일을 DB 연결을 통해 전달할 때 사용한다.   
PostgreSQL 서버가 클라이언트 파일시스템을 볼 필요가 없다.

## 내부 구조

### PostgreSQL 등록

각 C 엔트리포인트 앞에는 `PG_FUNCTION_INFO_V1(...)` 매크로가 있다.

```cpp
PG_FUNCTION_INFO_V1(parquet_read_byte);
Datum parquet_read_byte(PG_FUNCTION_ARGS)
```

`V1`은 확장 버전이 아니라 PostgreSQL C 함수 호출 규약 버전이다.   
SQL 설치 파일은 `AS '$libdir/parquet_io', 'parquet_read_byte'` 형식으로 C 심볼을 연결한다.

### 공통 reader

파일과 바이트 입력은 모두 `arrow::io::RandomAccessFile` 인터페이스로 합쳐진다.

```cpp
open_parquet_reader(infile)
```

- 파일 입력: `arrow::io::ReadableFile`
- 바이트 입력: `arrow::io::BufferReader`

이후 스키마 조회와 행 읽기 로직은 입력 출처와 무관하게 같은 코드 경로를 사용한다.

### PostgreSQL SPI

확장은 PostgreSQL SPI를 사용하여 서버 내부에서 SQL을 실행한다.

```cpp
SPI_connect();
SPI_execute(...);
SPI_prepare(...);
SPI_execute_plan(...);
SPI_finish();
```

SPI는 다음 작업에 사용된다.

- 대상 테이블 존재 여부 확인
- `DROP TABLE`, `CREATE TABLE` 실행
- 대상 테이블 컬럼명과 타입 OID 조회
- Parquet 행 삽입을 위한 `INSERT` plan 생성 및 실행
- Parquet 출력 대상 SQL 조회를 위한 cursor 생성

## 타입 변환

### Parquet 스키마에서 PostgreSQL 타입 생성

`parquet_create()`는 Arrow 타입을 다음 PostgreSQL 타입으로 단순화한다.

| Arrow 타입 | PostgreSQL 타입 |
| --- | --- |
| `BOOL` | `BOOLEAN` |
| 부호 및 비부호 정수 타입 | `BIGINT` |
| `HALF_FLOAT`, `FLOAT`, `DOUBLE`, `DECIMAL128`, `DECIMAL256` | `DOUBLE PRECISION` |
| `DATE32`, `DATE64` | `DATE` |
| `TIMESTAMP` | `TIMESTAMPTZ` |
| 그 외 타입 | `TEXT` |

Parquet의 원본 정밀도나 논리 타입을 모두 그대로 복원하지는 않는다.   
새 타입을 지원하려면 `arrow_type_to_pg_type()`과 실제 값 변환 로직을 함께 수정해야 한다.

### Parquet 행을 PostgreSQL에 삽입

`parquet_read()`는 PostgreSQL 대상 컬럼의 OID를 기준으로 Arrow 값을 `Datum`으로 변환한다.

| PostgreSQL 타입 | 변환 방식 |
| --- | --- |
| `SMALLINT`, `INTEGER`, `BIGINT` | Arrow 정수 값을 PostgreSQL 정수 `Datum`으로 변환 |
| `REAL`, `DOUBLE PRECISION` | Arrow 숫자 값을 부동소수점 `Datum`으로 변환 |
| `NUMERIC` | `float8_numeric`을 통해 `NUMERIC` 생성 |
| `BOOLEAN` | Arrow boolean 값을 PostgreSQL boolean으로 변환 |
| `DATE` | Unix epoch와 PostgreSQL epoch 차이를 보정 |
| `TIMESTAMP`, `TIMESTAMPTZ` | 시간 단위를 마이크로초로 맞추고 PostgreSQL epoch 차이를 보정 |
| 그 외 타입 | 문자열로 직렬화하여 `TEXT` 처리 |

`NUMERIC`은 Arrow decimal 값을 문자열 또는 double로 읽은 뒤 PostgreSQL `NUMERIC` Datum으로 변환한다.
정밀도가 중요한 컬럼은 입력 Parquet 스키마와 대상 PostgreSQL 컬럼 타입을 함께 점검한다.

### PostgreSQL 조회 결과를 Parquet로 출력

`parquet_write()`는 PostgreSQL OID를 Arrow builder 타입으로 변환한다.

| PostgreSQL 타입 | Arrow 타입 |
| --- | --- |
| `SMALLINT`, `INTEGER`, `BIGINT` | `int64` |
| `REAL`, `DOUBLE PRECISION`, `NUMERIC` | `float64` |
| `BOOLEAN` | `boolean` |
| `DATE` | `date32` |
| `TIMESTAMP` | `timestamp(MICRO)` |
| `TIMESTAMPTZ` | `timestamp(MICRO, "UTC")` |
| 그 외 타입 | `large_utf8` |

`NUMERIC`은 현재 `float64`로 출력되므로 고정밀 소수에는 정밀도 손실 가능성이 있다.

## 함수별 동작

### `parquet_create`

```sql
SELECT parquet_create($1::BYTEA, 'test.accounts', 'replace');
```

1. 파일 또는 바이트에서 Parquet reader를 연다.
2. Arrow 스키마를 읽는다.
3. 대상 테이블명을 `schema.table`로 분리한다. 스키마가 없으면 `public`을 사용한다.
4. `if_exists` 값을 검사한다.
5. Arrow 필드를 PostgreSQL 컬럼 정의로 변환한다.
6. 빈 테이블을 생성한다.
7. 생성한 컬럼 수를 `BIGINT`로 반환한다.

`if_exists`는 다음 값을 지원한다.

| 값 | 동작 |
| --- | --- |
| `error` | 테이블이 이미 존재하면 오류 발생 |
| `ignore` | 기존 테이블을 유지하고 `0` 반환 |
| `replace` | 기존 테이블을 삭제하고 다시 생성 |

### `parquet_read`

```sql
SELECT parquet_read($1::BYTEA, 'test.accounts');
```

1. 파일 또는 바이트에서 Parquet reader를 연다.
2. 대상 테이블 컬럼명과 타입 OID를 PostgreSQL 카탈로그에서 읽는다.
3. Parquet와 PostgreSQL 테이블에 공통으로 존재하는 컬럼을 이름으로 매칭한다.
4. PostgreSQL 컬럼 순서에 맞춰 parameterized `INSERT` plan을 만든다.
5. Parquet row group을 읽고 Arrow 값을 PostgreSQL `Datum`으로 변환한다.
6. 각 행을 기존 테이블에 삽입한다.
7. 삽입한 행 수를 `BIGINT`로 반환한다.

공통 컬럼이 하나도 없거나 대상 테이블이 없으면 오류가 발생한다.   
Parquet에 없는 대상 컬럼은 `INSERT` 컬럼 목록에서 제외되므로 PostgreSQL 기본값 또는 `NULL` 규칙이 적용된다.

### `parquet_write`

```sql
SELECT parquet_write('SELECT * FROM test.accounts');
SELECT parquet_write('SELECT * FROM test.accounts', '/tmp/accounts.parquet');
```

1. 전달받은 SQL 조회문으로 SPI cursor를 연다.
2. 첫 번째 batch의 `TupleDesc`에서 Arrow 출력 스키마를 만든다.
3. 조회 결과를 최대 `65536`행 단위로 읽는다.
4. PostgreSQL `Datum`을 Arrow builder에 추가한다.
5. Snappy 압축을 적용하여 Parquet로 직렬화한다.
6. 파일 출력은 기록한 행 수를 `BIGINT`로 반환한다.
7. 바이트 출력은 직렬화된 Parquet를 `BYTEA`로 반환한다.

현재 조회 결과가 0행이면 출력 스트림에 Parquet 스키마를 쓰기 전에 종료한다.   
따라서 빈 결과의 Parquet 파일 또는 바이트가 필요한 경우 구현 보완이 필요하다.

## 트랜잭션

확장 함수는 내부에서 `COMMIT` 또는 `ROLLBACK`을 직접 실행하지 않는다.   
트랜잭션 경계는 호출자가 제어한다.

```sql
BEGIN;
SELECT parquet_create($1::BYTEA, 'test.accounts', 'replace');
SELECT parquet_read($1::BYTEA, 'test.accounts');
COMMIT;
```

`parquet_read()`가 실패하면 호출자가 `ROLLBACK`하여 `parquet_create()`의 테이블 생성도 되돌릴 수 있다.   
PostgreSQL의 `CREATE TABLE`과 `DROP TABLE`은 일반적인 트랜잭션 대상이다.

Python의 `PostgresClient.create_table_from_parquet()`는 같은 cursor에서   
`parquet_create()`와 `parquet_read()`를 연속 실행한다. `commit=True`라면 두 호출이 모두 성공한 뒤 commit한다.
Python 호출부는 `parquet_create(%s::BYTEA, %s::TEXT, %s::TEXT)`와
`parquet_read(%s::BYTEA, %s::TEXT)`처럼 명시적 타입 캐스팅을 사용한다.

## 빌드 및 등록

확장 코드를 수정하면 PostgreSQL 이미지를 다시 빌드해야 한다.

```bash
cd postgres
./build.sh
docker compose up -d
```

새 데이터 디렉터리에서는 `init.sql`이 `CREATE EXTENSION IF NOT EXISTS parquet_io;`를 실행한다.

기존 데이터베이스에서 SQL 함수 정의가 변경된 경우에는 확장을 다시 등록한다.

```sql
DROP EXTENSION parquet_io;
CREATE EXTENSION parquet_io;
```

`DROP EXTENSION` 실행 전에는 확장 함수에 의존하는 객체가 있는지 확인한다.

## 유지보수 참고사항

- SQL 함수명과 C 엔트리포인트 연결은 `parquet_io--1.0.sql`에서 관리한다.
- 파일 경로 입력은 서버 파일시스템을 사용한다. 클라이언트 파일은 `BYTEA`로 전달한다.
- 읽기 로직은 Parquet와 대상 테이블 컬럼을 이름으로 매칭한다. 컬럼명 변경 시 적재 범위가 달라질 수 있다.
- `parquet_read()`는 row group 단위로 읽지만 PostgreSQL에는 행 단위로 삽입한다. 대규모 파일 성능 개선 시 bulk insert 전략을 검토한다.
- `parquet_write()`는 최대 `65536`행 단위로 조회한다.
- Arrow / Parquet 라이브러리 버전을 올릴 때 deprecated API 경고를 확인한다.
- `NUMERIC`과 decimal 계열은 현재 `float64` 중심으로 변환된다. 정밀도가 중요한 데이터는 변환 정책을 별도로 설계한다.
- Arrow 타입을 추가할 때는 create, read, write 세 방향의 타입 매핑을 함께 점검한다.
- PostgreSQL 확장 ABI 변경 가능성이 있으므로 PostgreSQL major 버전을 변경하면 반드시 이미지를 다시 빌드한다.
