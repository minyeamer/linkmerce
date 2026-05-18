# 테스트 요청
주피터 노트북 쉘을 통해 가져오는 데이터를 DuckDBConnection에 올리고 PostgresClient로 postgres 테이블에 적재하는 기능 테스트를 요청한다.

## DuckDB 구성
- 전체 행 수: 12
- 소스 테이블: `test` (최초 테스트 시 JSON 데이터를 적재하면서 생성)

## BigQuery 구성
- 연결(서비스 계정): `env/service.account.json`
- 타겟 테이블: `test.test` (최초 테스트 시 원본 테이블 `meta_ads.accounts` 스키마를 빈 내용으로 복제한다.)
- 타겟 메서드: `load_table_from_duckdb`, `overwrite_table_from_duckdb`, `merge_into_table_from_duckdb`

## PostgreSQL 구성
- 연결(DSN): `postgresql://linkmerce:linkmerce@localhost:5432/linkmerce`
- 쉘 접속: `docker exec -it "f2d7fab80548" psql -U "linkmerce" -d "linkmerce"`
- 타겟 테이블: `test.test` (최초 테스트 시 원본 테이블 `meta_ads.accounts` 스키마를 빈 내용으로 복제한다.)
- 타겟 메서드: `load_table_from_duckdb`, `overwrite_table_from_duckdb`, `upsert_table_from_duckdb`

## (공통) 스키마
원본 테이블 `meta_ads.accounts`은 다음과 같은 스키마를 가진다:

```sql
CREATE TABLE IF NOT EXISTS meta_ads.accounts (
    account_id TEXT NOT NULL -- 계정ID
  , account_name TEXT -- 계정명
  , account_group TEXT -- 계정그룹
  , account_seq BIGINT -- 계정순번
  , PRIMARY KEY (account_id)
);
```

## (공통) 적재 검증
정상적으로 적재한 후 검증할 때는 타겟 테이블에 대해 `COUNT(*)` 쿼리를 실행하여 전체 행 수가 출력되는지 확인한다.

## (공통) 삽입 오류 검증
의도적으로 오류 상황을 발생시켰을 때 삽입 오류가 발생하는지 확인한다. 추가로, 타겟 테이블에 대해 `COUNT(*)` 쿼리를 실행하여 행 수가 동일하지 않고 증가했는지 확인한다.

## (공통) 소스 스테이징 테이블
소스 테이블의 구성을 다르게 하여 실패를 유도할 때 소스 스테이징 테이블을 사용한다. 소스 스테이징 테이블은 `test` 명칭 뒤에 임의의 8글자 해시값을 붙여서 이름을 짓는다. 생성을 요청하면 기존 `test` 테이블을 내용 없이 스키마만 복제한다. 테스트 후 별도의 지침이 없으면 스테이징 테이블은 DROP한다.
- **구성 불일치 검증** 시에는 `account_seq` 열을 `account_no` 열로 이름을 변경하여 적재하고 **삽입 오류 검증**한다.
- **덮어쓰기 검증** 시에는 `account_seq` 정수열의 값을 1씩 증가시켜 적재하고 타겟 테이블의 값이 변경되는지 확인한다. 덮어쓰기 검증은 항상 타겟 스테이징 테이블을 대상으로 한다. 매번 타겟 테이블을 복제하고 확인하면 DROP한다.

## (공통) 타겟 스테이징 테이블
존재하지 않는 테이블을 대상으로 쿼리를 실행할 때 타겟 테이블 명칭에 `build_staging_table_name` 함수를 적용하여 타겟 스테이징 테이블 이름을 짓는다. 타겟 스테이징 테이블을 사용한다면 타겟 테이블의 역할을 대체하여 적재 대상으로 사용한다. overwrite/merge/upsert 작업 시 메서드 내부적으로 생성하는 스테이징 테이블과는 다른 개념이다.

## (주의) 빅쿼리 참조
빅쿼리는 테이블 참조 시 백틱(`)으로 감싸야 한다. 메서드 내부적으로 처리하기 때문에 테이블을 파라미터로 전달할 때는 그대로 전달하지만, 쿼리를 직접 실행한다면 주의한다.

# PostgreSQL 테스트
duckdb의 `test` 소스 테이블에서 postgres의 `test.test` 타겟 테이블로 적재하는 공통된 방향을 가진다.

## load_table_from_duckdb
메서드는 소스 테이블을 타겟 테이블 적재하는 기능을 한다. bigquery/postgres 각각 공통으로 다음 기능 테스트를 요청한다:

1. 적재 검증: 적재 검증하고 메서드 반환값도 `True`인지 확인한다.
2. 중복 검증: 1번 테스트를 반복해서 적재할 때 제약 조건에 대한 **삽입 오류 검증**한다.
3. **구성 불일치 검증**
4. `if_target_table_not_found` 조건 테스트: 타겟 스테이징 테이블의 이름만 짓고 존재하지 않는 타겟 스테이징 테이블을 타겟으로 3가지 조건에 대해 메서드를 실행한다.
    - `"break"`: 메서드 실행 시 `False`가 반환되는지 확인한다.
    - `"raise"`: 메서드 실행 시 `UndefinedTable` 오류가 발생하는지 확인한다.
    - `"create"`: 메서드 실행 시 타겟 테이블이 생성되는지 확인하고 적재 검증한다.
    - `"create"`: 동일한 조건에서 **구성 불일치 검증**한다. 삽입 오류가 발생했을 때 타겟 스테이징 테이블 생성이 롤백되지 않고 남아있는지 추가로 확인한다.
5. columns 지정 테스트: [account_id, account_name] 열만 지정하여 적재 검증한다. 누락된 [account_group, account_seq] 열은 NULL 값으로 대체되었는지 추가로 확인한다.

## overwrite_table_from_duckdb
메서드는 소스 테이블을 타겟 테이블에 부분 덮어쓰기로 적재하는 기능을 한다. bigquery/postgres 각각 공통으로 다음 기능 테스트를 요청한다:

1. WHERE문 검증: **덮어쓰기 검증**을 통해 값을 변경시키고 where_clause를 다르게 하여 적재 후 출력 결과가 다른지 검증한다.
    - `"account_group = '임피엘'"`: WHERE 문은 5개 행이 해당된다. 해당 행의 `account_seq` 값이 1씩 증가되었는지 확인한다.
    - `"account_group = '테스트'`: WHERE 문은 해당하는 행이 없다. 모든 행의 `account_seq` 값이 바뀌지 않았는지 확인한다.
    - `None`: WHERE 문을 생략하면 TRUNCATE 실행 후 적재한다. 모든 행의 `account_seq` 값이 1씩 증가되었는지 확인한다.
2. **구성 불일치 검증**: `where_clause=None` 조건에서 **구성 불일치 검증**한다. 삽입 오류가 발생했을 때 타겟 스테이징 테이블 생성이 롤백되지 않고 남아있는지 추가로 확인한다.
3. columns 지정 테스트: `where_clause="account_group = '임피엘'"` 조건에서 [account_id, account_name, account_seq] 열만 지정하여 적재 검증한다. 해당 행의 `account_seq` 값이 1씩 증가되고, `account_group` 값이 NULL인지 확인한다.
4. 스테이징 테이블 검증: `cleanup_staging_table=False` 조건으로 메서드를 실행 해 메서드 내부적으로 생성한 스테이징 테이블을 DROP하지 않고 남긴다. 메서드 종료 후 스테이징 테이블이 남아있는지 확인하고 DROP한다.

## merge_into_table_from_duckdb
메서드는 소스 테이블을 타겟 테이블에 UPSERT(bigquery 기준 MERGE)하는 기능을 한다. bigquery에서만 테스트를 요청한다:

공통으로 지정된 파라미터를 가지고 `overwrite_table_from_duckdb` 메서드의 WHERE문 검증과 동일한 검증을 한다.
**덮어쓰기 검증**을 위해 소스 스테이징 테이블 생성 시 `account_seq` 정수열 값을 1씩 증가시키고, 추가로 `account_group` 텍스트열 뒤에 "_그룹" 텍스트를 붙인다.

1. 다음 파라미터에 대해 **덮어쓰기 검증** 시 where_clause에 해당되는 [account_seq, account_group] 열의 값이 모두 변경되었는지 확인한다.
    - `on_conflict="account_id"`
    - `matched=":replace_all:"`
2. 다음 파라미터에 대해 **덮어쓰기 검증** 시 where_clause에 해당되는 모든 행의 값이 바뀌지 않았는지 확인한다.
    - `on_conflict="account_id"`
    - `matched=":do_nothing:"`
3. 다음 파라미터에 대해 **덮어쓰기 검증** 시 where_clause에 해당되는 모든 행의 값이 바뀌지 않았는지 확인한다.
    - `on_conflict="account_id"`
    - `matched={"account_name": "replace", "account_group": "ignore", "account_name": "least"}`

## upsert_table_from_duckdb
메서드는 소스 테이블을 타겟 테이블에 UPSERT하는 기능을 한다. postgres에서만 테스트를 요청한다:

공통으로 지정된 파라미터를 가지고 `overwrite_table_from_duckdb` 메서드의 WHERE문 검증과 동일한 검증을 한다.
**덮어쓰기 검증**을 위해 소스 스테이징 테이블 생성 시 `account_seq` 정수열 값을 1씩 증가시키고, 추가로 `account_group` 텍스트열 뒤에 "_그룹" 텍스트를 붙인다.

1. 다음 파라미터에 대해 **덮어쓰기 검증** 시 where_clause에 해당되는 [account_seq, account_group] 열의 값이 모두 변경되었는지 확인한다.
    - `on_conflict="account_id"`
    - `do_action=":replace_all:"`
2. 다음 파라미터에 대해 **덮어쓰기 검증** 시 where_clause에 해당되는 모든 행의 값이 바뀌지 않았는지 확인한다.
    - `on_conflict="account_id"`
    - `do_action=":do_nothing:"`
3. 다음 파라미터에 대해 **덮어쓰기 검증** 시 where_clause에 해당되는 모든 행의 값이 바뀌지 않았는지 확인한다.
    - `on_conflict="account_id"`
    - `do_action={"account_name": "replace", "account_group": "ignore", "account_name": "least"}`

bigquery/postgres에 대해 각각 모든 테스트가 종료되면 생성한 타겟 테이블을 DROP하고 각각의 연결을 명시적으로 종료시킨다.

# 작업 안내
위 테스트 계획을 토대로 `tests/` 경로에 `test_load.py` 테스트 파일을 생성하라.
테스트 게획은 `meta_ads.accounts`라는 특정 테이블을 대상으로 작성했는데, 이 테이블명을 공개적으로 노출할 수는 없기 때문에
`fixtures.yaml` 설정에서 `load.bigquery`, `load.postgres` 키값 경로를 만들고 각각 다음 설정을 작성하도록 한다.
- 소스 데이터 파일 경로
- 소스 데이터 형식(csv, json, parquet 중 1 또는 확장자로 유추하여 생략 가능)
- 소스 테이블 명칭(duckdb)
- 연결 정보(bigquery의 서비스 계정 또는 postgres의 DSN)
- 원본 테이블 명칭(bigquery 또는 postgres)
- 타겟 테이블 명칭(bigquery 또는 postgres)
- 구성 불일치 검증 시 열 이름 변경 규칙 (dict 타입 {변경전: 변경후})
- WHERE문 검증 시 where_clause 목록
- WHERE문 검증 시 값 변경 규칙
- columns 지정 테스트 시 칼럼 목록
- 덮어쓰기 검증 시 값 변경 규칙
- 덮어쓰기 검증 시 [on_conflict, do_action] 조합 목록

가능하면 테스트 문서 `README.md`에도 test_load에 대한 설명을 첨부하여 ETL 테스트 설명이 자연스럽게 구성되도록 하라.
Extract-Transform 은 의존성이 있지만, Load는 독립적인 테스트다.
