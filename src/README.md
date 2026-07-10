# linkmerce 패키지 안내

> LinkMerce 프로젝트의 핵심 실행 로직이 들어 있는 Python 패키지

다양한 이커머스 API와 웹문서 응답을 수집하고,   
DuckDB 기반 변환을 거쳐 BigQuery, PostgreSQL, Google Sheets 같은 외부 시스템으로 넘길 수 있게 구성되어 있다.

## 목차
- [한눈에 보기](#한눈에-보기)
- [패키지 구조](#패키지-구조)
- [실행 모델](#실행-모델)
- [api/](#api)
- [common/](#common)
- [core/](#core)
- [extensions/](#extensions)
- [utils/](#utils)
- [SQL 모델 규칙](#sql-모델-규칙)
- [ETL 기능 추가 규칙](#ETL-기능-추가-규칙)
- [사용 예시](#사용-예시)

## 한눈에 보기

- **패키지명**: `linkmerce`
- **버전**: `1.0.9`
- **Python**: `>=3.12`
- **핵심 의존성**: `aiohttp`, `requests`, `duckdb`, `bs4`, `openpyxl`, `ruamel-yaml`, `tqdm`

## 패키지 구조

1단계 경로를 펼치면 다음과 같은 역할을 하는 디렉터리가 존재한다.

```bash
src/linkmerce/
├── api/            # core/ 기능을 연결하는 API 함수 모음
├── common/         # core/ 기능이 참조하는 공통 클래스 모음
├── core/           # 핵심 실행 로직 구현
├── extensions/     # 외부 시스템과의 연동 로직 구현
└── utils/          # 유틸리티 함수 모음
```

2단계 경로를 펼치면 커머스 플랫폼 도메인 별로 구분된 디렉터리 또는 공통 모듈들이 존재한다.

```bash
src/linkmerce/
├── api/
│   ├── cj/
│   ├── coupang/
│   ├── ecount/
│   ├── google/
│   ├── meta/
│   ├── naver/
│   ├── sabangnet/
│   ├── searchad/
│   ├── smartstore/
│   ├── common.py
│   └── config.py
├── common/
│   ├── exceptions.py
│   ├── extract.py
│   ├── load.py
│   ├── models.py
│   ├── tasks.py
│   └── transform.py
├── core/
│   ├── cj/
│   ├── coupang/
│   ├── ecount/
│   ├── google/
│   ├── meta/
│   ├── naver/
│   ├── sabangnet/
│   ├── searchad/
│   └── smartstore/
├── extensions/
│   ├── bigquery.py
│   ├── postgres.py
│   └── gsheets.py
└── utils/
    ├── cast.py
    ├── date.py
    ├── excel.py
    ├── graphql.py
    ├── headers.py
    ├── nested.py
    ├── parse.py
    ├── progress.py
    └── regex.py
```

테스트와 관련된 `src/tests/` 경로에 대한 설명은 별도의 [문서](src/tests/README.md)를 참고한다.

## 실행 모델

linkmerce 패키지를 사용한 ETL 프로세스는 다음 5단계로 정리할 수 있다.

1. `Extractor`가 대상 웹사이트에 HTTP 요청을 보내 응답을 받는다.
2. `ResponseTransformer`가 응답 결과를 받아 SQL 쿼리로 다룰 수 있는 JSON 형식의 데이터로 변환한다.
3. `DuckDBTransformer`가 JSON 형식의 데이터를 임시 DuckDB 테이블에 맞게 가공하여 적재한다.
4. `api/*` 함수가 `Extractor`와 `Transformer`를 조합해 하나의 ET 프로세스를 구성한다.
5. `extensions/*` 기능으로 DuckDB 테이블을 BigQuery, PostgreSQL 등 외부 시스템에 적재하는 ETL 프로세스를 완성한다.

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

## api/

대부분의 API 함수는 공통적인 실행 규칙을 가진다. (API의 공통 로직은 `api/common.py`에 정리한다.)

1. `with_duckdb_connection` 데코레이터로 감싼 API 함수 실행 시 `connection`이 없으면 임시 DuckDB 연결을 만든다.
2. `prepare_duckdb_extract` 함수로 `Extractor`와 `Transformer`의 초기화 옵션을 생성한다.
   - `return_type != "raw"` 조건에서는 `Transformer().transform` 메서드를 `Extractor`의 초기화 옵션에 전달한다.
   - `return_type == "raw"` 조건에서는 `Transformer` 객체를 생성하지 않는다.
3. `Extractor` 객체를 생성하면서 `extract` 메서드를 실행한다.
4. API 함수 실행이 끝나면 다시 `with_duckdb_connection` 데코레이터로 돌아와 결과를 반환한다.
   - `return_type != "raw"` 조건에서는 DuckDB 테이블 조회 결과를 반환한다.
   - `return_type == "raw"` 조건에서는 `extract` 메서드의 실행 결과를 그대로 반환한다.

```bash
[Start]
    ↓   사용자의 API 호출 (return_type != "raw")
@with_duckdb_connection
    ↓   DuckDB 연결 보장
api(*args, **kwargs)
    ↓   Extractor, Transformer 불러오기
prepare_duckdb_extract()
    ↓   Extractor 초기화 옵션 생성 (parser = Transformer.transform())
Extractor.extract()
    ↓   실행 결과를 DuckDB 테이블에 적재
@with_duckdb_connection
    ↓   DuckDB 테이블 조회 결과 반환
[만약 DuckDB 연결을 임시로 열었다면 실행을 종료하면서 닫는다.]
```

## common/

ETL 프로세스를 구성하는 `core/` 핵심 로직에는 공통적인 패턴이 있다.   
그러한 패턴을 추상 클래스 형태로 `common/` 하위 모듈에 구현한다.

### `common/extract.py`

> 데이터 수집을 담당하는 `Extractor` 추상 클래스를 단계적으로 정의한다.

- `BaseSessionClient` 클래스는 HTTP 세션 기반 요청을 처리하는 추상 클라이언트 클래스다.
   - 실제 HTTP 요청 동작은 `BaseSessionClient` 클래스를 상속받는 2가지 하위 클래스에서 구현한다.
   - `RequestSessionClient` 클래스는 `requests.Session` 기반 동기식 HTTP 요청을 담당한다.
   - `AiohttpSessionClient` 클래스는 `aiohttp.ClientSession` 기반 비동기식 HTTP 요청을 담당한다.
- `SessionClient` 클래스는 위 2가지 하위 클래스를 다중 상속받아 동기식/비동기식 HTTP 요청 동작을 모두 가진다.
- `TaskClient` 클래스는 Task 기반의 요청 실행을 관리하는 클래스다. `common/task.py` 모듈을 참조한다.
- `Extractor` 클래스는 `SessionClient`와 `TaskClient`를 상속받으면서 `extract` 추상 메서드를 가진다.
- `LoginHandler`는 `Extractor`를 상속받으면서 외부 서비스 로그인을 처리하는 역할을 하는 추상 클래스다.

### `common/tasks.py`

> 다양한 HTTP 요청 패턴을 `Task` 추상 클래스에 기반해 정의한다.

`Task`로부터 파생된 클래스들은 `Extractor`의 주요 실행 패턴을 메서드 체이닝으로 추상화하여   
`Extractor`를 다룰 때 편의성을 제공한다. 두 클래스의 관계는 아래 표로 정리할 수 있다.

| Extractor 메서드 | Task 클래스 | 동작 |
| --- | --- | --- |
| `request` | `Request` | 단일 요청 |
| `request_loop` | `RequestLoop` | 조건 충족 시까지 재시도 |
| `request_each` | `RequestEach` | 매개변수 목록에 대한 순차 요청 |
| `request_each_loop` | `RequestEachLoop` | 재시도 + 순차 요청 |
| `paginate_all` | `PaginateAll` | 페이지 단위 순차 요청 |
| `request_each_pages` | `RequestEachPages` | 매개변수 목록 별 페이지 단위 순차 요청 |
| `cursor_all` | `CursorAll` | 커서 기반 반복 요청 |
| `request_each_cursor` | `RequestEachCursor` | 매개변수 목록 별 커서 기반 반복 요청 |

### `common/transform.py`

> 다양한 데이터 변환 패턴을 `Transformer` 추상 클래스에 기반해 정의한다.

`Transformer`는 크게 `ResponseTransformer`와 `DBTransformer`로 분기된다.

#### 1. `ResponseTransformer`

HTTP 응답 데이터를 SQL 문으로 다룰 수 있는 JSON 형식으로   
변환하기 위한 목적을 가지고 6단계 파이프라인을 연결한다.

1. `assert_valid_response`: HTTP 응답 데이터의 타입 및 유효성 검증
2. `get_scope`: 전체 데이터 중 파싱 대상이 되는 특정 지점(`scope`) 탐색
3. `parse`: 탐색된 데이터를 필드 선택에 용이한 데이터 구조로 변환
4. `select_fields`: 변환된 데이터에서 필요한 필드만 추출
5. `select_and_extend`: 필드 선택과 파생 필드 생성을 연결해 개별 항목 처리
6. `extend_fields`: 필드 선택 결과에 파생 필드 생성 또는 값 변환

`common/transform.py` 모듈 내에 정의된 `ResponseTransformer` 파생 클래스는 아래 3가지가 있다.
1. `JsonTransformer`: `dict` 또는 `list` 형식의 JSON 응답 데이터를 변환한다.
2. `HtmlTransformer`: `BeautifulSoup`으로 파싱할 수 있는 HTML 소스코드를 변환한다.
3. `ExcelTransformer`: 엑셀 `bytes`를 `openpyxl` 라이브러리로 읽어서 변환한다.

#### 2. `DBTransformer`

JSON 형식으로 변환된 데이터를 DB 테이블에 적재한다. 변환과 적재 작업을 2단계 파이프라인으로 구성한다.
1. `parse`: `ResponseTransformer`에 HTTP 응답 데이터를 전달해 JSON 형식으로 변환된 결과를 받는다.
2. `bulk_insert`: 주어진 INSERT 쿼리를 가지고 JSON 형식의 데이터를 DB 테이블에 적재한다.

`common/transform.py` 모듈 내에는 DuckDB 엔진을 사용하는 `DuckDBTransformer`만 구현되어 있다.

### `common/load.py`

> DB와의 연결 및 CRUD 작업을 지원하는 클래스를 `Connection` 추상 클래스에 기반해 정의한다.

인메모리 DuckDB 연결을 돕는 `DuckDBConnection`만 구현되어 있다.   
`DuckDBConnection`은 `DuckDBTransformer`의 내부 연결 객체로 사용된다.

### `common/models.py`

DB 테이블에 데이터를 적재할 때 사용할 SQL문을 별도의 `.sql` 파일에 기록해두고 읽어올 때   
`Models` 클래스를 사용한다.

`core/` 아래에 `models.sql` 파일에는 여러 개의 SQL문이 `-- 이름: 키` 주석으로 구분되어 있다.   
`Models` 클래스는 `.sql` 파일을 읽어오면서 각각의 SQL문을 파싱하는 역할을 한다.

## core/

`core/` 경로에는 ETL 프로세스를 구성하는 핵심 로직을 구현한다.

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

현재 구현된 핵심 로직을 아래 표로 정리할 수 있다.

| 플랫폼 | 호스트명 | 카테고리 | 플랫폼 구분 | 수집 범위 |
| --- | --- | --- | --- | --- |
| `cj` | `eflexs` | `stock` | CJ대한통운 eFLEXs | 재고 |
| `coupang` | `advertising` | `report` | 쿠팡 광고센터 | 광고 |
| `coupang` | `wing` | `product`, `settlement` | 쿠팡 판매자센터 | 상품, 매출 |
| `ecount` | `api` | `inventory`, `product` | 이카운트 API | 상품, 재고 |
| `google` | `api` | `ads` | 구글 API | 광고 |
| `meta` | `api` | `ads` | 메타 API | 광고 |
| `naver` | `main` | `search` | 네이버 메인 | 검색 |
| `naver` | `openapi` | `search` | 네이버 오픈 API | 검색 |
| `sabangnet` | `admin` | `account`, `order`, `product` | 사방넷 시스템 | 주문, 상품 |
| `searchad` | `api` | `contract`, `keyword`, `report` | 네이버 검색광고 API | 광고 보고서, 광고 계약, 검색량 |
| `searchad` | `center` | `exposure`, `report` | 네이버 광고주센터 (검색광고) | 광고 보고서, 광고 순위 |
| `searchad` | `gfa` | `report` | 네이버 성과형 디스플레이 광고 | 광고 보고서 |
| `smartstore` | `api` | `bizdata`, `order`, `product` | 네이버 커머스 API | 주문, 상품, 통계 |
| `smartstore` | `hcenter` | `catalog`, `pageview`, `sales` | 네이버 쇼핑파트너센터 | 매출, 방문 통계, 카탈로그/상품 |

`core/` 경로의 플랫폼과 호스트명 조합은 `api/`의 하위 경로와 대응된다.

```bash
api/{platform}/{hostname}.py
```

## extensions/

`extensions` 경로에는 외부 시스템과 연동하는 로직을 구현한다.

### `bigquery.py`

> [`google-cloud-bigquery`](https://pypi.org/project/google-cloud-bigquery/)
> 라이브러리에 의존하여 BigQuery 대상 쿼리 실행 기능을 구현한다.

- `ServiceAccount`: GCP 서비스 계정에 대한 로컬 JSON 키 파일을 불러온다.
- `BigQueryClient`: BigQuery 쿼리 실행을 담당하며, DuckDB와의 연동을 지원한다.

BigQuery 테이블에 대규모 데이터를 적재하는 Load Job을 활용한 5가지 메서드가 제공된다.

| 메서드 | 데이터 소스 | 추가 설명 |
| --- | --- | --- |
| `load_table_from_json` | `list[dict]` | 파이썬 객체에 대한 JSON 직렬화 지원 |
| `load_table_from_file` | 파일 스트림 | Avro, CSV, JSON, ORC, Parquet 포맷과 명시적 스키마 지원 |
| `load_table_from_duckdb` | DuckDB 테이블 | Load Job 적재 |
| `load_table_from_duckdb_by_partition` | DuckDB 테이블 | 파티션별 분할 적재 지원 |
| `overwrite_table_from_duckdb` | DuckDB 테이블 | 스테이징 테이블을 만든 후 대상 범위를 삭제하고 다시 적재 |
| `merge_table_from_duckdb` | DuckDB 테이블 | 스테이징 테이블을 만든 후 두 테이블을 병합 |

### `postgres.py`

> [`psycopg2`](https://pypi.org/project/psycopg2/)
> 라이브러리에 의존하여 PostgreSQL 대상 쿼리 실행 기능을 구현한다.

- `PostgresClient`: PostgreSQL 쿼리 실행을 담당하며, DuckDB와의 연동을 지원한다.
- `ensure_cursor`: PostgreSQL 커서 생성, 커밋, 롤백, 종료 정책을 지원하는 데코레이터.

PostgreSQL 테이블과 파일/객체/DuckDB 데이터를 연결하는 주요 메서드는 다음과 같다.

| 메서드 | 데이터 소스 | 추가 설명 |
| --- | --- |
| `insert_into_table` | 파일 또는 Python 객체 | CSV, JSON, Parquet 포맷을 읽어 테이블에 삽입 |
| `load_table_from_duckdb` | DuckDB 테이블 | DuckDB 소스 테이블 행을 PostgreSQL 테이블에 삽입 |
| `overwrite_table_from_duckdb` | DuckDB 테이블 | 스테이징 테이블을 만든 후 대상 범위를 삭제하고 다시 삽입 |
| `merge_table_from_duckdb` | DuckDB 테이블 | 스테이징 테이블을 만든 후 두 테이블을 병합 |

Parquet 읽기/쓰기는 PostgreSQL 자체 확장 기능인 `parquet_io`를 사용한다.

Parquet 파일 경로를 Python 메서드에 전달하면 클라이언트 프로세스가 파일을 읽어 `BYTEA` 타입으로 PostgreSQL에 전달한다.
따라서, PostgreSQL 서버가 Docker 컨테이너 또는 원격 서버에 있어도 클라이언트에서 파일을 적재할 수 있다.

### `gsheets.py`

> [`gspread`](https://pypi.org/project/gspread/)
> 라이브러리에 의존하여 Google Sheets 대상 읽기 쓰기 기능을 구현한다.

- `ServiceAccount`: GCP 서비스 계정에 대한 로컬 JSON 키 파일을 불러온다.
- `WorksheetClient`: Google Sheets의 키와 시트명을 대상으로 워크시트에 대한 읽기 쓰기를 담당한다.
- `worksheet2py`, `py2worksheet`: 날짜, 퍼센트, 불리언 등을 Python과 Google Sheets 간의 타입 호환을 지원한다.

Google Sheets의 워크시트 대상 읽기 쓰기를 위한 4가지 주요 메서드가 제공된다.

- `get_all_records`: 워크시트 전체를 `list[dict]` 형식으로 읽기
- `update_worksheet`: 워크시트의 지정된 위치에 `list[dict]` 형식의 데이터 덮어쓰기
- `overwrite_worksheet`: 워크시트의 기존 행을 제거하고 데이터 덮어쓰기
- `upsert_worksheet`: 워크시트의 기존 행을 읽고 새로운 데이터를 업데이트

## utils/

`utils/` 경로에는 다양한 모듈에서 사용하는 사소한 유틸리티 함수를 구현한다.

- `cast.py`: 안전한 타입 캐스팅
- `date.py`: 날짜 범위 분할 또는 시간대 변환 등
- `excel.py`: 엑셀 생성 및 서식 적용
- `graphql.py`: GraphQL 쿼리 빌드
- `headers.py`: 크롬 브라우저 HTTP 헤더 빌드
- `nested.py`: 중첩된 딕셔너리 조회
- `parse.py`: HTML 소스코드 파싱
- `progress.py`: tqdm 진행도 출력
- `regex.py`: 정규표현식 보조

## SQL 모델 규칙

`core/**/models.sql` 파일은 `DuckDBTransformer`가 사용하는 SQL 쿼리문 저장소다.

Jinja 템플릿과 DuckDB 파라미터를 지원하며 주요 표현식은 다음과 같다.

| 표현식 | 의미 |
| --- | --- |
| `{{ table }}` | 대상 테이블명 (단일 테이블의 경우) |
| `{{ rows }}` | JSON 변환된 데이터에 대한 SELECT 서브 쿼리 |
| `$param_name` | DuckDB 쿼리 실행 시 전달한 파라미터 |

## ETL 기능 추가 규칙

새로운 ETL 핵심 로직을 추가할 때는 보통 다음 순서를 따른다.

1. `core/{platform}/{hostname}/{category}/extract.py` 파일에 `Extractor` 클래스를 구현한다.
2. `core/{platform}/{hostname}/{category}/transform.py` 파일에 `DuckDBTransformer`를 구현한다.
3. `core/{platform}/{hostname}/{category}/models.sql` 파일에 `create` 및 `bulk_insert` 쿼리문을 정의한다.
4. `api/{platform}/{hostname}.py` 파일에 `core/` 로직을 연결하는 API 함수를 추가한다.
5. `src/tests/test_extract.py`, `src/tests/test_transform.py` 파일에 테스트를 추가한다.

## 사용 예시

```python
from linkmerce.api.smartstore.api import marketing_channel
from linkmerce.common.load import DuckDBConnection

with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
    rows = marketing_channel(
        client_id = "...",
        client_secret = "...",
        channel_seq = "...",
        start_date = "2025-01-01",
        end_date = "2025-01-31",
        connection = conn,
        return_type = "json",
    )

print(len(rows))
```

이 호출은 API 함수를 통해 `Extractor`와 `DuckDBTransformer`를 연쇄적으로 실행하고,   
결과를 DuckDB 테이블에 적재한 뒤, 테이블 행을 조회하여 `list[dict]` 형식으로 반환한다.
