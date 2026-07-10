# Apache Airflow 실행 안내

> LinkMerce 프로젝트의 Dag, 플러그인, Docker Compose 실행 환경을 설명한다.

## 목차

- [개요](#개요)
- [한눈에 보기](#한눈에-보기)
- [디렉터리 구조](#디렉터리-구조)
- [로컬 실행 환경](#로컬-실행-환경)
- [표준 Dag 패턴](#표준-dag-패턴)
- [dbt 후속 실행 패턴](#dbt-후속-실행-패턴)
- [Dag 목록](#dag-목록)
- [Dag 태그](#dag-태그)
- [플러그인](#플러그인)
- [Airflow Trigger](#airflow-trigger)

## 개요

Dag은 공통적으로 다음 흐름을 따른다.

1. `airflow_utils` 플러그인의 `read_config()` 또는 `read_credentials()` 함수로 설정과 인증 정보를 불러온다.
2. `linkmerce.api.*` 함수를 호출해 추출(extract)과 변환(transform) 과정을 수행한다.
3. `DuckDBConnection`을 통해 테이블에 적재된 API 실행 결과를 불러올 수 있다.
4. `dual_load` 플러그인을 통해 DuckDB 테이블을 PostgreSQL과 BigQuery에 함께 적재한다.
5. Task 결과를 `{params: {...}, results: {...}}` 딕셔너리로 반환한다.
6. 후속 dbt 모델이 있는 Dag은 적재 결과의 파티션 범위를 계산한 뒤 `cosmos.DbtTaskGroup`으로 `dbt_bigquery` 모델을 실행한다.

## 한눈에 보기

- **베이스 이미지**: `apache/airflow:3.2.2`
- **핵심 의존성**: `linkmerce`, `gspread`, `google-cloud-bigquery`, `psycopg2-binary`, `playwright==1.60.0`, `astronomer-cosmos==1.14.2`, `dbt-bigquery==1.11.1`
- **Providers**: `apache-airflow-providers-slack`

## 디렉터리 구조

```bash
airflow/
├── config/
│   └── airflow.cfg
├── dags/
│   ├── _deprecated/
│   ├── cj/
│   ├── coupang/
│   ├── ecount/
│   ├── gsheets/
│   ├── naver/
│   ├── sabangnet/
│   ├── searchad/
│   ├── smartstore/
│   └── ss_hcenter/
├── files/
├── logs/
├── plugins/
│   ├── airflow_api.py
│   ├── airflow_patches.py
│   ├── airflow_utils.py
│   ├── dbt_cosmos.py
│   ├── dual_load.py
│   └── pw_actions.py
├── scripts/
├── docker-compose.yaml
└── init.sh
```

## 로컬 실행 환경

`docker-compose.yaml` 설정은 Airflow Celery Executor 구성을 기준으로 다음 서비스를 실행한다.

- `postgres`
- `redis`
- `playwright`
- `airflow-apiserver`
- `airflow-scheduler`
- `airflow-dag-processor`
- `airflow-worker`
- `airflow-triggerer`
- `airflow-init`

API 서버에 대한 볼륨 마운트도 현재 저장소 구조를 기준으로 연결된다.

- `./config -> /opt/airflow/config`
- `./dags -> /opt/airflow/dags`
- `./files -> /opt/airflow/files`
- `./logs -> /opt/airflow/logs`
- `./plugins -> /opt/airflow/plugins`
- `../src/env -> /opt/airflow/files/env`
- `../src/linkmerce -> /opt/airflow/plugins/linkmerce`
- `../dbt_bigquery -> /opt/airflow/dbt/dbt_bigquery`

Docker Compose 실행은 다음 명령어 또는 `init.sh` 스크립트를 실행한다.

```bash
docker compose up airflow-init
docker compose up -d
```

실행 중인 API 서버에 접속할 때는 `scripts` 경로의 `exec_api.sh` 스크립트를 실행한다.

```bash
./scripts/exec_api.sh
---------------------
/opt/airflow$
```

Airflow의 MetaDB에 접속할 때는 `scripts` 경로의 `exec_db.sh` 스크립트를 실행한다.

```bash
./scripts/exec_db.sh
--------------------
psql (13.23 (Debian 13.23-1.pgdg13+1))
Type "help" for help.

airflow=#
```

## 표준 Dag 패턴

일반적인 Dag은 다음 패턴을 공유한다.

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

핵심 특징은 다음과 같다.

- 설정은 `airflow_utils` 플러그인의 `read_config` 함수를 통해 불러온다.
- 여러 개의 계정에 대한 ETL 프로세스를 독립적으로 실행할 경우 Dynamic Task Mapping을 활용한다.
- 실제 ETL 프로세스는 `linkmerce.api.*` 모듈의 API 함수에 위임한다.
- 실행 결과는 `DuckDBConnection`의 테이블에 적재되며, 이 연결을 `dual_load` 플러그인에 넘겨준다.
- PostgreSQL과 BigQuery 테이블에는 `APPEND`, `OVERWRITE`, `MERGE` 3가지 방식 중 한 가지 방식으로 적재한다.
- `dual_load`는 PostgreSQL 적재를 먼저 수행하여 기본키와 타입 제약을 검증한 뒤 BigQuery 적재를 실행한다.

일부 복잡한 Dag은 다음과 같은 전략을 사용한다.

- `BranchPythonOperator`: 분기에 따라 선택적으로 Task를 실행하는 경우
- `MultipleCronTriggerTimetable`: 여러 개의 크론탭으로 스케줄을 표현해야 하는 경우
- `Playwright`: 브라우저 활용이 필요한 어려운 스크래핑 작업을 처리하는 경우
- `PythonSensor`: 외부 Dag 실행 완료 여부를 API로 확인해야 하는 경우
- `ShortCircuitOperator`: 후속 실행 조건이 없을 때 downstream Task를 건너뛰는 경우
- `TaskGroup`: 여러 개의 Task를 하나의 그룹으로 묶을 경우
- `TriggerDagRunOperator`: Task 실행 전후로 다른 Dag을 호출할 경우
- `DbtTaskGroup`: ETL 결과를 바탕으로 `dbt_bigquery` 후속 모델을 실행하는 경우

## dbt 후속 실행 패턴

BigQuery 후속 모델을 실행하는 Dag은 ETL 완료 후 다음 흐름을 추가한다.

```python
dbt_date_range = generate_dbt_date_range(etl_result)
dbt_run = dbt_bigquery_example_group()

dbt_date_range >> prepare_dbt_run() >> dbt_run
```

핵심 규칙은 다음과 같다.

- ETL Task는 dbt 기간 계산에 사용할 날짜 배열을 `context.partitions`에 담아 반환한다.
   - 파티션 테이블은 실제 적재된 원천 테이블의 파티션 날짜를 반환한다.
   - 파티션이 없는 경우에는 Dag run 실행일 기준 전일부터 현재까지의 기간을 일별로 생성해 반환한다.
- `generate_dbt_date_range` Task는 `context.partitions`를 파싱해 `ds_start_date`, `ds_end_date` 변수를 계산한다.
- `prepare_dbt_run` Task는 `ShortCircuitOperator`로 구현하며, 날짜 범위가 없으면 dbt 실행을 건너뛴다.
- `dynamic_mapping_dbt_bigquery` 함수로 `DbtTaskGroup`을 생성하면서 날짜 범위를 XCom으로 전달한다.
- dbt 실행 대상은 `dbt_bigquery/selectors.yml`의 selector로 관리하며, selector 이름은 Dag ID와 맞춘다.
- ETL 또는 dbt 하위 Task 실패를 Dag run 최종 상태에 반영해야 하는 경우 마지막에 `finalize_dag_run` Task를 놓는다.

## Dag 목록

플랫폼 명칭별 하위 경로에 관련된 Dag을 묶어서 배치한다.

Dag 파일명과 Dag ID는 다를 수 있다.

### CJ대한통운 (cj)

| Dag ID | 스케줄 | 역할 |
| --- | --- | --- |
| `cj_eflexs_stock` | 매일 `09:00`, `17:00` | CJ대한통운 eFLEXs 상세재고조회 ETL |
| `cj_loisparcel_invoice` | 매일 `02:00` | CJ대한통운 로이스파셀 기업고객일별배송상세 ETL |

### 쿠팡 (coupang)

| Dag ID | 스케줄 | 역할 |
| --- | --- | --- |
| `coupang` | 매일 `09:00`, `17:00`, `23:00` | 판매자별 로그인 후 시간대별 downstream Dag을 트리거하는 통합 오케스트레이터 |
| `coupang_adreport` | 트리거 전용 | 쿠팡 광고 보고서 ETL |
| `coupang_campaign` | 트리거 전용 | 쿠팡 광고 캠페인/광고그룹/소재 ETL |
| `coupang_inventory` | 트리거 전용 | 쿠팡 로켓그로스 재고현황 ETL |
| `coupang_product_option` | 트리거 전용 | 쿠팡 상품 옵션 ETL |
| `coupang_rocket_sales` | 트리거 전용 | 쿠팡 로켓그로스 정산 리포트 ETL |

### 이카운트 (ecount)

| Dag ID | 스케줄 | 역할 |
| --- | --- | --- |
| `ecount_inventory` | 매일 `09:00`, `17:00` | 이카운트 재고현황 ETL |
| `ecount_product` | 평일 `08:50`, `16:50` | 이카운트 품목등록 리스트 ETL |

### 구글 시트 (gsheets)

| Dag ID | 스케줄 | 역할 |
| --- | --- | --- |
| `sync_gsheets` | 트리거 전용 | `task_ids`에 해당하는 구글 시트를 읽어 DB 테이블에 덮어쓰기 |
| `sync_gsheets__ads_master` | 트리거 전용 | 광고ID-품번코드 구글 시트 동기화 후 dbt 모델 실행 |
| `sync_gsheets__expense` | 트리거 전용 | 고정지출 구글 시트 동기화 후 dbt 모델 실행 |
| `sync_gsheets__extra_ads` | 트리거 전용 | 기타광고 구글 시트 동기화 후 dbt 모델 실행 |
| `sync_gsheets__extra_sales` | 트리거 전용 | 기타매출 구글 시트 동기화 후 dbt 모델 실행 |
| `sync_gsheets__opex` | 트리거 전용 | 운영비용 구글 시트 동기화 후 dbt 모델 실행 |
| `sync_gsheets__order_status` | 트리거 전용 | 주문상태 구글 시트 동기화 후 dbt 모델 실행 |

### 네이버 메인 (naver)

| Dag ID | 스케줄 | 역할 |
| --- | --- | --- |
| `naver_cafe_search` | `08:00-09:50` 10분 간격 | 네이버 카페 검색 모니터링 |
| `naver_main_search` | `08:00-09:50` 10분 간격 | 네이버 통합검색 모니터링 파이프라인 |
| `naver_shop_rank` | 매일 `06-18시` 정각, 비활성화 | 네이버 쇼핑 검색 순위 ETL |

### 사방넷 (sabangnet)

| Dag ID | 스케줄 | 역할 |
| --- | --- | --- |
| `sabangnet_order` | 매일 `23:30`, 트리거 지원 | 사방넷 발주 내역 ETL |
| `sabangnet_invoice` | 평일 `10:30`, `14:30`, `23:50` | 사방넷 주문 ETL |
| `sabangnet_product` | 평일 `23:20` | 사방넷 상품/옵션/매핑 ETL |

### 네이버 광고 (searchad)

| Dag ID | 스케줄 | 역할 |
| --- | --- | --- |
| `searchad_contract` | 평일 `05:30` | 네이버 검색광고 계약 정보 ETL |
| `searchad_login_gfa` | 매일 `05:00` | 네이버 GFA 로그인 상태 갱신 |
| `searchad_master_gfa` | 평일 `05:30` | 네이버 GFA 캠페인/광고 그룹/소재 ETL |
| `searchad_master_sad` | 평일 `23:40` | 네이버 검색광고 마스터 보고서 ETL |
| `searchad_report_gfa` | 매일 `05:20` | 네이버 GFA 성과 보고서 ETL |
| `searchad_report_sad` | 매일 `05:40` | 네이버 검색광고 다차원 보고서 ETL |

### 네이버 스마트스토어 (smartstore)

| Dag ID | 스케줄 | 역할 |
| --- | --- | --- |
| `smartstore_bizdata` | 매일 `08:10`, 비활성화 | 마케팅 채널 데이터 적재 |
| `smartstore_invoice` | `03:00` 매일, `10:30`/`15:00` 평일 | 송장/주문 후속 상태 갱신 |
| `smartstore_order` | 매일 `08:30` | 주문, 상품주문, 배송, 옵션, 변경 주문 상태 적재 |
| `smartstore_product` | 평일 `23:30` | 상품/옵션 카탈로그 적재 |

### 네이버 쇼핑파트너센터 (ss_hcenter)

| Dag ID | 스케줄 | 역할 |
| --- | --- | --- |
| `naver_hcenter_login` | 매일 `01:00` | 쇼핑파트너센터 로그인 상태 갱신 |
| `naver_brand_price` | 매일 `00:01` | 네이버 브랜드 상품 가격 ETL |
| `naver_product_catalog` | 트리거 전용 | 네이버 카탈로그-상품 매핑 ETL |

### 기타

하위 경로로 분류하지 않은 Dag은 `dags/` 경로 아래에 배치한다.

| Dag ID | 스케줄 | 역할 |
| --- | --- | --- |
| `google_ads` | 매일 `07:50` | 구글 광고 ETL |
| `meta_ads` | 매일 `07:40` | 메타 광고 ETL |
| `postgres_partman_maintenance` | 매일 `00:00` | PostgreSQL pg_partman 파티션 갱신 |
| `stock_report` | 평일 `11:00`, `17:30` | 재고 내역을 엑셀 파일로 변환해 Slack 업로드 |

스크래핑 대상 웹사이트의 구조적 변경으로 비활성화된 Dag은 `dags/_deprecated/` 경로로 옮긴다.

비공개 Dag은 `.gitignore` 대상인 `dags/_privated/` 경로로 옮긴다.

## Dag 태그

각 Dag의 `tags`는 Airflow UI에서 실행 목적과 운영 특성을 빠르게 필터링하기 위한 메타데이터다.

태그는 `prefix:value` 형식으로 작성하며, 하나의 Dag이 여러 플랫폼, 목적, 적재 방식 등을 가진다면
같은 prefix에 다른 value인 태그를 여러 개 지정할 수 있다.

| prefix | 설명 | value 예시 |
| --- | --- | --- |
| `priority` | 운영 중요도 또는 장애 대응 우선순위 | `high`, `medium`, `low` |
| `platform` | 웹 스크래핑 대상 플랫폼 또는 작업 대상 | `smartstore`, `coupang-wing`, `gsheets` |
| `objective` | Dag이 다루는 데이터의 활용 목적 | `sales`, `stock`, `ads` |
| `credentials` | Dag 실행 중 사용하는 인증 방식 | `api-key`, `userid`, `cookies` |
| `schedule` | Dag 스케줄 실행 주기 | `daily`, `weekdays`, `none` |
| `time` | Dag 스케줄 실행 시간대 | `morning`, `afternoon`, `night` |
| `write` | 외부 저장소 적재 방식 | `append`, `overwrite`, `merge` |
| `plugin` | Dag 실행 중 사용하는 Airflow 플러그인 | `dbt`, `playwright`, `rest-api` |
| `provider` | Dag 실행 중 사용하는 외부 Provider | `slack` |
| `upstream` | Dag 실행을 트리거하는 대상 | `dagrun`, `fastapi`, `streamlit` |
| `status` | 운영 상태 | `disabled`, `private` |

`schedule:none` 태그는 Airflow 스케줄이 없는 경우에 지정한다.

`status:disabled` 태그는 Dag이 비활성화된 경우 지정하며,
`_private/` 경로의 비공개 Dag은 `status:private` 태그를 부여한다.

현재 사용 중인 전체 태그 목록을 `prefix: [ ...values ]` 형식에 맞춰 정리하면 다음과 같다.

```yaml
credentials: [ api-key, cookies, service-account, userid ]
objective: [
    ads, alert, cost, delivery, login, maintenance, mapping,
    price, product, rank, sales, search, statistics, stock
]
platform: [
    cj-eflexs, cj-loisparcel, coupang-ads, coupang-wing, ecount,
    google-ads, gsheets, meta-ads, naver-hcenter, naver-main, naver-shop,
    postgres, sabangnet, searchad, smartstore
]
plugin: [ dbt, playwright, rest-api ]
priority: [ high, medium, low ]
provider: [ slack ]
schedule: [ daily, hourly, weekdays, none ]
status: [ disabled, private ]
time: [ afternoon, morning, night ]
upstream: [ dagrun, fastapi, streamlit, extension ]
write: [ append, overwrite, merge, file ]
```

## 플러그인

`plugins/` 경로는 Airflow에서 제공하는 플러그인 시스템이며, Dag에서 공용으로 사용하는 기능을 제공한다.

### `airflow_utils.py`

대부분의 Dag에서 공통으로 사용하는 유틸리티 함수를 제공한다.

- `read_config`: Airflow Variable로 등록한 설정 파일 경로의 내용 읽기
- `read_credentials`: 인증 정보를 읽으면서 쿠키 등 파일 참조를 실제 내용으로 치환
- `in_timezone`: `pendulum` 날짜에 시간대 설정 및 timedelta 연산
- `today`: 현재 시각을 지정한 시간대의 `pendulum` 객체로 반환
- `get_datetime`: Task context의 `data_interval_end`를 지정한 시간대의 `pendulum` 객체로 반환
- `format_datetime`: `get_datetime` 실행 결과를 날짜 문자열로 변환

### `airflow_api.py`

통합 오케스트레이터 Dag에서 Airflow REST API를 호출하여 다른 Dag을 동적으로 실행할 때 활용된다.

- `authenticate`: Airflow JWT 액세스 토큰을 발급
- `request`: Airflow REST API에 대한 HTTP 요청
- `trigger_dagrun`: Dag ID에 대한 Dag 실행을 트리거
- `wait_for_completion`: Dag run ID에 대한 Dag 실행 대기
- `get_xcom_value`: Dag run의 특정 Task에서 XCom 값을 조회

### `dbt_cosmos.py`

Astronomer Cosmos 기반으로 dbt 프로젝트를 Airflow TaskGroup에 연결하는 함수를 제공한다.

- `generate_date_array`: 선행 Task 결과에서 날짜 배열을 추출하고 중복 없는 단일 배열로 변환
- `generate_dbt_date_range`: 날짜 배열을 `ds_start_date`, `ds_end_date` dbt 변수로 변환
- `dynamic_mapping_dbt_bigquery`: Airflow Variable `dbt_bigquery` 설정과 selector를 조합해 `DbtTaskGroup` 생성
- `raise_on_failure`: ETL 또는 dbt Task 실패를 Dag run 마지막에 다시 반영하기 위한 목적

`dynamic_mapping_dbt_bigquery`는 Airflow Variable `dbt_bigquery`에서 다음 설정을 읽는다.

- `project_config`: dbt 프로젝트 경로와 dependency 설치 여부
- `profile_config`: BigQuery profile, target, connection, location, thread 수, job timeout
- `operator_args`: Cosmos dbt operator에 공통으로 전달할 인자. pool을 사용할 경우 이 값에 지정한다.

`ds_task_id`를 전달하면 해당 Task의 XCom에서 `ds_start_date`, `ds_end_date`를 꺼내
dbt `vars`로 전달한다.
Dag 코드에서는 직접 `vars`를 구성하지 않고 `ds_task_id`를 넘기는 방식을 표준으로 사용한다.

### `airflow_patches.py`

Airflow Variable `test_mode`가 `true`인 조건에서 다음 동작을 런타임 패치한다.

- `BigQueryClient`와 `PostgresClient`의 연결 생성을 차단하고 내부 connection 값을 `None`으로 둔다.
- BigQuery/PostgreSQL의 DuckDB 적재 메서드를 차단하고 DuckDB 테이블 미리보기(`LIMIT 5`)를 반환한다.
- `DbtTaskGroup` 하위 Cosmos operator의 `execute` 메서드를 비워 dbt Task 실행을 건너뛴다.
- Slack 파일 전송을 차단하고 파일 메타데이터만 포함한 가상의 응답 결과를 반환한다.

즉, test mode에서는 외부 적재, dbt 실행, Slack 파일 업로드를 모두 생략하고
Dag 오케스트레이션과 DuckDB 기준 결과만 확인할 수 있다.

### `dual_load.py`

DuckDB 테이블을 PostgreSQL과 BigQuery에 함께 적재하는 표준 helper를 제공한다.

| 함수 | PostgreSQL 동작 | BigQuery 동작 |
| --- | --- | --- |
| `load_table_from_duckdb` | `INSERT ...` | Load Job (`WRITE_APPEND`) |
| `overwrite_table_from_duckdb` | `DELETE ... WHERE ...; INSERT ...` | `DELETE ... WHERE ...; INSERT ...` |
| `merge_table_from_duckdb` | `MERGE ... WHEN MATCHED THEN ...` | `MERGE ... WHEN MATCHED THEN ...` |
| `overwrite_table_from_gsheets` | `TRUNCATE TABLE ...; INSERT ...` | Load Job (`WRITE_TRUNCATE`) |

DuckDB를 데이터 소스로 읽는 `*_from_duckdb` 함수는 공통적으로
적재한 소스 행 수와 백엔드별 성공 여부를 반환한다.

```python
{
    "table": "dataset.table",
    "count": 100,
    "pg_success": True,
    "bq_success": True,
}
```

구글 시트를 데이터 소스로 읽는 `*_from_gsheets` 함수는
적재 전 구글 시트와 적재 대상 BigQuery 테이블을 비교하여 신규로 추가된 행 목록을 반환한다.
이 반환값은 `sync_gsheets__*` Dag의 후속 dbt 실행 조건과 날짜 범위 계산에 사용된다.

PostgreSQL 연결 정보는 Airflow Connection `postgres`,
BigQuery 연결 정보는 Airflow Connection `gcp_bigquery`에서 읽는다.

### `pw_actions.py`

Playwright WebSocket 서버를 사용해 브라우저 자동화를 수행한다.

- `login_coupang`: 쿠팡 Wing 로그인 후 '광고센터' 탭까지 이동해 쿠키 수집
- `login_naver`: 저장된 세션 또는 재로그인으로 네이버 쿠키 수집
- `get_browser_cookies`: 세션 쿠키가 생길 때까지 대기 후 쿠키를 문자열로 반환

단, `playwright` 서비스가 비활성화되었을 경우 해당 함수를 사용할 수 없다.

## Airflow Trigger

상위의 `airflow_trigger/` 경로에는 운영 담당자가 Airflow UI에 직접 들어가지 않고도
필요한 Dag만 실행할 수 있도록 지원하는 서비스가 위치한다.

```bash
airflow_trigger/
├── fastapi/
└── streamlit/
```

두 가지 서비스 모두 Docker Compose로 실행할 수 있다.

| 구분 | FastAPI | Streamlit |
| --- | --- | --- |
| 주 용도 | URL에 접속하여 Dag 트리거 | 사용자가 Dag 목록과 설정을 입력해 트리거 |
| 대상 Dag | `sync_gsheets` | `sabangnet_order` |
| 입력 방식 | Path, Parameter | 설정 입력 폼 |

### FastAPI

`airflow_trigger/fastapi` 경로에는 구글 시트 관련 Dag을 트리거하기 위한 HTTP 서버가 위치한다.
브라우저용 `/trigger/...` 경로와 프로그램 호출용 `/api/trigger/...` 경로를 함께 제공하며,
요청마다 Airflow 액세스 토큰을 발급받아 Dag run을 생성한다.

`/trigger/...` 경로는 Dag 실행 중에만 화면을 표시하며, 실행이 완료되면 페이지를 자동으로 닫는다.

`/api/trigger/...` 경로는 실행 종료 후 JSON 응답 결과를 반환한다.

| 경로 | 실행 대상 | 실행 방식 |
| --- | --- | --- |
| `/trigger/gsheets` | `sync_gsheets` | `task_ids` 파라미터를 배열로 변환해 `dag_run.conf`에 전달 |
| `/trigger/gsheets/{target}` | `sync_gsheets__{target}` | `target`에 해당하는 Dag run 생성 |

지정 가능한 `target`은 [구글 시트 (gsheets)](#구글-시트-gsheets) 문단을 참조한다.

`/trigger/gsheets` 경로는 `target` 목록을
쉼표로 구분한 형태의 `task_ids` 문자열을 파라미터로 입력받는다.

`/trigger/gsheets/{target}` 경로는 dbt 후속 실행을 제어할 수 있는
다음의 파라미터를 입력받는다. 파라미터가 주어진다면
`dag_run.conf["dbt"]` 내부에 키-값 형태로 추가되어 Dag run 생성 시 전달된다.

- `ds_start_date`: dbt 변수, 실행 대상의 날짜 파티션 시작일
- `ds_end_date`: dbt 변수, 실행 대상의 날짜 파티션 종료일
- `run`: dbt 실행 여부
- `wait`: Dag 실행이 끝날 때까지 대기할지 여부

### Streamlit

`airflow_trigger/streamlit` 경로에는 사용자가 Dag 목록과 설정을 직접 입력해
트리거할 수 있는 UI를 제공하는 애플리케이션이 위치한다.

현재는 `sabangnet_order` Dag에 대해서만 지원하며,
사방넷에서 주문을 수집할 차수 및 시작/종료 일시를 선택해 대상 Dag을 트리거할 수 있다.

| 입력 값 | Airflow Dag run에 반영되는 값 |
| --- | --- |
| 시작 일시 | `logical_date`, `data_interval_start` |
| 종료 일시 | `data_interval_end`, `dag_run_id`의 접미사 |
| 차수 | `dag_run_id`의 `api__{차수명}__...` 형식 |

Streamlit 앱은 KST 기준으로 지정한 시작/종료 일시를
UTC 기준의 `logical_date`, `data_interval_start`, `data_interval_end`로 변환한 뒤
`api__{차수명}__{종료일시}` 형식의 ID를 가지는 Dag run을 생성한다.
ID가 겹치는 Dag run이 있으면 삭제 후 다시 생성한다.
