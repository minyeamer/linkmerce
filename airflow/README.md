# Apache Airflow 실행 안내

> LinkMerce 프로젝트의 Dag, 플러그인, Docker Compose 실행 환경을 설명한다.

## 목차

- [개요](#개요)
- [한눈에 보기](#한눈에-보기)
- [디렉터리 구조](#디렉터리-구조)
- [로컬 실행 환경](#로컬-실행-환경)
- [표준 Dag 패턴](#표준-dag-패턴)
- [Dag 구조](#dag-구조)
- [플러그인](#플러그인)

## 개요

Dag은 공통적으로 다음 흐름을 따른다.

1. `airflow_utils` 플러그인의 `read_config()` 또는 `read_credentials()` 함수로 설정과 인증 정보를 불러온다.
2. `linkmerce.api.*` 함수를 호출해 추출(extract)과 변환(transform) 과정을 수행한다.
3. `DuckDBConnection`을 통해 테이블에 적재된 API 실행 결과를 불러올 수 있다.
4. `dual_load` 플러그인을 통해 DuckDB 테이블을 PostgreSQL과 BigQuery에 함께 적재한다.
5. Task 결과를 `{params: {...}, results: {...}}` 딕셔너리로 반환한다.

## 한눈에 보기

- **베이스 이미지**: `apache/airflow:3.2.1-python3.12`
- **핵심 의존성**: `linkmerce`, `gspread`, `google-cloud-bigquery`, `psycopg2-binary`, `playwright==1.60.0`
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
- 실행 결과는 `DuckDBConnection`의 테이블에 적재되며, 이 연결을 `dual_load` 플로그인에 넘겨준다.
- PostgreSQL과 BigQuery 테이블에는 `APPEND`, `OVERWRITE`, `UPSERT/MERGE` 3가지 방식 중 한 가지 방식으로 적재한다.
- `dual_load`는 PostgreSQL 적재를 먼저 수행하여 기본키와 타입 제약을 검증한 뒤 BigQuery 적재를 실행한다.

일부 복잡한 DAG은 다음과 같은 전략을 사용한다.

- `BranchPythonOperator`: 분기에 따라 선택적으로 Task를 실행하는 경우
- `MultipleCronTriggerTimetable`: 여러 개의 크론탭으로 스케줄을 표현해야 하는 경우
- `Playwright`: 브라우저 활용이 필요한 어려운 스크래핑 작업을 처리하는 경우
- `PythonSensor`: 외부 Dag 실행 완료 여부를 API로 확인해야 하는 경우
- `TaskGroup`: 여러 개의 Task를 하나의 그룹으로 묶을 경우
- `TriggerDagRunOperator`: Task 실행 전후로 다른 Dag을 호출할 경우

## Dag 구조

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

### 네이버 메인 (naver)

| Dag ID | 스케줄 | 역할 |
| --- | --- | --- |
| `naver_cafe_search` | `08:00-09:50` 10분 간격 | 네이버 카페 검색 모니터링 |
| `naver_main_search` | `08:00-09:50` 10분 간격 | 네이버 통합검색 모니터링 파이프라인 |
| `naver_shop_rank` | 매일 `06-18시` 정각 | 네이버 쇼핑 검색 순위 ETL |

### 사방넷 (sabangnet)

| Dag ID | 스케줄 | 역할 |
| --- | --- | --- |
| `sabangnet_order` | 매일 `23:30` | 사방넷 발주 내역 ETL |
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
| `smartstore_bizdata` | 매일 `08:10` | 마케팅 채널 데이터 적재 |
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

하위 경로로 분류하지 않은 DAG은 `dags/` 경로 아래에 배치한다.

| Dag ID | 스케줄 | 역할 |
| --- | --- | --- |
| `google_ads` | 매일 `07:50` | 구글 광고 ETL |
| `meta_ads` | 매일 `07:40` | 메타 광고 ETL |
| `stock_report` | 평일 `17:00`, 트리거 가능 | CJ/eFLEXs, 이카운트, 쿠팡 재고를 병합해 소비기한 Slack 알림 전송 |

스크래핑 대상 웹사이트의 구조적 변경으로 비활성화된 DAG은 `_deprecated/` 경로로 옮긴다.

## 플러그인

`plugins/` 경로는 Airflow에서 제공하는 플러그인 시스템이며, DAG에서 공용으로 사용하는 기능을 제공한다.

### `airflow_utils.py`

대부분의 DAG에서 공통으로 사용하는 유틸리티 함수를 제공한다.

- `read_config`: Airflow Variable로 등록한 설정 파일 경로의 내용 읽기
- `read_credentials`: 인증 정보를 읽으면서 쿠키 등 파일 참조를 실제 내용으로 치환
- `in_timezone`: `pendulum` 날짜에 시간대 설정 및 timedelta 연산
- `format_date`: `in_timezone` 실행 결과를 문자열로 변환
- `get_execution_date`: 템플릿 변수 `data_interval_end`에 `format_date` 함수 적용

### `airflow_api.py`

통합 오케스트레이터 DAG에서 Airflow REST API를 호출하여 다른 DAG을 동적으로 실행할 때 활용된다.

- `authenticate`: Airflow JWT 액세스 토큰을 발급
- `request`: Airflow REST API에 대한 HTTP 요청
- `trigger_dagrun`: Dag ID에 대한 DAG 실행을 트리거
- `wait_for_completion`: DAG Run ID에 대한 DAG 실행 대기

### `airflow_patches.py`

Airflow Variable `test_mode`가 `true`인 조건에서 다음 동작을 런타임 패치한다.

- BigQuery Load Job 메서드의 동작을 차단하고 DuckDB 테이블 미리보기를 반환
- PostgreSQL 적재 메서드의 동작을 차단하고 DuckDB 테이블 미리보기를 반환
- Slack 파일 전송을 차단하고 가상의 응답 결과를 반환

즉, BigQuery/PostgreSQL 적재와 Slack 메시지 전송을 차단하여 DAG 로직을 확인할 때 쓰는 안전장치다.

### `dual_load.py`

DuckDB 테이블을 PostgreSQL과 BigQuery에 함께 적재하는 표준 helper를 제공한다.

| 함수 | PostgreSQL 동작 | BigQuery 동작 |
| --- | --- | --- |
| `load_table_from_duckdb` | `INSERT ...` | Load Job (`WRITE_APPEND`) |
| `overwrite_table_from_duckdb` | `DELETE ... WHERE ...; INSERT ...` | `DELETE ... WHERE ...; INSERT ...` |
| `upsert_table_from_duckdb` | `INSERT ... ON CONFLICT ...` | `MERGE ... WHEN MATCHED THEN ...` |

공통 반환값은 적재한 소스 행 수와 백엔드별 성공 여부를 포함한다.

```python
{
    "src_count": 100,
    "pg_success": True,
    "bq_success": True,
}
```

PostgreSQL 대상은 Airflow Connection `postgres`,
BigQuery 대상은 Airflow Connection `bigquery`에서 읽는다.

### `pw_actions.py`

Playwright WebSocket 서버를 사용해 브라우저 자동화를 수행한다.

- `login_coupang`: 쿠팡 Wing 로그인 후 '광고센터' 탭까지 이동해 쿠키 수집
- `login_naver`: 저장된 세션 또는 재로그인으로 네이버 쿠키 수집
- `get_browser_cookies`: 세션 쿠키가 생길 때까지 대기 후 쿠키를 문자열로 반환

단, `playwright` 서비스가 비활성화되었을 경우 해당 함수를 사용할 수 없다.
