# dbt 부분 업데이트 최적화 Backlog

> 전제: `dbt_postgres` 전체 모델 run이 먼저 통과해야 한다.

## 1. Master Dimension 영향 범위 분리

현재는 `sabangnet.model` 같은 master가 분석 테이블 중간 계산과 최종 표시 컬럼 양쪽에 섞여 있다. 정확성 검증 후 아래처럼 분리한다.

- 계산용 dimension: `delivery_group`, `delivery_fee`, `org_price`, category mapping처럼 fact 계산에 영향을 주는 컬럼.
- 표시용 dimension: `product_name`, `brand_name`, category display name처럼 조회 시점 join으로 해결 가능한 컬럼.

표시용 dimension은 분석 fact table에 물리 저장하지 않고 조회 layer에서 join하면 master overwrite 때 과거 fact partition을 다시 만들 필요가 줄어든다.

## 2. 수동 Google Sheets Overwrite 대응

수동 overwrite source는 예측 가능한 schedule이 없으므로 변경 발생 시점에 영향 범위를 기록해야 한다.

- `src/linkmerce/extensions/gsheets.py`의 `dual_load`에서 적재 로그를 남긴다.
- 로그에는 source schema/table, 적재 방식, 실행 시각, row count, 가능하면 변경 key/date min/max를 기록한다.
- master table처럼 날짜 범위가 직접 없으면 PostgreSQL에서 영향 받는 fact 날짜 범위를 먼저 계산한다.
- BigQuery는 PostgreSQL에서 산출한 affected date range만 vars로 받아 제한 실행한다.

## 3. Incremental 전환 후보

초기 구현은 정확성 검증을 위해 `table` materialization이다. 전체 검증 후 다음 순서로 incremental 전환한다.

- 일별 fact: `delete+insert` 또는 partition overwrite 방식으로 `ymd`/`order_dt`/`payment_dt` 범위만 갱신한다.
- master 영향 fact: affected key에서 affected date range를 산출한 뒤 해당 범위만 갱신한다.
- parameterized report: 운영에서 필요한 날짜 범위만 별도 실행하고 기본 daily selector에서는 제외한다.

## 4. PostgreSQL 18 변경 추적 검토

PostgreSQL 18의 변경 추적 기능은 source별 affected range 산출을 단순화할 수 있는 후보로 검토한다. 다만 분석 모델 갱신에는 source row 변경 자체보다 "그 변경이 어떤 fact date에 영향을 주는지"가 중요하므로, 변경 row capture와 영향 범위 계산 테이블을 함께 설계해야 한다.

검토 순서:

- source load 작업에서 변경 row key/date를 기록하는 방식과 비교한다.
- 수동 overwrite table은 전체 scan diff와 변경 추적의 비용을 비교한다.
- master table은 key 변경 목록에서 fact date range를 역추적하는 SQL을 먼저 만든다.

## 5. ClickHouse 적재 연동

PostgreSQL 분석 table이 안정화된 뒤 ClickHouse에는 전체 복제가 아니라 affected partition만 적재한다.

- PostgreSQL dbt run 결과에서 변경된 partition/date range를 기록한다.
- ClickHouse staging은 영구 중복 저장소가 아니라 교체 대상 partition을 만들기 위한 일시 테이블로 둔다.
- large IO가 우려되는 모델은 column subset export, partition 단위 파일 export, native ClickHouse insert 방식을 비교한다.
