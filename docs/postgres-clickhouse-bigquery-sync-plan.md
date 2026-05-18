# PostgreSQL → ClickHouse + BigQuery 분석 동기화 실행 계획

> 대상: 원천 데이터는 PostgreSQL에 적재하고, 대시보드 조회용 분석 테이블은 ClickHouse와 BigQuery에 동일하게 반영한다.

---

## 1. 한 줄 결론

현재 주문 분석 테이블은 약 30만 행 규모이고, 과거 데이터 수정/삭제/매핑 변경이 자주 전파될 수 있다.
이 조건에서는 **최종 분석 테이블까지 CDC로 세밀하게 반영하는 것보다, PostgreSQL에서 canonical 분석 결과를 다시 만든 뒤 ClickHouse와 BigQuery를 통째로 교체하는 방식이 더 단순하고 안전하다.**

즉, 지금 단계의 최선은 아래 구조다.

- PostgreSQL: 원천 저장소 + 분석 로직의 기준 엔진
- dbt-postgres: 분석 SQL과 lineage 관리
- Airflow: 5분 주기 실행, export, ClickHouse/BigQuery 반영, 검증
- ClickHouse / BigQuery: 대시보드용 서빙 저장소

여기서 중요한 보정이 하나 있다.

- **ClickHouse는 table swap 비용이 거의 운영 비용 문제로만 남는다.**
- **BigQuery는 SQL로 다시 계산하면 돈이 들지만, batch load job과 table copy job을 쓰면 query 비용 없이 교체할 수 있다.**

따라서 BigQuery 비용을 아끼려면 핵심은 "부분 업데이트" 자체보다,
**BigQuery 안에서 MERGE/CREATE OR REPLACE ... AS SELECT를 자주 돌리지 않는 것**이다.

---

## 2. 왜 CDC가 아니라 전체 재생성이 더 나은가

CDC는 보통 "원본 행이 바뀐 만큼만 아래로 전파"하는 방식이다.

그런데 주문 분석 테이블은 단순한 append 테이블이 아니다.
다음과 같은 변경이 과거 행까지 다시 계산되게 만든다.

1. `sabangnet.order`의 과거 주문 한 건 값 수정
2. `sabangnet.order`의 과거 주문 한 건 삭제
3. `relation.smt_opt_to_sbn_ids` 또는 그 추정 로직 변경으로 과거 스마트스토어 주문의 상품 매칭 변경
4. `sabangnet.model.product_name` 변경으로 과거 주문의 상품명 변경

즉, **한 행이 바뀌어도 최종 분석 테이블에서는 여러 과거 행이 다시 계산될 수 있다.**

특히 첨부한 [sabangnet_total_order.sql](postgres/var/exported_queries/routines/sabangnet_total_order.sql)을 보면 최종 결과가 다음 계층에 의존한다.

- `sabangnet.order`
- `sabangnet.order_invoice`
- `sabangnet.order_status`
- `smartstore.order_detail`
- `smartstore.order_delivery`
- `smartstore.order_status`
- `relation.smt_opt_to_sbn_ids`
- `relation.smt_opt_to_sbn_ids_est`의 입력 테이블들
- `sabangnet.model`
- `ecount.product`를 참조하는 `sabangnet.main_product()`

즉, 이 테이블은 사실상 **row-level CDC보다 재계산형 materialization**에 가깝다.

다만 여기서 한 가지 더 분리해서 봐야 한다.

- `delivery_group`, `delivery_fee`처럼 계산이나 파생값에 직접 쓰이는 속성
- `category_name1`, `product_name`처럼 마지막 표시용으로만 붙는 속성

실제 현재 루틴 기준으로는 `sabangnet.model` 전체가 단순 표시용 테이블은 아니다.

- `delivery_group`은 최종 출력과 수량 해석에 직접 쓰이고
- `delivery_fee`는 `COALESCE(ord.delivery_fee, mdl.delivery_fee, 0)` 형태로 계산에 들어간다.

따라서 **`sabangnet.model`의 모든 컬럼을 fact에 같이 싣는 것**과
**설명용 텍스트 컬럼만 별도 dimension으로 late join 하는 것**은 구분해야 한다.

그리고 지금 중요한 사실은 다음 두 가지다.

- 10억 행 순위 테이블은 예외이고,
- 진짜 핵심인 주문 분석 테이블은 30만 행 규모다.

30만 행이면 ClickHouse나 BigQuery는 물론, PostgreSQL에서도 5분마다 다시 계산하고 다시 밀어 넣는 방식이 충분히 현실적이다.

따라서 현재 단계에서는 **최종 주문 mart는 CDC보다 전체 재생성 + atomic 교체가 더 좋은 선택**이다.

---

## 3. 추천 아키텍처

```text
크롤링/Airflow 적재
        ↓
PostgreSQL raw
        ↓
dbt-postgres
  - stg_*    : 정리
  - int_*    : 조인/매핑
  - mart_*   : 최종 분석 결과
        ↓
Airflow export DAG (5분 주기)
        ├─ PostgreSQL -> ClickHouse stage -> swap
        └─ PostgreSQL -> BigQuery stage -> replace
```

핵심 원칙은 단순하다.

- **비즈니스 계산은 한 곳에서만 한다.**
- **서빙 DB는 계산 결과를 받기만 한다.**

이렇게 해야 ClickHouse와 BigQuery의 결과가 계속 동일하게 유지된다.

---

## 4. dbt와 Airflow의 역할 분담

### 4.1 dbt가 맡을 일

dbt는 **리니지와 변환 로직의 기준 저장소**로 쓴다.

권장 방식:

- 원천 PostgreSQL을 dbt source로 정의
- `staging` 모델에서 컬럼명/타입/기초 정리
- `intermediate` 모델에서 상품 매핑, 번들 매핑, 상태 계산
- `mart` 모델에서 최종 주문 분석 테이블 생성

여기서 중요한 점은, **dbt를 ClickHouse와 BigQuery에 각각 따로 돌리지 않는 것**이다.

그렇게 하면 문제가 생긴다.

- 같은 비즈니스 로직을 ClickHouse SQL과 BigQuery SQL 두 벌로 유지해야 한다.
- 디버깅할 때 어느 쪽이 기준인지 헷갈린다.
- 숫자가 다르면 원인을 찾기 어렵다.

그래서 추천은:

- **dbt는 PostgreSQL 기준으로 한 번만 운영**
- ClickHouse와 BigQuery는 **동일한 export 결과를 받아서 저장만**

### 4.2 Airflow가 맡을 일

Airflow는 **언제 실행할지와 어디에 반영할지**를 담당한다.

Airflow DAG가 할 일:

1. dbt 모델 갱신
2. PostgreSQL 최종 mart 추출
3. ClickHouse stage 적재
4. ClickHouse target 교체
5. BigQuery stage 적재
6. BigQuery target 교체
7. row count / sum 검증
8. 성공/실패 기록

즉, **dbt는 계산**, **Airflow는 배포**다.

---

## 5. 주문 분석 테이블에 대한 최적 전략

## 5.1 현재 단계의 최선

주문 분석 테이블은 다음 방식이 가장 좋다.

### 전략: 5분 주기 전체 재생성 + stage 교체

구체적으로는:

1. PostgreSQL에서 최종 주문 mart를 다시 계산한다.
2. 결과를 stage 테이블에 적재한다.
3. 검증이 통과하면 ClickHouse와 BigQuery의 target 테이블을 stage 결과로 교체한다.

이 전략이 좋은 이유는 다음과 같다.

- 과거 수정/삭제를 별도 예외 처리하지 않아도 된다.
- 상품 매핑 변경, 상품명 변경도 자동 반영된다.
- 삭제도 자연스럽게 처리된다.
- ClickHouse와 BigQuery의 결과를 항상 동일하게 맞출 수 있다.
- 30만 행 수준이면 운영 난이도 대비 가장 효율적이다.

그리고 BigQuery 비용 관점에서도 이 전략은 생각보다 불리하지 않다.

- BigQuery **query**는 bytes scanned 기준으로 비용이 붙는다.
- 하지만 BigQuery **batch load**는 무료다.
- BigQuery **table copy**도 무료다.

즉, 주문 mart를 PostgreSQL에서 계산한 뒤 Parquet 파일로 만들어 BigQuery에 **load job**으로 적재하고,
필요하면 **copy job**으로 target을 교체하면,
BigQuery 쪽 업데이트 비용은 사실상 **query 비용이 아니라 storage 중심**이 된다.

### 전략 보정: fact core + product display dimension 분리

주문 mart를 설계할 때는 모든 컬럼을 한 테이블에 납작하게 넣는 것보다,
다음을 분리하는 편이 더 낫다.

- **fact core**: 집계, 필터, 계산에 필요한 키/수치/파생값
- **product display dimension**: `product_name`, `category_name1..4`, `brand_name`, `color` 같은 설명용 속성

권장 이유는 단순하다.

- fact row 폭이 줄어 ClickHouse와 BigQuery 저장량이 줄어든다.
- `product_name` 변경이 fact 전체 재생성 사유가 아니게 된다.
- 상품명/카테고리명은 현재값 표시가 더 중요할 가능성이 높아 late binding에 잘 맞는다.

즉, 현재 문서의 "전체 재생성" 대상은
**주문 fact core**로 보는 것이 더 정확하다.

반대로 아래 속성은 fact 또는 계산용 intermediate에 남겨야 한다.

- `org_price`
- `delivery_group`
- `delivery_fee`
- 기타 공급가/배송비/원가 계산에 직접 들어가는 속성

이들은 설명용 텍스트가 아니라 **측정값을 바꾸는 속성**이기 때문이다.

### ClickHouse에서 JOIN으로 빼도 되는가

이 경우에는 **된다**고 보는 쪽이 맞다.

단, 전제는 다음과 같다.

- JOIN 대상은 `product_id` 기준의 작은 1:1 dimension 이어야 한다.
- ClickHouse에서 여러 대형 테이블을 관계형으로 계속 JOIN하는 구조는 피한다.
- 필요하면 dimension을 dictionary 또는 작은 key-value 성격 테이블로 둬서 조회 시 lookup 하게 한다.

현재 주문 mart가 약 30만 행 수준이고,
JOIN 대상이 `sabangnet.model`에서 뽑은 소형 상품 dimension 하나라면,
이 정도 late join 은 ClickHouse에서 걱정할 수준의 병목이 될 가능성이 높지 않다.

즉, **ClickHouse가 느린 것은 "JOIN" 일반이 아니라, 큰 fact끼리 여러 번 붙이는 경우**다.
지금처럼 **작은 상품 dimension 하나만 late join 하는 구조**는 충분히 현실적이다.

### 지금은 하지 않는 것이 좋은 것

- Debezium/Kafka 기반 full CDC 인프라
- ClickHouse에서 PostgreSQL 원격 테이블을 직접 JOIN해서 최종 mart 생성
- BigQuery와 ClickHouse에 같은 변환 로직을 따로 구현

이 셋은 지금 단계에서는 복잡도만 크게 늘릴 가능성이 높다.

---

## 5.2 왜 전체 재생성이 실제로 괜찮은가

사용자가 준 조건을 그대로 반영하면 다음과 같다.

- 원천 PostgreSQL은 나 혼자만 본다.
- Airflow 배치 적재 외에 큰 병목이 없다.
- 대시보드는 5분 단위 자동 업데이트면 충분하다.
- 몇 초 딜레이는 허용된다.
- 핵심 주문 mart는 30만 행 수준이다.

이 조건이면 "변경분만 아주 똑똑하게 계산"하는 데 시간을 쓰는 것보다,
**"그냥 전체를 다시 만들고 교체"**하는 것이 더 싸고, 더 안전하고, 더 설명하기 쉽다.

BigQuery 비용도 이 관점에서 다시 보면 다음처럼 정리된다.

### 비싼 방식

- BigQuery 안에서 `MERGE`
- BigQuery 안에서 `CREATE OR REPLACE TABLE target AS SELECT ...`
- BigQuery 안에서 source/target을 다시 JOIN해 부분 반영

이 방식은 query bytes processed 기준으로 과금된다.

### 싼 방식

- PostgreSQL에서 이미 계산된 결과를 Parquet로 export
- BigQuery에 batch load job 수행
- 필요 시 table copy job으로 target 교체

이 방식은 BigQuery 공식 가격 기준으로 batch load와 copy 자체에는 query 비용이 들지 않는다.

따라서 **지금 문제는 "30만 건 전체 교체가 비싸냐"가 아니라, "BigQuery에서 다시 계산하느냐"가 더 중요하다.**

내 판단은 다음과 같다.

- PostgreSQL에서 계산 완료된 30만 건 결과를 BigQuery에 통째로 다시 넣는 것은 **충분히 저렴하다.**
- 오히려 BigQuery 안에서 변경분 `MERGE`를 자주 돌리는 편이 더 비쌀 가능성이 높다.

---

## 6. `sabangnet_total_order`에 대한 구체 대응

### 6.1 이 테이블을 어떻게 봐야 하나

`sabangnet_total_order`는 단순 원천 복제가 아니라 **최종 비즈니스 mart**다.

따라서 이 테이블에는 다음 규칙을 적용한다.

- append incremental 대상이 아니다.
- update/delete fan-out 대상이다.
- 기준은 row-level CDC가 아니라 full rebuild다.

### 6.2 사용자가 준 4가지 변경 사례에 대한 처리

#### 사례 1. 과거 주문 한 건의 `order_amount` 수정

처리:

- PostgreSQL 최종 mart 전체 재계산
- ClickHouse / BigQuery target 교체

설명:

- 한 건만 바뀌어도 합계, 공급가, 비용 등 파생 컬럼이 바뀔 수 있다.
- 전체 재생성에서는 별도 예외 처리 없이 정확히 반영된다.

#### 사례 2. 과거 주문 한 건 삭제

처리:

- PostgreSQL 최종 mart 전체 재계산
- ClickHouse / BigQuery target 교체

설명:

- CDC로는 tombstone 처리, downstream delete 처리까지 신경 써야 한다.
- 전체 교체에서는 삭제된 행이 새 결과에 없으므로 자동으로 반영된다.

#### 사례 3. `relation.smt_opt_to_sbn_ids` 변경으로 과거 주문 매칭 변경

처리:

- PostgreSQL 최종 mart 전체 재계산
- ClickHouse / BigQuery target 교체

설명:

- 이 변경은 특정 `option_id`만 바꾸는 것처럼 보여도, 실제로는 그 option을 가진 과거 주문 전체에 영향을 준다.
- row-level upsert보다 전체 재계산이 훨씬 단순하다.

#### 사례 4. `sabangnet.model.product_name` 변경으로 과거 상품명 변경

처리:

- 권장 구조에서는 product dimension 만 갱신
- ClickHouse / BigQuery fact core 는 그대로 유지

설명:

- `product_name`, `category_name1` 같은 컬럼이 설명용 late join 대상이라면,
  이 변경은 fact 재계산 사유가 아니다.
- dimension refresh 만으로 과거 주문 화면 표시를 갱신할 수 있다.
- 다만 `delivery_group`, `delivery_fee`처럼 계산에 들어가는 속성 변경은 여전히 fact 재계산 사유다.

---

## 7. 구현 방식 상세

## 7.1 PostgreSQL 쪽 구현

### 권장 스키마

```text
raw_pg        : 원천 적재 테이블
analytics_pg  : dbt가 만드는 분석용 스키마
ops_pg        : Airflow 실행 로그, row count, checksum 기록
```

### dbt 모델 구조

```text
models/
  sources/
    raw_sources.yml
  staging/
    stg_sabangnet_order.sql
    stg_smartstore_order_detail.sql
    stg_relation_smt_opt_to_sbn_ids.sql
    ...
  intermediate/
    int_sabangnet_product_order.sql
    int_smartstore_order_complete.sql
    int_rocket_order_complete.sql
    int_main_product.sql
    ...
  marts/
    mart_total_order.sql
```

### 중요한 구현 포인트

현재 routines는 BigQuery 함수 스타일이다.

- `DS_START_DATETIME`, `DS_END_DATETIME` 같은 함수 파라미터 기반
- `SAFE_CAST`, `QUALIFY`, `UNNEST`, backtick 등 BigQuery 문법 사용

주문 mart 전략을 전체 재생성으로 잡으면,
이 함수형 쿼리를 **전체 이력 기준 dbt 모델**로 바꾸기 쉬워진다.

즉, 주문 mart에서는 날짜 파라미터를 억지로 유지할 필요가 없다.

권장 방향:

- 주문 mart 계열은 전체 이력 기준 dbt 모델로 포팅
- 필요 시 최종 export 시점에만 필터를 건다
- 하지만 현재는 30만 행이므로 export도 전체로 진행

### 권장 모델 분리

주문 mart는 아래처럼 나누는 것이 좋다.

- `mart_total_order_core`: 주문 fact core
- `dim_product_display_current`: 상품 표시용 dimension
- `v_mart_total_order_wide`: 필요 시 둘을 다시 붙인 조회용 view

`mart_total_order_core`에는 다음만 남긴다.

- 주문 식별 키
- `product_id`, `option_id` 같은 조인 키
- 수량/매출/원가/공급가/배송비 같은 수치 컬럼
- `delivery_group`처럼 계산에 필요한 속성

`dim_product_display_current`에는 다음을 둔다.

- `product_id`
- `product_name`
- `category_name1..4`
- `brand_name`
- `maker_name`
- `color`
- 기타 화면 표시용 텍스트

이렇게 하면 상품명 변경은 dimension 갱신으로 끝나고,
주문 fact 전체 재생성은 진짜 계산 로직이 바뀔 때만 수행하면 된다.

### 최종 mart에 꼭 넣을 컬럼

- `order_line_id`: 안정적인 surrogate key
- `source_system`: sabangnet / smartstore / coupang 구분
- `order_dt`
- `order_date`
- `order_month`
- `rebuilt_at`
- `batch_id`

`order_line_id`는 아래처럼 안정적으로 만든다.

- source_system
- account_no
- order_id
- product_order_id
- option_id
- product_id_shop
- option_id_shop

이 조합을 문자열로 이어 붙여 hash 키를 만들면 된다.

이 키는 BigQuery와 ClickHouse 양쪽 모두에서 동일한 행 식별자로 쓸 수 있다.

---

## 7.2 ClickHouse 쪽 구현

### 권장 DB 구조

```text
raw_ch      : 필요 시 원천 mirror
mart_ch     : 대시보드 조회 테이블
dim_ch      : 상품 표시용 dimension / dictionary source
stage_ch    : 교체 직전 임시 적재 테이블
ops_ch      : 검증 로그
```

### 권장 테이블 엔진

주문 mart는 `ReplacingMergeTree`보다 **일반 `MergeTree` 기반 stage-swap**가 더 단순하다.

이유:

- 우리는 row-by-row dedup보다 전체 교체를 할 것이기 때문이다.
- 최종 상태를 정확히 맞추는 데는 table swap이 더 직관적이다.

### 권장 업데이트 방식

1. `stage_ch.mart_total_order_next` 생성
2. PostgreSQL export 결과를 여기에 적재
3. row count / 핵심 합계 검증
4. 검증 통과 시 `mart_ch.mart_total_order`와 swap

핵심은 **운영 중인 target 테이블에 직접 delete/insert 하지 않는 것**이다.

그래야 대시보드 사용자가 중간 상태를 보지 않는다.

### 상품명/카테고리 late join 권장 방식

ClickHouse에서는 다음 구조를 권장한다.

1. `mart_ch.mart_total_order_core`는 얇은 fact 로 유지
2. `dim_ch.product_display_current`를 `product_id` 기준 소형 dimension 으로 유지
3. 대시보드에서는 둘을 조회 시 붙이거나, 필요하면 `view`로 감싼다

지연이 민감하면 다음 중 하나를 선택한다.

- 작은 dimension table 을 우측에 두는 단일 JOIN
- dictionary 기반 lookup

즉, ClickHouse에서도 **설명용 텍스트 컬럼 하나 때문에 fact 를 계속 넓게 유지할 필요는 없다.**
다만 여러 대형 dimension 을 연쇄 JOIN 하는 방향으로는 가지 않는 것이 좋다.

---

## 7.3 BigQuery 쪽 구현

### 권장 DB 구조

```text
raw_bq      : 필요 시 원천 mirror
mart_bq     : 대시보드 조회 테이블
dim_bq      : 상품 표시용 dimension
stage_bq    : 교체 직전 임시 적재 테이블
ops_bq      : 검증 로그
```

### 권장 업데이트 방식

1. `stage_bq.mart_total_order_next`에 PostgreSQL export 결과 적재
2. row count / 핵심 합계 검증
3. 검증 통과 시 target 교체

BigQuery에서는 다음 두 방식 중 하나를 쓰면 된다.

- `WRITE_TRUNCATE`로 target 직접 교체
- stage 적재 후 **table copy job**으로 target overwrite

권장은 두 번째다.

이유:

- stage 데이터 검증 후 교체할 수 있다.
- ClickHouse와 절차가 비슷해져 운영이 단순하다.
- SQL query를 실행하지 않아 query bytes 비용을 피할 수 있다.

### BigQuery에서 피해야 할 것

가급적 아래 방식은 피한다.

- `MERGE INTO mart_bq.mart_total_order ...`
- `CREATE OR REPLACE TABLE mart_bq.mart_total_order AS SELECT * FROM stage_bq...`

이 두 방식은 query job이라서, 스캔량이 커질수록 돈이 붙는다.

반면 아래 방식은 현재 요구에 더 맞다.

1. Airflow에서 PostgreSQL 결과를 Parquet로 만든다.
2. BigQuery stage 테이블에 batch load 한다.
3. 검증한다.
4. BigQuery copy job으로 target을 덮어쓴다.

이렇게 하면 BigQuery는 "계산 엔진"이 아니라 "저장/조회 엔진"으로만 쓰게 된다.

### BigQuery에서의 late join

BigQuery는 작은 상품 dimension 을 fact 와 JOIN 해서 조회하는 구조가 자연스럽다.

권장 방식:

- `mart_bq.mart_total_order_core`
- `dim_bq.product_display_current`
- 필요 시 `view` 로 wide table 노출

이 구조에서는 `product_name` 변경 시 fact 재적재 없이 dimension 만 다시 load 하면 된다.

---

## 8. PostgreSQL → ClickHouse + BigQuery 동시 반영 방법

## 8.1 가장 쉬운 방식

Airflow가 PostgreSQL canonical 결과를 읽고,
그 결과를 ClickHouse와 BigQuery에 동시에 반영한다.

권장 포맷:

- Parquet

이유:

- 현재 프로젝트는 DuckDB를 이미 활용 중이다.
- PostgreSQL 결과를 DuckDB temp table에 담고 Parquet로 내보내기 쉽다.
- BigQuery는 Parquet 적재가 강하다.
- ClickHouse도 Python client로 Arrow/Parquet 계열 적재가 쉽다.

### 권장 흐름

1. Airflow가 PostgreSQL에서 `analytics_pg.mart_total_order_core` 조회
2. Airflow가 PostgreSQL에서 `analytics_pg.dim_product_display_current` 조회
3. DuckDB temp table에 적재
4. fact core / product dimension Parquet 생성
5. 두 payload를 ClickHouse와 BigQuery에 각각 적재
6. ClickHouse는 fact swap + dimension refresh 수행
7. BigQuery는 fact copy replace + dimension load 수행

즉, **계산은 한 번, 반영은 두 번**이다.

설명용 상품 속성을 분리하면 export 흐름은 아래처럼 약간 바뀐다.

1. 주문 fact core payload export
2. 상품 display dimension payload export
3. 둘을 ClickHouse / BigQuery에 각각 반영

이때도 핵심 계산은 PostgreSQL에서 한 번만 관리한다는 원칙은 그대로다.

### 왜 BigQuery도 전체 교체가 괜찮다고 보는가

30만 건이면 보통 논리적으로 큰 테이블이 아니다.

대략적인 감각만 잡으면:

- 행당 1KB라고 가정하면 30만 건은 약 300MB 수준이다.
- 행당 10KB라고 가정해도 약 3GB 수준이다.

BigQuery active storage는 TiB-month 단가 기준이라,
이 정도는 월 storage 비용이 대체로 아주 작다.

거기에 batch load와 copy를 쓰면,
"5분마다 갈아끼우는 행위 자체" 때문에 query 비용이 커지지 않는다.

즉, 현재 규모에서는 **BigQuery도 전체 교체가 비용상 충분히 허용 가능**하다고 본다.

---

## 9. Airflow DAG 구성안

### DAG 이름 예시

- `sync_order_mart_pg_to_ch_bq`

### Task 구성

1. `dbt_build_order_marts_pg`
   - dbt-postgres 실행
   - 주문 fact core / 상품 display dimension 갱신

2. `export_order_fact_core_from_pg`
   - PostgreSQL에서 주문 fact core 조회
   - DuckDB temp 적재
   - Parquet 생성

3. `export_product_dimension_from_pg`
   - PostgreSQL에서 상품 display dimension 조회
   - DuckDB temp 적재
   - Parquet 생성

4. `load_stage_clickhouse_fact`
   - fact Parquet를 ClickHouse stage 테이블에 적재

5. `refresh_clickhouse_dimension`
   - 상품 dimension 을 ClickHouse dimension table 또는 dictionary source 에 반영

6. `validate_clickhouse_stage`
   - row count
   - `payment_amount`, `supply_amount`, `supply_cost` 합계 검증

7. `swap_clickhouse_target`
   - stage와 target 교체

8. `load_stage_bigquery_fact`
   - fact Parquet를 BigQuery stage 테이블에 적재

9. `refresh_bigquery_dimension`
   - 상품 dimension 을 BigQuery table 에 적재

10. `validate_bigquery_stage`
   - row count
   - 핵심 합계 검증

11. `replace_bigquery_target`
   - stage -> target 교체

12. `write_sync_audit_log`
   - batch_id
   - 시작/종료 시각
   - 소스 row count
   - ClickHouse row count
   - BigQuery row count
   - 합계 checksum

### 실행 주기

- 주문 mart: 5분 주기
- 대용량 append fact: 별도 DAG, 더 단순한 watermark incremental

---

## 10. 추천 운영 규칙

### 규칙 1. 주문 mart는 무조건 전체 교체

현재는 이 규칙이 가장 중요하다.

예외를 만들지 말고, 아래처럼 단순하게 간다.

- 주문 관련 mart
- 상품 매핑을 많이 타는 mart
- 과거 수정/삭제가 자주 전파되는 mart

이 셋은 모두 전체 재생성.

단, BigQuery에서는 "전체 재생성"의 구현을 SQL 재계산이 아니라
**free batch load + free copy** 중심으로 구현한다.

### 규칙 1-보정. 설명용 텍스트는 fact에서 분리

다음처럼 계산에 직접 쓰이지 않는 속성은 가능하면 fact에서 뺀다.

- `product_name`
- `category_name1..4`
- `brand_name`
- `color`
- 기타 화면 표시용 속성

이들은 `product_id` 기준 dimension 으로 두고,
BigQuery는 일반 JOIN 또는 view,
ClickHouse는 작은 dimension join 또는 dictionary lookup 으로 붙인다.

반면 아래 속성은 fact 또는 계산용 intermediate 에 남긴다.

- `org_price`
- `delivery_group`
- `delivery_fee`
- 기타 측정값을 바꾸는 속성

### 규칙 2. 대형 append fact는 별도 incremental

예외인 대형 순위 테이블이나 광고 fact는 주문 mart와 다르게 처리한다.

- watermark 기반 incremental
- 필요 시 row-level CDC 또는 append + correction window

즉, **주문 mart와 10억 행 순위 테이블은 같은 전략으로 운영하지 않는다.**

그리고 BigQuery 비용 절약은 이쪽에서 더 중요하다.

- 대형 append fact는 partitioned table로 설계
- 최근 파티션만 batch load 또는 MERGE
- 전체 교체는 주문 mart처럼 작은 재계산형 테이블에만 허용

즉,

- 주문 mart: 전체 교체
- 대형 fact: 부분 갱신

### 규칙 3. ClickHouse는 serving, PostgreSQL은 truth

문제가 생기면 항상 PostgreSQL 결과를 기준으로 본다.

ClickHouse와 BigQuery는 결과 복제본이다.

---

## 11. 나중에 규모가 커지면 어떻게 바꿀까

현재는 전체 교체가 최선이지만, 나중에 주문 mart가 몇 백만~몇 천만 행으로 커지면 다음 단계로 진화하면 된다.

### 2단계 전략

- 전체 교체 → 영향 월 파티션 재생성

예:

- 현재 월은 5분마다 재생성
- 과거 월 변경은 change log 기반으로 영향 월만 재생성

BigQuery에서는 이때 다음 방식으로 바꾼다.

- target을 `order_date` 또는 `order_month` 파티션 테이블로 만든다.
- 영향을 받은 파티션만 다시 export 한다.
- 해당 파티션만 `WRITE_TRUNCATE` load 한다.

이 방식은 query-based MERGE보다 훨씬 단순하고,
전체 교체보다 네트워크/적재 시간도 줄일 수 있다.

### 3단계 전략

- raw layer는 CDC
- mart layer는 영향 파티션 재생성

중요한 점은,
**커져도 최종 mart를 row-level CDC로 직접 관리하지는 않는 편이 좋다**는 것이다.

CDC는 raw 복제에는 유용하지만,
최종 비즈니스 mart는 여전히 재계산형이 더 안전하다.

---

## 12. 단계별 실행 순서

### 1단계. 모델 기준 확정

- routines의 BigQuery 함수형 SQL을 PostgreSQL용 dbt 모델로 변환
- 우선순위는 `sabangnet_total_order` 체인부터 시작

### 2단계. PostgreSQL canonical mart 완성

- `analytics_pg.mart_total_order_core` 생성
- `analytics_pg.dim_product_display_current` 생성
- row count와 핵심 합계 검증

### 3단계. ClickHouse 서빙 테이블 생성

- `stage_ch.mart_total_order_core_next`
- `mart_ch.mart_total_order_core`
- `dim_ch.product_display_current`

### 4단계. BigQuery 서빙 테이블 생성

- `stage_bq.mart_total_order_core_next`
- `mart_bq.mart_total_order_core`
- `dim_bq.product_display_current`

### 5단계. Airflow export DAG 구현

- dbt build
- export fact parquet
- export product dimension parquet
- load CH/BQ fact stage
- refresh CH/BQ product dimension
- validate
- swap/replace

### 6단계. 대시보드 전환

- 로컬 대시보드: ClickHouse
- 원격 대시보드: BigQuery

### 7단계. 운영 기준 수립

- 실패 시 target 미교체
- stage 보존 시간
- 최근 성공 batch_id 기록
- row count / checksum 비교 기준 수립

---

## 13. 최종 제안

현재 조건만 보면 다음이 가장 좋다.

1. 주문 분석 테이블은 CDC로 미세 업데이트하지 않는다.
2. PostgreSQL에서 dbt-postgres로 canonical 주문 **fact core** 와 상품 **display dimension** 을 만든다.
3. Airflow가 5분마다 fact core 를 다시 뽑고, 상품 dimension 은 별도 cadence 로 갱신한다.
4. 같은 결과를 ClickHouse와 BigQuery에 stage 적재한다.
5. ClickHouse는 fact core 를 swap 하고, 상품명/카테고리명은 작은 dimension join 또는 dictionary lookup 으로 붙인다.
6. BigQuery는 fact core 를 batch load + copy job으로 교체하고, 상품명/카테고리명은 dimension join 으로 붙인다.

쉽게 말하면,

- **계산은 PostgreSQL + dbt에서 한 번만**
- **배포는 Airflow가 ClickHouse와 BigQuery로**
- **주문 fact core 는 전체 재생성으로 안전하게**
- **설명용 텍스트는 별도 dimension 으로 late join**
- **BigQuery는 SQL 재계산이 아니라 load/copy로 싸게 운영**

이 방식이 지금 규모와 변경 패턴에 가장 잘 맞는다.