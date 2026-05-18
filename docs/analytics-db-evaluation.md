# 분석 DB 후보 검토: PostgreSQL vs TimescaleDB vs ClickHouse

> 대상: LinkMerce의 ETL 원천 적재는 PostgreSQL에 유지하고, 분석용 DB 레이어를 어디에 둘지 검토한다.

---

## 1. 결론 요약

현재 LinkMerce의 데이터 성격을 기준으로 보면, 선택 기준은 다음처럼 정리된다.

- 원천 적재 DB는 계속 PostgreSQL로 유지하는 것이 맞다.
- 분석 DB에서 대규모 시간 범위 스캔, 집계, 재집계, 대시보드 동시 조회가 핵심이면 ClickHouse는 유의미하다.
- 분석 DB에서 정규화된 테이블 간 JOIN이 많고, PostgreSQL과 거의 같은 SQL/운영 모델을 유지하고 싶으면 TimescaleDB가 더 안전하다.
- 따라서 지금 시점의 가장 현실적인 권고안은 다음 2단계 접근이다.
  1. 단기: PostgreSQL 기반 분석 레이어를 유지하거나 TimescaleDB를 우선 검토한다.
  2. 중기: 1억~10억 행급 fact 테이블 중심 대시보드가 병목이면 ClickHouse를 별도 분석 DB로 도입한다.

즉, ClickHouse는 "대체재"라기보다 "대규모 fact 집계용 분석 엔진"으로 보는 편이 맞다.

---

## 2. 왜 TimescaleDB를 같이 봐야 하는가

TimescaleDB는 PostgreSQL 확장이다. 따라서 다음 이점이 있다.

- PostgreSQL SQL 호환성이 높다.
- 일반 테이블 JOIN, 서브쿼리, UPDATE, UPSERT, 트랜잭션 처리 감각이 그대로 유지된다.
- hypertable이 시간 기준으로 자동 청크 분할을 수행하고, 청크 스킵으로 범위 조회를 줄인다.
- hypercore는 신규 데이터는 rowstore에 쓰고, 오래된 데이터는 columnstore로 자동 전환한다.
- 공식 문서 기준으로 columnstore 압축률은 90~98% 수준까지 기대할 수 있다.
- continuous aggregate를 통해 증분 갱신되는 요약 테이블을 만들 수 있다.

특히 이번 요구처럼 "원천은 PostgreSQL", "분석 DB 안에서 JOIN이 또 발생", "아직 ClickHouse 도입을 결정하지 않음"이라는 조건이면, TimescaleDB는 운영 복잡도를 크게 늘리지 않으면서 분석 성능을 끌어올릴 수 있는 중간 해법이다.

---

## 3. 후보별 판단

### 3.1 PostgreSQL + dbt-postgres

적합한 경우:

- 분석 데이터 규모가 아직 PostgreSQL 튜닝 범위 안에 있다.
- 대시보드 쿼리가 복잡한 JOIN 중심이고, 대규모 full scan 집계는 상대적으로 적다.
- 운영 단순성이 가장 중요하다.

장점:

- 현재 구조와 가장 자연스럽게 이어진다.
- dbt-postgres만으로 바로 구축 가능하다.
- 원천 적재 DB와 분석 DB가 같은 엔진이므로 운영 지식이 분산되지 않는다.

한계:

- 이벤트성 fact 테이블이 커질수록 넓은 시간 범위 집계와 고동시성 대시보드 조회에서 한계가 빨리 온다.
- 요약 테이블을 잘 만들지 않으면 원본 fact scan 비용이 계속 증가한다.

### 3.2 TimescaleDB + dbt-postgres

적합한 경우:

- 시계열/이벤트 데이터가 많지만 PostgreSQL 계열의 JOIN 유연성을 유지해야 한다.
- 분석 DB 내 재JOIN이 자주 발생한다.
- ClickHouse 수준의 별도 엔진 운영 복잡도는 아직 부담이다.

장점:

- hypertable, chunk skipping, continuous aggregate, hypercore를 활용할 수 있다.
- rowstore와 columnstore를 함께 쓰므로 쓰기와 분석을 동시에 어느 정도 양립시킬 수 있다.
- TimescaleDB v2.16 기준 continuous aggregate에 JOIN을 지원한다.
- continuous aggregate 위에 또 다른 continuous aggregate를 쌓는 계층형 요약이 가능하다.

주의점:

- continuous aggregate의 JOIN은 제약이 있다.
- 공식 문서 기준으로 standard PostgreSQL 테이블의 변경은 continuous aggregate가 자동 추적하지 않는다.
- 대규모 scan/aggregation만 놓고 보면 ClickHouse가 더 공격적인 선택이다.

### 3.3 ClickHouse + dbt-clickhouse

적합한 경우:

- 대시보드가 대규모 fact 테이블 집계 중심이다.
- 시간 범위 스캔, 집계, top-N, funnel, cohort, 광고/로그 분석이 핵심이다.
- 분석용 fact/mart를 미리 적재해두고 조회하는 구조가 가능하다.

장점:

- 대용량 scan/aggregation 성능에 가장 유리하다.
- PostgreSQL 원천 데이터를 직접 읽어오는 PostgreSQL table engine / table function을 제공한다.
- dbt-clickhouse는 table, view, incremental, materialized view, snapshots, tests 등을 공식 지원한다.

주의점:

- dbt-clickhouse 공식 문서상 매우 큰 row-preserving 모델은 실행 시간이 길어질 수 있고, 요약 위주의 모델링을 권장한다.
- ClickHouse는 JOIN을 지원하지만, 반복적인 대형 정규화 JOIN 워크로드에 최적화된 엔진은 아니다.
- 대규모 fact-to-fact JOIN이나 자주 바뀌는 차원 테이블과의 복잡한 재JOIN은 설계가 나쁘면 메모리/네트워크 비용이 커진다.
- MaterializedPostgreSQL database engine은 공식 문서상 experimental이며 ClickHouse Cloud에서 미지원이다.

---

## 4. 이번 요구에 대한 핵심 판단

### 4.1 ClickHouse가 유의미한가?

유의미하다. 다만 전제는 있다.

유의미한 이유:

- 이 프로젝트는 이미 BigQuery식 파티션/클러스터링 사고방식과 대용량 시계열 테이블을 다루고 있다.
- 특히 rank, 광고 보고서, 주문/노출/클릭/전환과 같은 데이터는 ClickHouse의 강점과 맞닿아 있다.
- 원천 적재를 PostgreSQL에 두고, 분석용 fact/mart만 ClickHouse로 분리하면 역할 분담이 명확해진다.

유의미하지 않은 경우:

- 분석 DB 안에서 다시 발생하는 JOIN이 대부분 복잡한 정규화 조인이고,
- 비즈니스 로직이 자주 바뀌며,
- 소량 테이블들을 반복적으로 결합해 상세 레벨 리포트를 즉석 생성하는 패턴이 중심이라면,
- TimescaleDB 쪽이 전체 운영 효율은 더 나을 수 있다.

### 4.2 이번 케이스에서 더 현실적인 권고

현재 조건을 감안하면 다음처럼 보는 것이 가장 균형적이다.

- 1차 권고: "분석 DB 전체를 바로 ClickHouse로 갈아탄다" 보다는,
  대용량 fact mart만 ClickHouse로 보내는 하이브리드 구조가 적절하다.
- 2차 권고: ClickHouse 도입 전, TimescaleDB로도 목표 SLA가 나오는지 먼저 보는 것이 안전하다.
- 3차 권고: ClickHouse를 선택하더라도 PostgreSQL 원천을 실시간 원격 JOIN하는 구조를 주력으로 삼지 말고,
  ClickHouse 안에 raw/staging를 먼저 적재한 뒤 그 안에서 mart를 만드는 구조가 바람직하다.

---

## 5. dbt로 PostgreSQL 데이터를 JOIN해서 ClickHouse에 적재할 수 있는가

### 5.1 원칙

가능하다. 다만 "dbt가 크로스 DB를 직접 오케스트레이션한다"기보다,
"ClickHouse가 PostgreSQL 데이터를 읽을 수 있게 만든 뒤, dbt가 ClickHouse 안에서 모델을 실행한다"에 가깝다.

dbt 공식 문서 기준:

- dbt는 profiles.yml의 target을 기준으로 한 번의 run에서 하나의 타깃 플랫폼에 연결한다.
- source는 타깃 플랫폼에서 접근 가능한 테이블이어야 한다.

즉, target이 ClickHouse라면 모델 SQL은 ClickHouse에서 실행된다.

### 5.2 가능한 방식

#### 방식 A. ClickHouse에서 PostgreSQL 원격 테이블을 직접 읽기

ClickHouse는 다음 기능을 제공한다.

- PostgreSQL table engine
- postgresql() table function

따라서 ClickHouse 안에서 아래와 같은 SQL이 가능하다.

```sql
SELECT ...
FROM postgresql('pg-host:5432', 'linkmerce', 'fact_table', 'user', 'password') AS pg_fact
JOIN local_dim AS dim ON ...
```

그리고 이 SELECT 결과를 dbt-clickhouse 모델로 materialize하면 ClickHouse 테이블에 적재할 수 있다.

#### 방식 B. ClickHouse에 PostgreSQL raw를 먼저 복제/배치 적재 후, 로컬 JOIN 수행

더 권장되는 방식이다.

- Airflow 또는 CDC 도구로 PostgreSQL raw를 ClickHouse raw/staging에 적재한다.
- dbt-clickhouse는 ClickHouse 내부 raw/staging를 source로 받아서 model을 만든다.
- JOIN도 ClickHouse 내부 로컬 테이블끼리 수행한다.

### 5.3 실무적으로 어떤 방식이 맞는가

방식 B가 맞다.

이유는 ClickHouse 공식 문서상 PostgreSQL engine/table function의 동작 특성 때문이다.

- 단순 WHERE 정도만 PostgreSQL 쪽으로 pushdown된다.
- JOIN, aggregation, sorting은 PostgreSQL 조회가 끝난 뒤 ClickHouse에서 수행된다.

즉, 대형 PostgreSQL 원격 테이블 두세 개를 ClickHouse에서 직접 JOIN하면,
결국 원격 데이터 전송량이 커지고 네트워크 병목이 생길 수 있다.

정리하면:

- PoC / 소형 차원 테이블 / 부트스트랩: 방식 A 가능
- 운영 / 대형 fact / 반복적 대시보드: 방식 B 권장

### 5.4 MaterializedPostgreSQL engine은 써도 되는가

신중해야 한다.

공식 문서 기준:

- experimental 기능이다.
- ClickHouse Cloud에서는 지원되지 않는다.
- DDL 동기화 지원에 제약이 있다.

따라서 LinkMerce에서는 이 기능을 기본 경로로 잡지 않는 편이 낫다.
실전 운영 경로는 아래 중 하나가 더 안정적이다.

- Airflow 배치 증분 적재
- PostgreSQL CDC 도구 사용
- ClickPipes / PeerDB 계열 동기화 도구 검토

---

## 6. 분석 DB 안에서 JOIN이 또 발생하는 경우

이 조건은 ClickHouse와 TimescaleDB를 가르는 핵심 기준이다.

### 6.1 TimescaleDB가 더 유리한 경우

- 대시보드가 여러 분석 테이블을 상세 레벨에서 다시 JOIN한다.
- 조인 키가 복잡하고, 조인 패턴이 자주 바뀐다.
- 차원 테이블 수정이 잦다.
- PostgreSQL 문법 그대로 유지하는 것이 중요하다.

### 6.2 ClickHouse가 여전히 유리한 경우

- 재JOIN이 있더라도 대부분 사실상 star schema다.
- 큰 fact와 작은 dimension을 조인한다.
- 혹은 이미 요약된 mart끼리 조인한다.
- 대시보드 응답시간이 가장 중요하고, 일부 정규화 유연성은 포기 가능하다.

### 6.3 ClickHouse에서 권장하는 모델링 방향

- raw를 그대로 여러 번 JOIN하지 않는다.
- stg -> int -> mart 흐름에서 미리 폭넓게 정리한다.
- 자주 쓰는 차원은 dictionary 또는 소형 dimension table로 유지한다.
- 대시보드가 직접 보는 것은 최종 mart 위주로 제한한다.

즉, ClickHouse를 쓴다면 "분석 DB 안에서 또 JOIN" 자체는 가능하지만,
그 JOIN이 "원시 fact를 매번 다시 섞는 패턴"이면 설계가 나쁜 것이다.

---

## 7. 최종 권고안

### 권고안 A. 가장 보수적이고 현실적인 경로

- 원천 적재: PostgreSQL 유지
- 분석 DB: TimescaleDB 우선 검토
- dbt: dbt-postgres 유지

추천 조건:

- 아직 ClickHouse까지 운영 범위를 넓히고 싶지 않다.
- JOIN 유연성이 중요하다.
- 대시보드 병목이 있으나, 아직 "ClickHouse가 아니면 안 되는 수준"인지 확신이 없다.

### 권고안 B. 대용량 fact 중심으로 분리하는 경로

- 원천 적재: PostgreSQL 유지
- 분석 DB: ClickHouse 별도 도입
- dbt: ClickHouse용 프로젝트 또는 ClickHouse target 분리
- 적재: PostgreSQL -> Airflow batch/CDC -> ClickHouse raw/staging -> dbt mart

추천 조건:

- 1억~10억 행급 fact 테이블이 대시보드 성능을 지배한다.
- 쿼리 대부분이 시간 범위 집계, 랭킹, 광고 성과 분석, 주문/유입 요약이다.
- 최종 모델을 재사용 가능한 mart 중심으로 설계할 수 있다.

### 이 프로젝트에 대한 내 판단

현재 LinkMerce의 데이터 성격을 보면 ClickHouse는 분명 유의미하다.
다만 "분석 DB 전체를 ClickHouse로 단일화"가 아니라,
"ClickHouse를 대용량 분석 mart 전용으로 추가"하는 방향이 맞다.

그리고 도입 순서는 다음이 가장 안전하다.

1. PostgreSQL raw 유지
2. TimescaleDB 또는 PostgreSQL 튜닝으로 1차 검증
3. 병목이 남는 대형 fact mart만 ClickHouse로 이관
4. ClickHouse 안에서 재JOIN이 필요한 모델은 mart/summary 중심으로 재설계

---

## 8. ClickHouse 서비스 폴더 구성 계획

ClickHouse 도입이 유의미하다고 판단되므로, PostgreSQL 서비스 폴더 패턴과 유사하게 별도 서비스 폴더를 아래처럼 설계한다.

### 8.1 목표

- 원천 적재 DB는 PostgreSQL에 둔다.
- ClickHouse는 분석 전용 서비스로 분리한다.
- Airflow가 PostgreSQL -> ClickHouse 적재를 담당한다.
- dbt-clickhouse는 ClickHouse 내부 raw/staging를 source로 사용한다.

### 8.2 제안 폴더 구조

```text
clickhouse/
  .env
  build.sh
  docker-compose.yaml
  Dockerfile
  README.md
  config/
    config.d/
      logging.xml
      network.xml
      merge_tree.xml
    users.d/
      linkmerce.xml
  initdb/
    00_databases.sql
    10_roles.sql
    20_pg_external_sources.sql
    30_raw_tables.sql
    40_materialized_views.sql
  scripts/
    healthcheck.sh
```

### 8.3 파일별 역할

- docker-compose.yaml
  - 단일 노드 ClickHouse 서버
  - 데이터/로그 볼륨
  - 8123 HTTP, 9000 native 포트 노출

- Dockerfile
  - 커스텀 설정 복사
  - 필요 시 CA, locale, helper script 포함

- build.sh
  - postgres/build.sh와 동일한 방식으로 custom image 빌드

- config/config.d
  - MergeTree, 메모리, 로그, 네트워크 설정

- config/users.d
  - admin / readonly / etl 사용자 분리

- initdb/00_databases.sql
  - raw, mart, ops 등 데이터베이스 생성

- initdb/20_pg_external_sources.sql
  - PostgreSQL engine 기반 외부 테이블 또는 named collection 정의
  - 단, 운영 본선에서는 소형 dimension/PoC 외에는 최소화

- initdb/30_raw_tables.sql
  - Airflow 배치 적재 대상 raw MergeTree 테이블 정의

- initdb/40_materialized_views.sql
  - 필요 시 raw -> int 수준의 자동 반영 MV 정의

### 8.4 데이터베이스 레이어 권장안

```text
raw_pg_ext   : PostgreSQL 원격 참조용 (소형/임시/PoC)
raw_ch       : PostgreSQL에서 복제/적재된 ClickHouse raw
stg_ch       : 형변환/표준화 계층
int_ch       : 조인/비즈니스 중간 계층
mart_ch      : 대시보드 최종 조회 계층
ops_ch       : 적재 워터마크, 배치 상태, 품질 점검 로그
```

### 8.5 적재 전략 권장안

#### 권장 1순위: Airflow 배치 증분 적재

- PostgreSQL에서 watermark 기준 증분 추출
- ClickHouse raw_ch에 INSERT
- 적재 완료 후 ops_ch에 watermark 기록

장점:

- 현재 Airflow 구조와 가장 잘 맞는다.
- 적재 제어와 재처리가 쉽다.
- experimental 기능 의존을 줄인다.

#### 권장 2순위: CDC 도구 검토

- PostgreSQL WAL 기반 CDC -> ClickHouse

장점:

- 지연이 짧다.

단점:

- 운영 복잡도가 급격히 증가한다.
- 현재 프로젝트 단계에서는 과할 수 있다.

### 8.6 dbt 운영 계획

- 기존 dbt 프로젝트를 바로 변경하기보다 target을 분리하거나 별도 프로젝트를 둔다.
- 초기에 더 안전한 방식은 아래 둘 중 하나다.

#### 안 1. dbt 프로젝트 분리

- dbt/
  - PostgreSQL 분석용 유지
- dbt_clickhouse/
  - ClickHouse 전용 신설

장점:

- 어댑터 충돌과 모델 목적이 섞이지 않는다.
- 배포 파이프라인이 단순하다.

#### 안 2. 단일 dbt 프로젝트 + target 분리

- profile에 postgres, clickhouse target을 같이 둔다.
- 모델 경로를 분리한다.

장점:

- 공통 macro 재사용이 쉽다.

단점:

- 운영자가 target 실수를 낼 수 있다.
- adapter별 제약이 한 프로젝트에 섞인다.

현재 단계에서는 안 1이 더 적절하다.

### 8.7 구현 순서 제안

1. ClickHouse 단일 노드 서비스 폴더 생성
2. raw_ch, mart_ch, ops_ch DB 생성
3. PostgreSQL 대형 fact 2~3개만 증분 적재
4. dbt-clickhouse로 stg/int/mart 최소 모델 작성
5. 기존 대시보드 상위 10개 쿼리로 성능 비교
6. 효과가 확인되면 광고/랭크/주문 요약 marts 확장

### 8.8 PoC 대상 우선순위

가장 먼저 옮길 후보:

- naver_shp.rank 계열
- searchad.report / report_gfa
- coupang_ads.report_pa / report_nca
- smartstore 주문 집계성 fact

나중에 검토할 후보:

- 차원 성격이 강하고 자주 수정되는 기준 테이블
- 운영성 상세 조회 테이블

---

## 9. 최종 한 줄 제안

지금 당장 하나만 고르라면 TimescaleDB가 더 안전하고,
대규모 fact 대시보드까지 고려하면 ClickHouse는 충분히 도입 가치가 있다.

다만 ClickHouse를 쓴다면 "PostgreSQL 원격 JOIN을 바로 dbt에서 돌린다"가 아니라,
"PostgreSQL -> ClickHouse raw 적재 -> dbt-clickhouse로 mart 생성" 구조로 가야 한다.