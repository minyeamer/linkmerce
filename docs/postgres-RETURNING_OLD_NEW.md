# PostgreSQL 18 `RETURNING OLD/NEW` 정리

## 무엇이 달라졌나

PostgreSQL 18에서는 `INSERT`, `UPDATE`, `DELETE`, `MERGE` 구문에서 `RETURNING` 절을 통해 변경 전 행(`OLD`)과 변경 후 행(`NEW`)을 더 직접적으로 다룰 수 있습니다.

이전에는:

- `INSERT`는 주로 새로 들어간 행만 반환
- `UPDATE`는 주로 수정된 최종 행만 반환
- `DELETE`는 삭제된 행만 반환
- 한 쿼리에서 변경 전후 값을 같이 비교하기가 번거로움

PostgreSQL 18에서는 이런 식으로 변경 전후 값을 한 번에 받을 수 있습니다.

```sql
UPDATE product_prices
SET price = price * 1.05
WHERE shop_id = 42
RETURNING
  product_id,
  old.price AS old_price,
  new.price AS new_price,
  new.price - old.price AS price_diff;
```

공식 문서:

- https://www.postgresql.org/docs/18/dml-returning.html

## 왜 유용한가

질문하신 구조처럼 PostgreSQL을 적재용 중간 저장소로 쓰고, 이후 변경분을 ClickHouse에 반영하려는 경우 이 기능이 특히 잘 맞습니다.

보통 이런 정보가 필요합니다.

- 이번 쓰기가 `insert`인지 `update`인지
- 어떤 컬럼이 실제로 바뀌었는지
- 변경 전 값과 변경 후 값이 각각 무엇인지
- ClickHouse로 보낼 변경 이벤트를 어떻게 만들지
- 트리거나 정규화 로직이 반영된 최종 저장값이 무엇인지

`RETURNING`은 트리거 적용 이후의 최종 행 기준으로 값을 돌려주므로, DB 내부에서 후처리를 하고 있다면 그 결과까지 포함해 downstream으로 넘기기 좋습니다.

## 이 기능이 특히 좋은 패턴

### 1. UPSERT 후 변경 내용을 바로 수집

적재 파이프라인에서 가장 실용적인 패턴입니다.

```sql
INSERT INTO product_snapshot (
  shop_id,
  product_id,
  title,
  price,
  updated_at
)
VALUES (
  $1, $2, $3, $4, now()
)
ON CONFLICT (shop_id, product_id)
DO UPDATE SET
  title = excluded.title,
  price = excluded.price,
  updated_at = now()
RETURNING
  shop_id,
  product_id,
  old.title AS old_title,
  new.title AS new_title,
  old.price AS old_price,
  new.price AS new_price,
  CASE
    WHEN old.product_id IS NULL THEN 'insert'
    ELSE 'update'
  END AS change_type,
  new.updated_at;
```

이 패턴의 장점:

- 한 쿼리로 저장
- 한 쿼리로 `insert/update` 판별
- 한 쿼리로 변경 전후 값 확보

특히 `INSERT ... ON CONFLICT DO UPDATE`에서는 충돌이 발생한 경우 `old.*` 값이 채워질 수 있어서, 변경 감지에 아주 유용합니다.

### 2. ClickHouse 반영용 outbox 이벤트 생성

애플리케이션에서 PostgreSQL과 ClickHouse를 동시에 직접 갱신하는 것보다, PostgreSQL 트랜잭션 안에서 outbox 이벤트를 남기는 방식이 더 안전한 경우가 많습니다.

```sql
WITH changed_rows AS (
  INSERT INTO orders_stage (
    order_id,
    status,
    total_amount,
    updated_at
  )
  VALUES ($1, $2, $3, now())
  ON CONFLICT (order_id)
  DO UPDATE SET
    status = excluded.status,
    total_amount = excluded.total_amount,
    updated_at = now()
  RETURNING
    order_id,
    old.status AS old_status,
    new.status AS new_status,
    old.total_amount AS old_total_amount,
    new.total_amount AS new_total_amount,
    CASE
      WHEN old.order_id IS NULL THEN 'insert'
      ELSE 'update'
    END AS change_type,
    new.updated_at
)
INSERT INTO clickhouse_sync_outbox (
  entity_type,
  entity_id,
  change_type,
  payload,
  created_at
)
SELECT
  'order',
  order_id::text,
  change_type,
  jsonb_build_object(
    'old_status', old_status,
    'new_status', new_status,
    'old_total_amount', old_total_amount,
    'new_total_amount', new_total_amount,
    'updated_at', updated_at
  ),
  now()
FROM changed_rows;
```

이 방식의 장점:

- 원본 테이블 반영과 outbox 생성이 같은 트랜잭션에서 처리됨
- ClickHouse 동기화 워커가 안정적으로 읽을 수 있는 변경 로그가 생김
- 무엇이 바뀌었는지 확인하려고 추가 조회를 하지 않아도 됨

### 3. 실제 변화가 없는 update는 downstream 반영 생략

상류 데이터가 자주 들어오지만 값은 그대로인 경우가 많다면, ClickHouse에 불필요한 업데이트를 줄일 수 있습니다.

```sql
WITH changed_rows AS (
  INSERT INTO inventory_stage (
    sku,
    stock_qty,
    updated_at
  )
  VALUES ($1, $2, now())
  ON CONFLICT (sku)
  DO UPDATE SET
    stock_qty = excluded.stock_qty,
    updated_at = now()
  RETURNING
    sku,
    old.stock_qty AS old_stock_qty,
    new.stock_qty AS new_stock_qty,
    new.updated_at
)
SELECT *
FROM changed_rows
WHERE old_stock_qty IS DISTINCT FROM new_stock_qty;
```

이 패턴을 쓰면 실제 값이 달라진 경우만 후속 처리 대상으로 삼을 수 있습니다.

## 현재 구조에 잘 맞는 이유

PostgreSQL을 변경 가능한 staging 계층으로 쓰고, ClickHouse를 최종 분석 저장소로 쓰는 구조에서는 `RETURNING OLD/NEW`가 잘 맞습니다.

잘 맞는 상황:

- PostgreSQL에서 먼저 정규화, 중복 제거, 보정 작업을 수행함
- ClickHouse에는 변경된 행만 보내고 싶음
- 변경 전후 값을 추적해서 디버깅하거나 검증하고 싶음
- 애플리케이션에서 이중 쓰기보다 outbox 패턴을 선호함

## 한계와 주의점

유용한 기능이지만, 이것만으로 완전한 CDC 시스템이 되는 것은 아닙니다.

주의할 점:

- `RETURNING`을 포함한 SQL을 통해 쓰기한 경우에만 그 결과를 바로 받을 수 있음
- 다른 프로세스가 별도 방식으로 테이블을 갱신하면 같은 형식의 변경 이벤트가 자동으로 생기지 않음
- 대량 배치 업데이트에서 반환 결과가 매우 커질 수 있으므로 애플리케이션 처리 전략이 필요함
- 변경 후 최종 값만 필요하다면 `old/new`를 모두 돌려받는 것이 과할 수 있음

## 추천하는 적용 방식

지금 구조에서는 다음 흐름이 가장 현실적입니다.

1. PostgreSQL에 `INSERT ... ON CONFLICT DO UPDATE`
2. `RETURNING`으로 `old.*`, `new.*`, `change_type` 확보
3. 같은 트랜잭션에서 outbox 테이블에 compact event 기록
4. 별도 워커가 outbox를 읽어 ClickHouse 반영
5. 반영 완료 후 outbox 상태 갱신

## 실무 추천

PostgreSQL 18의 `RETURNING OLD/NEW`를 가장 먼저 써볼 만한 곳은 다음입니다.

- 주문, 상품, 재고 같은 mutable staging 테이블
- `UPSERT`가 많은 적재 구간
- ClickHouse 동기화용 변경 이벤트 생성 구간

처음 도입할 때는 모든 컬럼을 다 싣기보다:

- 비즈니스 키
- 변경 판별용 주요 컬럼
- `change_type`
- `updated_at`

정도로만 outbox에 넣는 쪽이 운영하기 편합니다.

공식 참고:

- https://www.postgresql.org/docs/18/dml-returning.html
- https://www.postgresql.org/docs/18/release-18.html
