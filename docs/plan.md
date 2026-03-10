DBTransformer 개선 계획
================================================================================

본 문서는 linkmerce DBTransformer 레이어의 두 가지 문제를 해결하는 설계 계획이다.
  - T-1: SQL 방언 추상화 (다이얼렉트 Jinja 매크로)
  - T-2: fields 선언 기반 JSON 행 정규화 (BinderException 해소)
  - T-3: DuckDBTransformer 선언적 추상화 (json_source / table_keys / table_prefix)


================================================================================
전체 클래스 현황 분석 (T-2 / T-3 적용 범위)
================================================================================

core/ 하위 24개 transform.py 파일의 Transformer 서브클래스를 전수 조사하여
T-2(key 정규화)와 T-3(선언적 추상화) 적용 가능 여부를 분류한다.

패턴 정의
---------
  T3-A  표준 단일 (json_source 설정, params 없음)
  T3-B  단일 + params 자동 언팩 (json_source + params=kwargs)
  T3-C  다중 테이블 표준 (json_source + table_keys + table_prefix)
  T3-D  json_source_path 필요 (JsonTransformer에 path를 runtime에 전달)
  T3-E  비표준 (직접 구현 유지 — Excel/CSV/binary 입력, 이중 소스, 커스텀 삽입)

  T2-A  fields 선언 (JsonTransformer 레벨, 모든 행 필터링 + None 채움)
  T2-C  비표준 (Excel 헤더 정규화 등, 직접 유지)

DuckDBTransformer 현황
----------------------
  T3  클래스                          파일                              json_source
  --- -------------------------------- --------------------------------  -------------------
  A   Stock                           cj/eflexs/stock                   StockList
  A   Inventory                       ecount/api/inventory              InventoryList
  A   Product                         ecount/api/product                ProductList (+ T2-A: fnlChgDt)
  A   Order (sbn)                     sabangnet/admin/order             OrderList
  A   ProductMapping                  sabangnet/admin/order             ProductList
  A   Campaign (gfa)                  searchad/gfa/adreport             Content
  A   CafeTab                         naver/main/search                 CafeList
  A   CafeArticle                     naver/main/search                 CafeArticleJson

  B   SkuMapping                      sabangnet/admin/order             SkuList
  B   MarketingChannel                smartstore/api/bizdata            MarketingChannelList
  B   OrderTime                       smartstore/api/order              OrderList
  B   OrderStatus                     smartstore/api/order              OrderStatusList (+ T2-A)
  B   AdSet (gfa)                     searchad/gfa/adreport             Content
  B   Creative (gfa)                  searchad/gfa/adreport             Content
  B   RocketInventory                 coupang/wing/product              RocketInventorylist
  B   RocketOption                    coupang/wing/product              RocketInventorylist
  B   RocketSettlement                coupang/wing/settlement           RocketSettlementList
  B   _AdTransformer (google)         google/api/ads                    AdResults
  B   _AdTransformer (meta)           meta/api/ads                      AdObjects
      └ Campaign, AdGroup, Insight, Asset (google) — 부모 상속
      └ Campaigns, Adsets, Ads (meta)             — 부모 상속
  B   DailyReport (← csv, custom)    searchad/manage/adreport          AdvancedReport (T3-B but custom csv parse)

  C   Order (smartstore)              smartstore/api/order              OrderList
      table_keys=["order","product_order","delivery","option"], prefix="smartstore"
  C   (Insights는 prefix 로직이 비표준 → T3-E)

  D   _SearchTransformer + 파생 8종   naver/openapi/search              SearchItems(path=["items"])
  D   BrandCatalog, BrandProduct      smartstore/brand/catalog          CatalogItems(path=["items"])
  D   _PageView + 파생 3종            smartstore/brand/pageview         PageViewItems (path 고정, T3-B 가능)
  D   _SalesTransformer + 파생 3종    smartstore/brand/sales            SalesList(path 동적) → property 사용

  E   Ad, AssetView                   google/api/ads                    AdList/AssetViewList (커스텀 parse)
  E   Insights                        meta/api/ads                      AdObjects (prefix 비표준)
  E   Campaign (coupang ads)          coupang/advertising/adreport      이중 소스 (CampaignList+AdgroupList)
  E   Creative (coupang ads)          coupang/advertising/adreport      커스텀 list 변환 후 삽입
  E   ProductAdReport                 coupang/advertising/adreport      Excel 입력
  E   NewCustomerAdReport             coupang/advertising/adreport      Excel 입력
  E   ProductOption (coupang)         coupang/wing/product              by="option" 모드 분기
  E   ProductDetail                   coupang/wing/product              insert key 조건분기
  E   ProductDownload                 coupang/wing/product              Excel + request_type 분기
  E   RocketSettlementDownload        coupang/wing/settlement           binary 입력 + 다중시트
  E   OrderDownload                   sabangnet/admin/order             Excel + download_type 생성자
  E   OrderStatus (sbn)               sabangnet/admin/order             Excel + 동적 render
  E   Option (sabangnet)              sabangnet/admin/product           이중 소스 + 헤더 정규화
  E   OptionDownload (sabangnet)      sabangnet/admin/product           Excel 입력
  E   AddProductGroup, AddProduct     sabangnet/admin/product           obj 직접 접근
  E   Product (sbn)                   sabangnet/admin/product           커스텀 parse
  E   Option (smartstore)             smartstore/api/product            이중 소스 (simple/comb/supplement)
  E   BrandPrice                      smartstore/brand/catalog          insert_into_table 오버라이드
  E   ProductCatalog                  smartstore/brand/catalog          insert_into_table 오버라이드
  E   AggregatedSales                 smartstore/brand/sales            split_params 커스텀
  E   ShoppingRank                    naver/openapi/search              insert_into_table 오버라이드
  E   ExposureRank                    searchad/manage/exposure          reparse_object + split_params
  E   Ad (searchad/api)               searchad/api/adreport             멀티 TSV 타입
  E   Search                          naver/main/search                 HTML 파서 + 직접 SQL
  E   Keyword                         searchad/api/keyword              max_rank 슬라이스
  E   TimeContract, BrandNewContract  searchad/api/contract             obj가 이미 list
  E   _PerformanceReport 파생 2종      searchad/gfa/adreport             zip+CSV 입력

JsonTransformer 현황 (T-2 fields / body)
--------------------------------------------------
  T2  클래스               파일                          패턴
  --- -------------------- ---------------------------   --------------------------------
  A   OrderList            smartstore/api/order          validate_content + 4× validate_*
                                                         → fields 선언으로 완전 대체 ✓
  A   Product              smartstore/api/product        validate_product (5 keys)
                                                         → fields 선언 + _apply_fields ✓

  A   Product              sabangnet/admin/product       obj[0]["fnlChgDt"] 패치 → fields에 포함 ✓
  A   Option               sabangnet/admin/product       obj[0]["fnlChgDt"] 패치 → fields에 포함 ✓
  A   OrderStatus          smartstore/api/order          validate_change_status → OrderStatusList.fields ✓
  A   ExposureRank         searchad/manage/exposure      reparse_object → AdList.fields에 포함 ✓

  C   OptionDownload       sabangnet/admin/product       Excel 헤더 정규화 → 직접 유지

HtmlTransformer 현황
---------------------
  모두 naver/main/search/transform.py에 위치. HTML DOM 파싱 특화.
  T-2/T-3 추상화 대상이 아님. 별도 계획 없음.

  클래스                  부모
  ----------------------  ----------------------------
  SearchSection           HtmlTransformer
  PowerLink               HtmlTransformer
  RelatedKeywords         HtmlTransformer
  _ShoppingTransformer    HtmlTransformer
  Shopping                _ShoppingTransformer
  NewShopping             Shopping
  CafeList                HtmlTransformer

T-3 적용 효과 요약
------------------
  적용 가능 (T3-A/B/C/D):  약 30개 클래스 (transform() 직접 구현 제거)
  직접 구현 유지 (T3-E):    약 20개 클래스 (비표준 입력/로직)
  중간 base 클래스 효과:    _AdTransformer(google), _AdTransformer(meta),
                            _SearchTransformer, _SalesTransformer, _PageView 등
                            → base 클래스에서 json_source 선언 시 파생 클래스 전체 혜택


================================================================================
현황 분석
================================================================================

구조 요약
---------
  Transformer (ABC)
  └── DBTransformer (ABC)     ← 쿼리/테이블/모델 관리, Jinja render
      └── DuckDBTransformer   ← 유일한 구체 구현체
            .insert_into_table(obj, ...)
              → expr_values() → render SELECT query
              → conn.execute(INSERT INTO table (SELECT ...), obj=obj)

DuckDB의 UNNEST 패턴 (expr_array):
  (SELECT obj.* FROM (SELECT UNNEST($obj) AS obj))
  → $obj 파라미터로 list[dict] 전달
  → dict의 키가 자동으로 컬럼이 됨 (스키마는 obj[0]에서 추론)

문제 1: DB 방언 고착
-------------------
  models.sql에 DuckDB 전용 함수가 하드코딩되어 있어 다른 DB 백엔드로 교체가
  불가능하다. 현재 사용 중인 DuckDB 전용 문법:

  함수                                  출현 위치
  ---------------------------------     ----------------------------
  TRY_CAST(x AS T)                      전체 models.sql (60+ 곳)
  TRY_STRPTIME(x, fmt)                  naver, cj, smartstore 외
  QUALIFY ROW_NUMBER() OVER (...) = 1   3개 models.sql
  REGEXP_EXTRACT(str, pat, group)       naver search models.sql
  product.identifier                    smartstore, coupang (중첩 struct 접근)

  → DuckDB가 아닌 다른 DB에서는 위 문법이 하나도 통하지 않음.

문제 2: BinderException
-----------------------
  DuckDB는 UNNEST 시 obj[0]으로 컬럼 스키마를 결정함.
  obj[1+]에 obj[0]에 없는 키가 있거나, obj[0]에 있는 키가 이후에 없으면
  BinderException 발생.

  현재 workaround: transform.py 개별 파일에서 obj[0]["key"] = obj[0].get("key")
  패턴을 수동으로 작성. 이는:
    - 누락 시 런타임 오류 → 조회 가능 기간 초과 시 복구 불가
    - 클래스마다 파편적으로 작성되어 보장 범위가 불분명함


================================================================================
T-1. SQL 방언 추상화 (Jinja 다이얼렉트 매크로)
================================================================================

설계 원칙
---------
  - models.sql에 DB 종류를 직접 명시하지 않는다. 방언 함수는 Jinja 매크로로 호출.
  - 렌더링 시점에 dialect에 맞는 매크로 구현체를 Jinja 환경에 주입한다.
  - 모든 DB에서 동일하게 동작하는 표준 SQL (CAST, TO_TIMESTAMP, CTE 등)은
    그대로 쓴다. 방언 매크로는 "DB가 다르면 결과를 낼 수 없는" 함수에만 적용.

T-1-1. 다이얼렉트 매크로 레지스트리 (linkmerce/utils/dialect.py 신설)
----------------------------------------------------------------------
  매크로는 Python callable로 등록. Jinja 템플릿에서 함수처럼 호출.

  # 매크로 시그니처 (models.sql 작성 기준):
  {{ try_cast(expr, type) }}            ← 안전 형변환
  {{ to_date(expr, fmt) }}              ← 날짜 파싱
  {{ to_timestamp(expr, fmt) }}         ← 타임스탬프 파싱
  {{ regexp_extract(expr, pat, g=1) }}  ← 정규식 추출 (g=그룹번호)

  구현 예시 (dialect.py):

    DIALECT_MACROS = {
        "duckdb": {
            "try_cast": lambda e, t: f"TRY_CAST({e} AS {t})",
            "to_date": lambda e, f: f"TRY_CAST(TRY_STRPTIME({e}, '{f}') AS DATE)",
            "to_timestamp": lambda e, f: f"TRY_CAST(TRY_STRPTIME({e}, '{f}') AS TIMESTAMP)",
            "regexp_extract": lambda e, p, g=1: f"REGEXP_EXTRACT({e}, '{p}', {g})",
        },
        "postgresql": {
            "try_cast": lambda e, t: f"NULLIF({e}, '')::text::{t}",  # 주: safe_cast UDF 필요 검토
            "to_date": lambda e, f: f"TO_DATE({e}, '{_strptime_to_pg(f)}')",
            "to_timestamp": lambda e, f: f"TO_TIMESTAMP({e}, '{_strptime_to_pg(f)}')",
            "regexp_extract": lambda e, p, g=1: f"SUBSTRING({e} FROM '{p}')",
        },
    }

    def get_macros(dialect: str) -> dict:
        return DIALECT_MACROS.get(dialect, DIALECT_MACROS["duckdb"])

  날짜 포맷 변환 (DuckDB strptime → PG to_date):
    %Y → YYYY,  %m → MM,  %d → DD,  %H → HH24,  %M → MI,  %S → SS
    %a → Day  (영문 요일, PG: "Dy"),  %b → Mon  (영문 월, PG: "Mon")

T-1-2. DBTransformer.render_query() 확장
-----------------------------------------
  dialect 파라미터를 추가하고, Jinja 환경에 매크로를 주입.

    class DBTransformer:
        dialect: str = "duckdb"  ← 클래스 레벨 기본값

        def render_query(self, query_: str, render: dict | None = None) -> str:
            from linkmerce.utils.dialect import get_macros
            from linkmerce.utils.jinja import render_string
            context = dict(get_macros(self.dialect), **(render or {}))
            return render_string(query_, **context)

  DBTransformer 생성자:
    def __init__(self, dialect: str = "duckdb", ...):
        self.dialect = dialect
        ...

  API 레이어 호출 변경:
    # 기존
    transformer = BookSearch()
    # 신규 (기본값 duckdb이므로 기존 코드 변경 불필요, 필요 시 명시)
    transformer = BookSearch(dialect="postgresql")

T-1-3. QUALIFY → CTE 패턴으로 표준화
--------------------------------------
  QUALIFY는 DuckDB/Snowflake 전용. PostgreSQL에 없는 절이며 CTE로 대체 가능.
  CTE 패턴은 DuckDB·PostgreSQL 모두 지원.

  AS-IS (DuckDB 전용):
    SELECT
        col1, col2, ...
    FROM {{ array }}
    WHERE condition
    QUALIFY ROW_NUMBER() OVER (PARTITION BY col1 ORDER BY col2 DESC) = 1;

  TO-BE (표준 CTE, 두 DB 모두 지원):
    WITH ranked AS (
        SELECT
            col1, col2, ...
          , ROW_NUMBER() OVER (PARTITION BY col1 ORDER BY col2 DESC) AS __rn
        FROM {{ array }}
        WHERE condition
    )
    SELECT col1, col2, ... FROM ranked WHERE __rn = 1;

  대상 파일 (3개):
    - core/sabangnet/admin/order/models.sql    (line 149)
    - core/smartstore/brand/sales/models.sql   (line 150)
    - core/smartstore/api/order/models.sql     (line 180)

  QUALIFY 매크로 방식은 쿼리 구조 전체를 변경해야 하므로 Jinja 매크로가 아닌
  기계적 1회 마이그레이션으로 처리한다.

T-1-4. {{ array }} 추상화 계층
--------------------------------
  현재 DuckDBTransformer.render_query()가 {{ array }}를 DuckDB UNNEST 표현식으로
  치환한다. PostgreSQLTransformer에서는 이를 다르게 치환하면 된다.

  DuckDB:    {{ array }} → (SELECT obj.* FROM (SELECT UNNEST($obj) AS obj))
  PostgreSQL: {{ array }} → _tmp_obj  (사전에 채워진 임시 테이블)

  models.sql의 {{ array }} 심볼은 그대로 유지. 각 DBTransformer 서브클래스가
  expr_array() 메서드에서 DB별 표현식을 반환한다.

T-1-5. 중첩 구조 접근 (DuckDB struct dot-notation)
---------------------------------------------------
  DuckDB는 UNNEST된 dict의 중첩 필드를 점 표기법으로 접근한다:
    product.identifier, content.order.orderId 등

  이는 DuckDB 전용 struct 접근이며 PostgreSQL에서는 지원하지 않는다.
  현재 이 패턴을 사용하는 모델: smartstore, coupang wing 등

  해결 방향:
    A) DuckDB 전용 유지 (현재 아키텍처에서 DuckDB가 transform 엔진이므로 OK)
    B) 장기적으로 PostgreSQLTransformer는 사전 플랫닝(pre-flatten) 입력을 받는
       별도 모델 파일을 사용

  T-1 범위에서는 A안으로 처리. 중첩 dot-notation은 DuckDB 전용이므로
  dialect 매크로 대상이 아니며 현재 그대로 유지한다.


================================================================================
T-2. fields 선언 기반 JSON 행 정규화
================================================================================

문제 재확인
-----------
  DuckDB의 UNNEST 패턴: obj[0]의 키 집합으로 컬럼 스키마를 결정.
  → obj[i]에 없는 키를 SELECT에서 참조하면 BinderException.
  → obj[0]에 있는 키를 obj[i]에서 누락해도 BinderException.

  현재 수동 패치:
    obj[0]["missing_key"] = obj[0].get("missing_key")  ← transform.py마다 작성

핵심 아이디어
--------------
  모든 JsonTransformer 서브클래스에서 fields를 명시적으로 선언한다.
  transform() 시 각 행을 fields에 선언된 키만 남기고, 없는 키는 None으로 채운다.

  → 모든 행이 동일한 키 집합을 가짐 → DuckDB UNNEST obj[0] 스키마 = 전체 행 스키마
  → BinderException 근본 해소. 기존 obj[0]["key"] = obj[0].get() 패치 코드 불필요.

  예시 (중첩 딕셔너리 형식):
    fields = {"key1": {"key2": ["a", "b"]}, "key3": None}
    입력: {}
    출력: {"key1": {"key2": {"a": None, "b": None}}, "key3": None}

    fields = {"key1": {"key2": ["a", "b"]}, "key3": None}
    입력: {"key1": {"key2": {"a": 1, "x": 99}}, "key3": "y", "unrelated": "z"}
    출력: {"key1": {"key2": {"a": 1, "b": None}}, "key3": "y"}   ← x, unrelated 제거

T-2-1. utils/map.py — filter_fields() 구현
-------------------------------------------------
  변수명 확정:
    body    ← 기존 path 속성 대체 (JSON 경로 탐색 기점, dot-notation 문자열)
    fields  ← 스키마 선언 (nested dict 또는 list[str])

  def _parse_list_key(key: str) -> tuple[str, int | None]:
    """"key[N]" 표기를 (key, N)으로 분해."""

  def filter_fields(row: dict, fields: dict | list) -> dict:
    """
    nested dict 형식의 fields 선언에 따라 row에서 필드를 추출한다.
    선언된 키만 결과에 포함되고, 없는 키는 None으로 채운다.

    fields 형식:
        {key: None}         → key 값을 그대로 포함 (없으면 None)
        {key: list[str]}    → key 하위 dict에서 목록의 키만 추출
        {key: dict}         → key 하위 dict에 대해 재귀 적용
        {"key[N]": spec}    → key[N] 리스트 인덱스 접근 → 결과는 key: [spec_result]
        list[str]           → {k: None for k in list} 의 shorthand (평탄한 스키마)
    """

  동작 예시:
    filter_fields({}, {"a": {"b": ["c"]}, "d": None})
    → {"a": {"b": {"c": None}}, "d": None}

    filter_fields({"a": {"b": {"c": 1, "x": 2}, "y": 3}, "d": 4, "e": 5},
                  {"a": {"b": ["c"]}, "d": None})
    → {"a": {"b": {"c": 1}}, "d": 4}          ← x, y, e 제거

    filter_fields({"items": [{"id": 1, "name": "n", "extra": "x"}]},
                  {"items[0]": ["id", "name"]})
    → {"items": [{"id": 1, "name": "n"}]}

    filter_fields({}, {"items[0]": ["id"]})
    → {"items": [{"id": None}]}

T-2-2. JsonTransformer에 body / fields 속성 + transform() 수정
----------------------------------------------------------
  class JsonTransformer:
      body: str | None = None           ← 기존 path 대체 (dot-notation 문자열, e.g. "data.contents")
      path: list | None = None          ← 하위 호환 유지 (기존 서브클래스 무변경)
      fields: dict | list | None = None ← 스키마 선언 (nested dict 또는 list[str])

      def parse(self, obj, **kwargs):
          # body 우선, 없으면 path fallback
          if self.body is not None:
              from linkmerce.utils.map import hier_get
              return hier_get(obj, self.body.split("."))
          elif self.path is not None:
              from linkmerce.utils.map import hier_get
              return hier_get(obj, self.path)
          return obj

      def _apply_fields(self, result):
          """fields 선언에 따라 결과 행을 필터링한다. transform()에서 parse() 직후 호출."""
          if result is None or self.fields is None:
              return result
          from linkmerce.utils.map import filter_fields
          if isinstance(result, list):
              return [filter_fields(row, self.fields) for row in result]
          return filter_fields(result, self.fields)

      def transform(self, obj, **kwargs):
          if isinstance(obj, self.dtype):
              if self.is_valid_response(obj):
                  return self._apply_fields(self.parse(obj, **kwargs))
          ...

  transform()을 완전히 오버라이드하는 서브클래스 (ProductList 등)에서는
  직접 self._apply_fields(result)를 호출하여 fields를 적용한다.

  fields가 None이면 기존 동작 그대로 (하위 호환 완전 보장).

T-2-3. T2-A JsonTransformer 서브클래스에 fields 선언
---------------------------------------------------------
  기존 validate_* 메서드를 제거하고 fields 선언으로 대체.
  기존 obj[0]["fnlChgDt"] = obj[0].get(...) 패치 코드 제거.

  OrderList Before/After:
    AS-IS: validate_content + 4개 validate_* 메서드, 72줄

    TO-BE:
      class OrderList(JsonTransformer):
          dtype   = dict
          body    = "data.contents"
          fields  = {
              "productOrderId": None,
              "content": {
                  "order": ["orderId", "ordererNo", "ordererId", "ordererName",
                            "payLocationType", "orderDate", "paymentDate"],
                  "productOrder": {
                      "merchantChannelId": None, "productId": None, "optionCode": None,
                      "sellerProductCode": None, "optionManageCode": None,
                      "productOrderStatus": None, "claimStatus": None,
                      "productClass": None, "productName": None, "productOption": None,
                      "inflowPath": None, "inflowPathAdd": None,
                      "deliveryAttributeType": None, "deliveryTagType": None,
                      "quantity": None, "unitPrice": None, "optionPrice": None,
                      "deliveryFeeAmount": None, "productDiscountAmount": None,
                      "sellerBurdenDiscountAmount": None, "totalPaymentAmount": None,
                      "paymentCommission": None, "expectedSettlementAmount": None,
                      "decisionDate": None,
                      "shippingAddress": ["zipCode", "latitude", "longitude"],
                  },
                  "delivery": ["trackingNumber", "deliveryCompany", "deliveryMethod",
                               "pickupDate", "sendDate", "deliveredDate"],
                  "completedClaims[0]": ["claimType", "claimRequestAdmissionDate"],
              },
          }

  sabangnet/ProductList, OptionList fnlChgDt 패치 제거:
    AS-IS: obj[0]["fnlChgDt"] = obj[0].get("fnlChgDt")  ← transform.py에 직접 작성
    TO-BE: ProductList / OptionList의 fields 리스트에 "fnlChgDt" 포함
           (SQL SELECT에서 사용하는 모든 키를 fields로 선언 → 전체 로우 스키마 보장)

  주: ecount/api/product/Product는 fnlChgDt 패치가 존재하지 않음 (계획 오기재).


================================================================================
T-3. DuckDBTransformer 선언적 추상화
================================================================================

설계 원칙
---------
  현재 DuckDBTransformer 서브클래스마다 다음 3개 메서드를 반복 구현한다:
    - set_tables():   테이블명 등록
    - create_table(): 테이블 생성 DDL 실행
    - transform():    JsonTransformer 파싱 + insert_into_table() 호출

  이 3개 메서드의 구조는 항상 동일한 패턴이다:
    - set_tables:   {key: f"{prefix}_{key}" for key in keys} 딕셔너리 생성
    - create_table: table_keys 순환하며 create 쿼리 실행
    - transform:    JsonTransformer().transform(obj) → 파싱 → 삽입

  선언적 클래스 속성으로 이 패턴을 추상화하여 서브클래스에서
  직접 구현을 제거한다. 복잡한 경우 (searchad/adreport 등)는 기존 방식 유지.

신규 클래스 속성 (DuckDBTransformer)
-------------------------------------
  json_source: type[JsonTransformer] | None = None
    → transform()에서 obj를 파싱할 JsonTransformer 클래스
    → None이면 transform()을 직접 구현 (기존 방식 그대로)

  json_source_path: list | None = None
    → json_source 인스턴스화 시 path= 인자로 전달.
    → None이면 path 생략 (JsonTransformer 클래스 속성의 default 사용).
    → e.g., CatalogItems(path=["items"]).transform(obj) →
           json_source=CatalogItems, json_source_path=["items"]
    → 경로가 다른 class attr에서 동적으로 생성되는 경우 (e.g., _SalesTransformer):
           _parse_source() 메서드 오버라이드로 처리.

  table_keys: list[str] | None = None
    → 다중 테이블 모드. 각 키는 테이블명 접미사 부분.
    → None이면 단일 테이블 모드.

  table_prefix: str | None = None
    → 테이블명 프리픽스. table_keys와 조합하여 실제 테이블명 생성.
    → f"{table_prefix}_{key}" 패턴.
    → e.g., table_prefix="smartstore", key="order" → "smartstore_order"
    → 비표준 prefix 로직 (Insights의 meta_insights vs meta_metrics 등)은
           set_tables()를 직접 구현하거나 table_prefix 미사용.

T-3-1. 자동화되는 동작
-----------------------
  _parse_source() — json_source 호출 가상 메서드:
    def _parse_source(self, obj: JsonObject) -> list | None:
        if self.json_source is None:
            return None
        if self.json_source_path is not None:
            return self.json_source(path=self.json_source_path).transform(obj)
        return self.json_source().transform(obj)

    → 동적 path가 필요한 _SalesTransformer 등은 이 메서드를 오버라이드한다.

  queries 자동 계산:
    - table_keys 설정 시:
        [f"{kw}_{key}" for key in table_keys for kw in ["create", "select", "insert"]]
    - json_source만 설정 시 (단일 테이블):
        ["create", "select", "insert"]
    - 둘 다 None: 서브클래스에서 queries 직접 선언 (기존 방식)

  set_tables() 자동:
    - table_keys + table_prefix 설정 시:
        base = {key: f"{table_prefix}_{key}" for key in table_keys}
        super().set_tables(dict(base, **(external_tables or {})))

  create_table() 자동:
    - table_keys 설정 시:
        for key in table_keys:
            super().create_table(key=f"create_{key}", table=f":{key}:")

  transform() 기본 구현:
    - json_source 설정 시:
        def transform(self, obj, **kwargs):
            data = self._parse_source(obj)
            if not data:
                return
            if self.table_keys:
                for key in self.table_keys:
                    self.insert_into_table(data,
                        key=f"insert_{key}",
                        table=f":{key}:",
                        values=f":select_{key}:")
            else:
                self.insert_into_table(data, params=kwargs or None)
    - json_source가 None이면 NotImplementedError 발생

T-3-2. Before / After 비교 (smartstore/api/order/transform.py)
---------------------------------------------------------------
  AS-IS (Order — 다중 테이블):
    ORDER_TABLES = ["order", "product_order", "delivery", "option"]

    class Order(DuckDBTransformer):
        queries = [f"{keyword}_{table}"
            for table in ORDER_TABLES
            for keyword in ["create", "select", "insert"]
        ]

        def set_tables(self, tables: dict | None = None):
            base = {table: f"smartstore_{table}" for table in ORDER_TABLES}
            super().set_tables(dict(base, **(tables or dict())))

        def create_table(self, **kwargs):
            for table in ORDER_TABLES:
                super().create_table(key=f"create_{table}", table=f":{table}:")

        def transform(self, obj: JsonObject, **kwargs):
            orders = OrderList().transform(obj)
            if orders:
                for table in ORDER_TABLES:
                    self.insert_into_table(orders,
                        key=f"insert_{table}",
                        table=f":{table}:",
                        values=f":select_{table}:")

  TO-BE (Order):
    class Order(DuckDBTransformer):
        table_keys   = ["order", "product_order", "delivery", "option"]
        table_prefix = "smartstore"
        json_source  = OrderList

  → 15줄 → 3줄.

  AS-IS (OrderTime — 단일 테이블):
    class OrderTime(DuckDBTransformer):
        queries = ["create", "select", "insert"]

        def transform(self, obj: JsonObject, channel_seq: int | str | None = None, **kwargs):
            orders = OrderList().transform(obj)
            if orders:
                self.insert_into_table(orders, params=dict(channel_seq=channel_seq))

  TO-BE (OrderTime):
    class OrderTime(DuckDBTransformer):
        json_source = OrderList

  → channel_seq는 **kwargs로 전달 → params=kwargs로 자동 언팩.
  → queries 선언 불필요 (json_source 설정 시 자동).

T-3-3. 적용 범위 및 복잡도 분류
---------------------------------
  T3-D 추가 Before/After (_SearchTransformer — json_source_path 패턴):

    AS-IS (_SearchTransformer):
      class _SearchTransformer(DuckDBTransformer):
          content_type: Literal[...]
          queries: list[str] = ["create", "select", "insert"]

          def transform(self, obj: JsonObject, query: str, start: int = 1, **kwargs):
              items = SearchItems(path=["items"]).transform(obj)
              if items:
                  params = dict(keyword=query, start=(start-1))
                  self.insert_into_table(items, params=params)

    TO-BE (_SearchTransformer):
      class _SearchTransformer(DuckDBTransformer):
          content_type: Literal[...]
          json_source      = SearchItems
          json_source_path = ["items"]

    → transform()에서 파라미터명 변환(query→keyword, start→start-1)이 있으므로
      params 언팩 로직은 직접 유지하거나 파생 클래스의 파라미터를 그대로 쓴다.
      (query/start 이름 변환은 T-3 표준 params=kwargs 패턴에서 벗어남 → 하이브리드)

    실용적 절충안:
      class _SearchTransformer(DuckDBTransformer):
          content_type: Literal[...]
          json_source_path = ["items"]

          def _parse_source(self, obj):
              return SearchItems(path=["items"]).transform(obj)  # 명시적 경로 유지

    → queries/set_tables/create_table은 자동, transform()만 유지:
      def transform(self, obj, query, start=1, **kwargs):
          items = self._parse_source(obj)
          if items:
              self.insert_into_table(items, params=dict(keyword=query, start=start-1))

  T3-D 추가 Before/After (_SalesTransformer — 동적 경로):

    AS-IS (_SalesTransformer):
      class _SalesTransformer(DuckDBTransformer):
          sales_type: Literal["store","category","product"]
          queries: list[str] = ["create", "select", "insert"]

          def transform(self, obj, mall_seq=None, start_date=None, end_date=None, **kwargs):
              sales = SalesList(path=["data",f"{self.sales_type}Sales"]).transform(obj)
              if sales:
                  params = dict(mall_seq=mall_seq, end_date=end_date)
                  if self.start_date:
                      params.update(start_date=start_date)
                  self.insert_into_table(sales, params=params)

    TO-BE (_SalesTransformer):
      class _SalesTransformer(DuckDBTransformer):
          sales_type: Literal["store","category","product"]
          start_date: bool = False

          def _parse_source(self, obj):
              return SalesList(path=["data", f"{self.sales_type}Sales"]).transform(obj)

          def transform(self, obj, mall_seq=None, start_date=None, end_date=None, **kwargs):
              sales = self._parse_source(obj)
              if sales:
                  params = dict(mall_seq=mall_seq, end_date=end_date)
                  if self.start_date:
                      params.update(start_date=start_date)
                  self.insert_into_table(sales, params=params)

    → transform() 직접 구현 유지 (동적 params 조건분기 때문)
    → 파생 클래스(StoreSales, CategorySales, ProductSales)는 queries 선언도 불필요:
      class StoreSales(_SalesTransformer):
          sales_type = "store"
      class CategorySales(_SalesTransformer):
          sales_type = "category"
      class ProductSales(_SalesTransformer):
          sales_type = "product"

  직접 구현 유지 (비표준 T3-E):
    - coupang/advertising: Campaign(이중 소스), ProductAdReport(Excel)
    - coupang/wing: ProductOption(by= 분기), ProductDownload(Excel), RocketSettlementDownload(binary 다중)
    - sabangnet/admin: OrderDownload(Excel + 생성자), OrderStatus(Excel + render), Option/OptionDownload
    - sabangnet/admin: AddProductGroup, AddProduct (obj 직접 접근)
    - smartstore/api: Option (이중 소스 + 다중 쿼리키)
    - smartstore/brand: BrandPrice, ProductCatalog, AggregatedSales (insert_into_table 오버라이드)
    - naver/openapi: ShoppingRank (insert_into_table 오버라이드)
    - searchad/manage: ExposureRank (reparse + split_params)
    - searchad/api: Ad (멀티 TSV)
    - naver/main: Search (HTML + 직접 SQL)
    - searchad/api: Keyword (max_rank 슬라이스)
    - searchad/api: TimeContract, BrandNewContract (obj가 이미 list)
    - searchad/gfa: _PerformanceReport (zip+CSV)

  T-3 속성은 클래스에 선언되지 않으면 기존 동작 그대로.
  기존 서브클래스는 수정 없이 동작 (하위 호환 완전 보장).


================================================================================
구현 단계
================================================================================

우선순위: T-2 먼저 (즉각적 BinderException 해소), T-1 다음 (구조적 개선)

  T-2-1. linkmerce/utils/fill.py 신설
          project_paths(row, paths) 구현
          단위 테스트: 빈 dict, 부분 dict, 초과 키 포함 dict, list 인덱스 경로로 검증

  T-2-2. JsonTransformer에 fields 클래스 속성 추가 및 transform() 수정
          fields = None (기본값) → None이면 기존 동작 그대로 (하위 호환)

  T-2-3. 모든 JsonTransformer 서브클래스에 fields 선언
          기존 validate_* 메서드 제거
          기존 obj[0]["key"] = obj[0].get("key") 패치 코드 제거

  ---

  T-1-0. linkmerce/utils/dialect.py 신설
          DIALECT_MACROS 딕셔너리 + get_macros(dialect) 함수
          _strptime_to_pg(fmt) 포맷 변환 함수

  T-1-1. DBTransformer 수정
          dialect: str = "duckdb" 클래스 속성 추가
          __init__에 dialect 파라미터 추가
          render_query()에 get_macros(self.dialect) 주입

  T-1-2. DuckDBTransformer 수정
          dialect 기본값을 "duckdb"로 명시적 유지
          render_query() 오버라이드: {{ array }} 치환 유지 (기존 로직 그대로)

  T-1-3. models.sql QUALIFY → CTE 마이그레이션 (3개 파일)
          sabangnet/admin/order, smartstore/brand/sales, smartstore/api/order

  T-1-4. models.sql TRY_CAST 등 → Jinja 매크로 마이그레이션 (점진적)
          우선순위: BinderException이 자주 발생하는 파일부터
          전체 60+ 지점을 한번에 변경하지 않고 파일 단위로 진행

  ---

  T-3-0. DuckDBTransformer에 클래스 속성 추가
          json_source, table_keys, table_prefix = None (기본값)

  T-3-1. queries 자동 계산 구현
          __init_subclass__ 또는 __init__ 시점에 table_keys/json_source 기반으로
          queries 자동 설정 (서브클래스에서 명시적으로 선언하지 않은 경우)

  T-3-2. set_tables() 기본 구현 추가
          table_keys + table_prefix 설정 시 자동 테이블 등록

  T-3-3. create_table() 기본 구현 추가
          table_keys 설정 시 자동 루프 실행

  T-3-4. transform() 기본 구현 추가
          json_source 설정 시 단일/다중 자동 분기

  T-3-5. core/smartstore/api/order/transform.py 적용
          Order, OrderTime, OrderStatus → 선언적 속성으로 교체

  T-3-6. 기타 표준 패턴 DuckDBTransformer 전체 적용
          T3-A/B 우선: Stock, Inventory, Product(ecount), Order(sbn),
                       ProductMapping, Campaign(gfa), CafeTab, CafeArticle,
                       SkuMapping, MarketingChannel, AdSet(gfa), Creative(gfa),
                       RocketInventory, RocketOption, RocketSettlement
          T3-B 중간: _AdTransformer(google), _AdTransformer(meta)
          T3-C: Order(smartstore) — 다중 테이블
          T3-D: _SearchTransformer(_parse_source 오버라이드), BrandCatalog, BrandProduct


================================================================================
models.sql 마이그레이션 가이드
================================================================================

함수 치환 대조표
----------------
  AS-IS (DuckDB 전용)                          TO-BE (방언 중립)
  -------------------------------------------  ------------------------------------------
  TRY_CAST(x AS INTEGER)                       {{ try_cast('x', 'INTEGER') }}
  TRY_CAST(x AS BIGINT)                        {{ try_cast('x', 'BIGINT') }}
  TRY_CAST(x AS DATE)                          {{ try_cast('x', 'DATE') }}
  TRY_CAST(x AS TIMESTAMP)                     {{ try_cast('x', 'TIMESTAMP') }}
  TRY_CAST(TRY_STRPTIME(x, '%Y%m%d') AS DATE)  {{ to_date('x', '%Y%m%d') }}
  TRY_CAST(TRY_STRPTIME(x, fmt) AS TIMESTAMP)  {{ to_timestamp('x', fmt) }}
  REGEXP_EXTRACT(str, pat, 1)                  {{ regexp_extract('str', pat) }}
  REGEXP_EXTRACT(str, pat, 2)                  {{ regexp_extract('str', pat, 2) }}

QUALIFY 마이그레이션 예시 (sabangnet/admin/order/models.sql)
------------------------------------------------------------
  AS-IS:
    SELECT
        col1, col2, col3 ...
    FROM {{ array }}
    WHERE condition
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TRY_CAST("계정등록순번" AS INTEGER), "상품코드(쇼핑몰)"
        ORDER BY TRY_CAST("주문일시(YYYY-MM-DD HH:MM)" AS TIMESTAMP) DESC) = 1;

  TO-BE:
    WITH ranked AS (
        SELECT
            col1, col2, col3 ...
          , ROW_NUMBER() OVER (
                PARTITION BY {{ try_cast('"계정등록순번"', 'INTEGER') }}, "상품코드(쇼핑몰)"
                ORDER BY {{ try_cast('"주문일시(YYYY-MM-DD HH:MM)"', 'TIMESTAMP') }} DESC
            ) AS __rn
        FROM {{ array }}
        WHERE condition
    )
    SELECT col1, col2, col3 ... FROM ranked WHERE __rn = 1;

단순 치환 예시 (naver/openapi/search/models.sql)
------------------------------------------------
  AS-IS:
    , TRY_CAST(discount AS INTEGER) AS sales_price
    , TRY_CAST(TRY_STRPTIME(pubdate, '%Y%m%d') AS DATE) AS publish_date
    , TRY_CAST(REGEXP_EXTRACT(link, '/products/(\d+)$', 1) AS BIGINT) AS product_id

  TO-BE:
    , {{ try_cast('discount', 'INTEGER') }} AS sales_price
    , {{ to_date('pubdate', '%Y%m%d') }} AS publish_date
    , {{ try_cast(regexp_extract('link', '/products/(\\d+)$'), 'BIGINT') }} AS product_id


================================================================================
PostgreSQLTransformer 설계 방향 (Phase 1 구현 시 참조)
================================================================================

배경
----
  T-1·T-2 완성 후 PostgreSQLTransformer를 추가하면 DuckDB 없이 PostgreSQL만으로
  transform이 가능해진다. 단위 테스트에서 DuckDBConnection 대신 PostgreSQL
  임시 테이블을 사용하는 시나리오를 지원한다.

  단, 현재 아키텍처에서 DuckDB는 transform 엔진으로 남아 있으며 (Q6 결정에서
  Parquet 아카이브 + 변환 엔진 역할 유지), PostgreSQLTransformer는 choice 옵션.

  class PostgreSQLTransformer(DBTransformer):
      dialect = "postgresql"

      def set_connection(self, conn=None, **kwargs):
          from linkmerce.extensions.postgresql import PostgreSQLClient
          self.__conn = conn or PostgreSQLClient(**kwargs)

      def expr_array(self, array: str = "_tmp_obj") -> str:
          return array  # 임시 테이블명 반환

      def insert_into_table(self, obj, ...):
          # 1. normalize_json() 자동 (DBTransformer에서 상속)
          # 2. _tmp_obj 임시 테이블 생성 + executemany로 obj 삽입
          # 3. SELECT ... FROM _tmp_obj 실행 → INSERT INTO target
          # 4. _tmp_obj DROP
          ...

{{ array }} 소스 확보 방식 비교
--------------------------------
  DuckDB:    UNNEST($obj) → dict의 모든 키가 자동으로 컬럼
             중첩 dict → struct로 처리 (product.identifier 가능)
             → test speed: 빠름 (in-memory)

  PostgreSQL: 두 가지 옵션:
    A) temp table approach  (위 설계, 추천)
       CREATE TEMP TABLE _tmp_obj AS VALUES (...);
       SELECT ... FROM _tmp_obj;  ← PG-compatible SQL만 사용
    B) jsonb_populate_recordset approach
       FROM jsonb_populate_recordset(NULL::schema_type, $1::jsonb) AS obj
       → DB에 composite type 사전 정의 필요, 관리 오버헤드 있음
    → A안 채택

  중첩 struct 처리:
    DuckDB: product.identifier 직접 접근
    PostgreSQL: json_keys = ["product", ...] 선언 후 임시 테이블에
                {"product": {...}, ...} 형태로 삽입 → PG에서는
                플랫닝된 컬럼 또는 JSONB 접근자 사용 필요
                → 중첩 struct는 PostgreSQL 모델에서 플랫닝 필요 (별도 모델 파일)


================================================================================
의존관계 및 영향 범위
================================================================================

  변경 대상              영향
  -------------------    -------------------------------------------------------
  utils/fill.py          신규 파일. project_paths() 구현. 의존성 없음.
  utils/dialect.py       신규 파일. 의존성 없음.
  utils/jinja.py         변경 없음.
  common/transform.py    DBTransformer + DuckDBTransformer 클래스 수정.
                         기존 서브클래스(core/*/transform.py)는 변경 없음.
                         (dialect 기본값이 "duckdb"이므로 하위 호환)
  core/*/models.sql      점진적 마이그레이션. 미마이그레이션 파일도 동작함.
                         (렌더링 시 Jinja 변수가 없으면 원본 그대로 유지)
  core/*/transform.py    validate_*()/obj[0].get() 패턴 제거 (T-2 완료 후 정리)
                         json_source/table_keys/table_prefix 선언으로 교체 (T-3)
  pyproject.toml         변경 없음.

  하위 호환성:
    - dialect 파라미터 기본값 "duckdb" → 기존 호출 코드 변경 불필요
    - normalize_json 자동 적용 → obj가 이미 정규화되어 있어도 무해
    - models.sql 미마이그레이션 파일 → Jinja 미정의 변수는 그대로 출력
      (주의: Jinja의 undefined_variable 정책 설정 필요 → silent 모드로 처리)
