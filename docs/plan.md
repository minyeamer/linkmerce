DBTransformer 개선 계획
================================================================================

본 문서는 linkmerce DBTransformer 레이어의 개선 설계 계획이다.
  - T-2: DuckDBTransformer 선언적 추상화 + fields 기반 JSON 행 정규화 (BinderException 해소)
  - EXT: PostgreSQLTransformer — extensions/postgresql.py 로 분리
  - HTML: HtmlTransformer 전용 추상화 (DuckDB 연동)

아키텍처 방향 결정
------------------
  1. 변환 엔진 고정: DuckDB를 변환(transform) 엔진으로 고정한다.
     TRY_CAST, TRY_STRPTIME, QUALIFY 등 DuckDB 전용 SQL 구문을 그대로 유지한다.
     별도 방언 추상화(T-1)는 진행하지 않는다.

  2. PostgreSQL·BigQuery는 출력 대상(sink)이다.
     Python → DuckDB(변환) → PostgreSQL / BigQuery / Parquet (출력) 파이프라인.
     PostgreSQLTransformer는 BigQueryClient처럼 extensions/postgresql.py 로 분리한다.

  3. JsonTransformer 직접 상속 제거.
     서브클래스는 DuckDBTransformer만 상속한다.
     scope / fields / on_missing 등 JSON 파싱 속성을 DuckDBTransformer에 통합하고,
     내부적으로 JsonTransformer 인스턴스를 생성·호출한다.

  4. HtmlTransformer는 별도 처리한다.
     HTML → dict 변환 결과를 DuckDBTransformer에 전달하는 연동 레이어를 추가한다.


================================================================================
전체 클래스 현황 분석 (T-2 적용 범위)
================================================================================

core/ 하위 24개 transform.py 파일의 DuckDBTransformer 서브클래스를 전수 조사하여
T-2(선언적 추상화 + fields 정규화) 적용 가능 여부를 분류한다.

패턴 정의
---------
  A  표준 단일 (json_source 설정, params 없음)
  B  단일 + params 자동 언팩 (json_source + params=kwargs)
  C  다중 테이블 표준 (json_source + table_keys + table_prefix)
  D  json_source_scope 필요 (JsonTransformer에 scope를 runtime에 전달)
  E  비표준 (직접 구현 유지 — Excel/CSV/binary 입력, 이중 소스, 커스텀 삽입)

DuckDBTransformer 현황
----------------------
  패턴  클래스                          파일                              json_source
  ----- -------------------------------- --------------------------------  -------------------
  A     Stock                           cj/eflexs/stock                   StockList
  A     Inventory                       ecount/api/inventory              InventoryList
  A     Product                         ecount/api/product                ProductList
  A     Order (sbn)                     sabangnet/admin/order             OrderList
  A     ProductMapping                  sabangnet/admin/order             ProductList
  A     Campaign (gfa)                  searchad/gfa/adreport             Content
  A     CafeTab                         naver/main/search                 CafeList
  A     CafeArticle                     naver/main/search                 CafeArticleJson

  B     SkuMapping                      sabangnet/admin/order             SkuList
  B     MarketingChannel                smartstore/api/bizdata            MarketingChannelList
  B     OrderTime                       smartstore/api/order              OrderList
  B     OrderStatus                     smartstore/api/order              OrderStatusList
  B     AdSet (gfa)                     searchad/gfa/adreport             Content
  B     Creative (gfa)                  searchad/gfa/adreport             Content
  B     RocketInventory                 coupang/wing/product              RocketInventorylist
  B     RocketOption                    coupang/wing/product              RocketInventorylist
  B     RocketSettlement                coupang/wing/settlement           RocketSettlementList
  B     _AdTransformer (google)         google/api/ads                    AdResults
  B     _AdTransformer (meta)           meta/api/ads                      AdObjects
        └ Campaign, AdGroup, Insight, Asset (google) — 부모 상속
        └ Campaigns, Adsets, Ads (meta)             — 부모 상속
  B     DailyReport (← csv, custom)    searchad/manage/adreport          AdvancedReport (B이지만 custom csv parse)

  C     Order (smartstore)              smartstore/api/order              OrderList
        table_keys=["order","product_order","delivery","option"], prefix="smartstore"

  D     _SearchTransformer + 파생 8종   naver/openapi/search              SearchItems(scope="items")
  D     BrandCatalog, BrandProduct      smartstore/brand/catalog          CatalogItems(scope="items")
  D     _PageView + 파생 3종            smartstore/brand/pageview         PageViewItems (scope 고정)
  D     _SalesTransformer + 파생 3종    smartstore/brand/sales            SalesList(scope 동적) → property 사용

  E     Ad, AssetView                   google/api/ads                    AdList/AssetViewList (커스텀 parse)
  E     Insights                        meta/api/ads                      AdObjects (prefix 비표준)
  E     Campaign (coupang ads)          coupang/advertising/adreport      이중 소스 (CampaignList+AdgroupList)
  E     Creative (coupang ads)          coupang/advertising/adreport      커스텀 list 변환 후 삽입
  E     ProductAdReport                 coupang/advertising/adreport      Excel 입력
  E     NewCustomerAdReport             coupang/advertising/adreport      Excel 입력
  E     ProductOption (coupang)         coupang/wing/product              by="option" 모드 분기
  E     ProductDetail                   coupang/wing/product              insert key 조건분기
  E     ProductDownload                 coupang/wing/product              Excel + request_type 분기
  E     RocketSettlementDownload        coupang/wing/settlement           binary 입력 + 다중시트
  E     OrderDownload                   sabangnet/admin/order             Excel + download_type 생성자
  E     OrderStatus (sbn)               sabangnet/admin/order             Excel + 동적 render
  E     Option (sabangnet)              sabangnet/admin/product           이중 소스 + 헤더 정규화
  E     OptionDownload (sabangnet)      sabangnet/admin/product           Excel 입력
  E     AddProductGroup, AddProduct     sabangnet/admin/product           obj 직접 접근
  E     Product (sbn)                   sabangnet/admin/product           커스텀 parse
  E     Option (smartstore)             smartstore/api/product            이중 소스 (simple/comb/supplement)
  E     BrandPrice                      smartstore/brand/catalog          insert_into_table 오버라이드
  E     ProductCatalog                  smartstore/brand/catalog          insert_into_table 오버라이드
  E     AggregatedSales                 smartstore/brand/sales            split_params 커스텀
  E     ShoppingRank                    naver/openapi/search              insert_into_table 오버라이드
  E     ExposureRank                    searchad/manage/exposure          reparse_object + split_params
  E     Ad (searchad/api)               searchad/api/adreport             멀티 TSV 타입
  E     Search                          naver/main/search                 HTML 파서 + 직접 SQL
  E     Keyword                         searchad/api/keyword              max_rank 슬라이스
  E     TimeContract, BrandNewContract  searchad/api/contract             obj가 이미 list
  E     _PerformanceReport 파생 2종      searchad/gfa/adreport             zip+CSV 입력

JsonTransformer 현황 (내부 사용 전환 대상)
------------------------------------------
  T-2 적용 후 JsonTransformer를 직접 상속하는 서브클래스는 없어진다.
  아래는 현재 T2-A(fields 선언)로 전환된 클래스 현황이다.
  (기존 validate_* / obj[0].get() 패치는 이미 fields 선언으로 대체 완료)

  클래스               파일                          fields 선언 상태
  -------------------- ---------------------------   --------------------------------
  OrderList            smartstore/api/order          ✓ (중첩 dict + completedClaims[0])
  Product              smartstore/api/product        ✓
  Product              sabangnet/admin/product       ✓ (fnlChgDt 포함)
  Option               sabangnet/admin/product       ✓ (fnlChgDt 포함)
  OrderStatusList      smartstore/api/order          ✓
  AdList               searchad/manage/exposure      ✓ (lowPrice, mobileLowPrice 포함)
  (기타 전체 목록은 chat.log Q11 참조)

  비표준 (직접 유지):
    OptionDownload      sabangnet/admin/product       Excel 헤더 정규화 → 직접 유지

HtmlTransformer 현황
---------------------
  모두 naver/main/search/transform.py에 위치. HTML DOM 파싱 특화.
  T-2 선언적 추상화 직접 적용 대상이 아니나, 파싱 결과를 DuckDBTransformer에
  전달하는 연동 레이어(html_source 속성)를 추가한다 (HTML 섹션 참조).

  클래스                  부모
  ----------------------  ----------------------------
  SearchSection           HtmlTransformer
  PowerLink               HtmlTransformer
  RelatedKeywords         HtmlTransformer
  _ShoppingTransformer    HtmlTransformer
  Shopping                _ShoppingTransformer
  NewShopping             Shopping
  CafeList                HtmlTransformer

T-2 적용 효과 요약
------------------
  적용 가능 (A/B/C/D):  약 30개 클래스 (transform() 직접 구현 제거)
  직접 구현 유지 (E):    약 20개 클래스 (비표준 입력/로직)
  중간 base 클래스 효과:  _AdTransformer(google), _AdTransformer(meta),
                          _SearchTransformer, _SalesTransformer, _PageView 등
                          → base 클래스에서 json_source 선언 시 파생 클래스 전체 혜택


================================================================================
현황 분석
================================================================================

구조 요약
---------
  Transformer (ABC)
  ├── JsonTransformer (ABC)    ← HTTP 응답 파싱 전담. scope/fields/select_fields.
  ├── HtmlTransformer (ABC)    ← HTML DOM 파싱 전담.
  └── DBTransformer (ABC)      ← 쿼리/테이블/모델 관리, Jinja render
      └── DuckDBTransformer    ← 유일한 구체 구현체
            .insert_into_table(obj, ...)
              → expr_values() → render SELECT query
              → conn.execute(INSERT INTO table (SELECT ...), obj=obj)

  현재 core/*/transform.py 구조:
    - JsonTransformer 서브클래스 (OrderList 등): HTTP 응답 파싱 담당
    - DuckDBTransformer 서브클래스 (Order 등): JSON 파싱 + DuckDB 삽입
    → 두 클래스가 항상 쌍으로 정의됨. DuckDB 서브클래스가 JSON 서브클래스를
      내부에서 호출하는 패턴이 반복됨.

DuckDB의 UNNEST 패턴 (expr_array):
  (SELECT obj.* FROM (SELECT UNNEST($obj) AS obj))
  → $obj 파라미터로 list[dict] 전달
  → dict의 키가 자동으로 컬럼이 됨 (스키마는 obj[0]에서 추론)

문제: BinderException
---------------------
  DuckDB는 UNNEST 시 obj[0]으로 컬럼 스키마를 결정함.
  obj[1+]에 obj[0]에 없는 키가 있거나, obj[0]에 있는 키가 이후에 없으면
  BinderException 발생.

  현재 workaround: transform.py 개별 파일에서 obj[0]["key"] = obj[0].get("key")
  패턴을 수동으로 작성. 이는:
    - 누락 시 런타임 오류 → 조회 가능 기간 초과 시 복구 불가
    - 클래스마다 파편적으로 작성되어 보장 범위가 불분명함

  → T-2에서 fields 선언을 DuckDBTransformer에 통합하여 근본 해소.

아키텍처 단순화 방향
--------------------
  AS-IS:
    class OrderList(JsonTransformer):
        scope = "data.contents"
        fields = {...}

    class Order(DuckDBTransformer):
        def transform(self, obj, **kwargs):
            orders = OrderList().transform(obj)
            if orders:
                self.insert_into_table(orders)

  TO-BE:
    class Order(DuckDBTransformer):
        scope        = "data.contents"
        fields       = {...}
        table_keys   = ["order", "product_order", "delivery", "option"]
        table_prefix = "smartstore"

    → JsonTransformer 서브클래스(OrderList) 삭제.
    → DuckDBTransformer가 내부적으로 JsonTransformer를 생성·호출.
    → 한 클래스에 파싱 스키마 + DB 삽입 로직이 함께 선언됨.


================================================================================
T-2. DuckDBTransformer 선언적 추상화 + fields 기반 JSON 행 정규화
================================================================================

설계 원칙
---------
  T-2는 기존 T-2(fields 정규화)와 T-3(선언적 추상화)를 병합한 단일 작업이다.

  핵심 결정:
    - JsonTransformer 직접 상속을 폐지한다.
    - scope / fields / on_missing / defaults 등 JSON 파싱 속성을
      DuckDBTransformer 클래스 속성으로 직접 선언한다.
    - DuckDBTransformer가 내부적으로 JsonTransformer 인스턴스를 생성·호출한다.
      (JsonTransformer 클래스 자체는 내부 유틸리티로 유지, 공개 인터페이스 아님)
    - 하위 호환: 속성 미선언 시 기존 동작(transform() 직접 구현)을 완전히 유지.

신규 클래스 속성 (DuckDBTransformer)
-------------------------------------
  JSON 파싱 속성 (JsonTransformer에서 이관):
    scope: str | None = None
      → JSON 응답에서 데이터를 추출할 dot-notation 경로. e.g., "data.contents"
      → 기존 JsonTransformer.scope 와 동일한 의미.

    fields: dict | list | None = None
      → DuckDB UNNEST 스키마 선언. nested dict 또는 list[str].
      → 선언된 키만 추출되고, 없는 키는 None으로 채워짐 → BinderException 해소.
      → 기존 JsonTransformer.fields 와 동일한 의미.

    on_missing: Literal["ignore","raise"] = "raise"
      → scope 경로 탐색 실패 시 동작. "raise"이면 ParseError, "ignore"이면 None 반환.

    defaults: dict | None = None
      → 파싱 후 각 행에 추가할 기본 키-값. $kwarg 형식으로 런타임 값 주입 가능.

  DB 삽입 선언 속성:
    table_keys: list[str] | None = None
      → 다중 테이블 모드. 각 키는 테이블명 접미사. None이면 단일 테이블 모드.

    table_prefix: str | None = None
      → 테이블명 프리픽스. table_keys와 조합하여 실제 테이블명 생성.
      → e.g., table_prefix="smartstore", key="order" → "smartstore_order"

    json_source_scope: str | None = None
      → scope를 런타임에 외부에서 주입해야 할 때 사용.
        (기존 json_source_path를 scope 기반으로 재명명)
      → None이면 클래스 선언 scope 사용.
      → 동적 scope가 필요한 경우 _parse_source() 메서드 오버라이드로 처리.

T-2-1. _parse_source() 내부 구현
----------------------------------
  DuckDBTransformer에 다음 메서드를 추가한다.

    def _parse_source(self, obj: JsonObject, **kwargs) -> list | None:
        """
        scope/fields 선언 기반으로 JSON obj를 파싱한다.
        scope 또는 fields가 선언된 경우 내부적으로 JsonTransformer를 생성·호출.
        둘 다 None이면 None 반환 (transform() 직접 구현 모드).
        """
        if self.scope is None and self.fields is None:
            return None
        scope = self.json_source_scope or self.scope
        from linkmerce.common.transform import JsonTransformer

        class _InlineSource(JsonTransformer):
            pass

        _InlineSource.scope      = scope
        _InlineSource.fields     = self.fields
        _InlineSource.on_missing = self.on_missing
        _InlineSource.defaults   = self.defaults
        return _InlineSource().transform(obj, **kwargs)

  → _SalesTransformer처럼 동적 scope가 필요한 경우:
      def _parse_source(self, obj, **kwargs):
          scope = f"data.{self.sales_type}Sales"
          # scope를 직접 지정하여 JsonTransformer 호출

T-2-2. queries 자동 계산
--------------------------
  __init_subclass__ 또는 __init__ 시점에 table_keys 기반으로 자동 계산.

  table_keys 설정 시:
    [f"{kw}_{key}" for key in table_keys for kw in ["create", "select", "insert"]]

  scope 또는 fields만 설정 시 (단일 테이블):
    ["create", "select", "insert"]  (기본값 그대로)

  둘 다 None (기존 방식): 서브클래스에서 queries 직접 선언 유지.

T-2-3. set_tables() 자동화
---------------------------
  table_keys + table_prefix 설정 시:
    base = {key: f"{table_prefix}_{key}" for key in table_keys}
    super().set_tables(dict(base, **(external_tables or {})))

  → set_tables() 직접 구현 불필요.

T-2-4. create_table() 자동화
------------------------------
  table_keys 설정 시:
    for key in table_keys:
        super().create_table(key=f"create_{key}", table=f":{key}:")

  → create_table() 직접 구현 불필요.

T-2-5. transform() 기본 구현
------------------------------
  scope 또는 fields가 선언된 경우:
    def transform(self, obj, **kwargs):
        data = self._parse_source(obj, **kwargs)
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

  scope/fields 모두 None이면 transform()은 추상 메서드로 남아
  서브클래스에서 직접 구현해야 한다.

T-2-6. fields 동작 (BinderException 해소)
-------------------------------------------
  DuckDB UNNEST 패턴: obj[0]의 키 집합으로 컬럼 스키마 결정.
  fields 선언 시 모든 행에 동일한 키 구조를 보장 → BinderException 근본 해소.

  fields 형식:
    list[str]           → 평탄한 스키마 (각 키 값 그대로, 없으면 None)
    {key: None}         → key 값을 그대로 포함 (없으면 None)
    {key: list[str]}    → key 하위 dict에서 목록의 키만 추출
    {key: dict}         → key 하위 dict에 대해 재귀 적용
    {"key[N]": spec}    → key 리스트의 N번째 인덱스 접근

  동작 예시:
    fields = {"key1": {"key2": ["a", "b"]}, "key3": None}
    입력: {}
    출력: {"key1": {"key2": {"a": None, "b": None}}, "key3": None}

    fields = {"key1": {"key2": ["a", "b"]}, "key3": None}
    입력: {"key1": {"key2": {"a": 1, "x": 99}}, "key3": "y", "unrelated": "z"}
    출력: {"key1": {"key2": {"a": 1, "b": None}}, "key3": "y"}  ← x, unrelated 제거

  이 동작은 현재 JsonTransformer.select_fields() → utils/nested.select_values()
  에서 이미 구현되어 있다. DuckDBTransformer는 내부적으로 이를 그대로 활용한다.

T-2-7. Before / After 비교
----------------------------
  패턴 C — Order (다중 테이블):

    AS-IS:
      class OrderList(JsonTransformer):
          dtype  = dict
          scope  = "data.contents"
          fields = { "productOrderId": None, "content": {...}, ... }

      ORDER_TABLES = ["order", "product_order", "delivery", "option"]

      class Order(DuckDBTransformer):
          queries = [f"{kw}_{t}" for t in ORDER_TABLES for kw in ["create","select","insert"]]

          def set_tables(self, tables=None):
              base = {t: f"smartstore_{t}" for t in ORDER_TABLES}
              super().set_tables(dict(base, **(tables or {})))

          def create_table(self, **kwargs):
              for t in ORDER_TABLES:
                  super().create_table(key=f"create_{t}", table=f":{t}:")

          def transform(self, obj, **kwargs):
              orders = OrderList().transform(obj)
              if orders:
                  for t in ORDER_TABLES:
                      self.insert_into_table(orders,
                          key=f"insert_{t}", table=f":{t}:", values=f":select_{t}:")

    TO-BE:
      class Order(DuckDBTransformer):
          scope        = "data.contents"
          fields       = { "productOrderId": None, "content": {...}, ... }
          table_keys   = ["order", "product_order", "delivery", "option"]
          table_prefix = "smartstore"

    → OrderList 클래스 삭제. 30줄 이상 → 5줄.

  패턴 B — OrderTime (단일 테이블 + params):

    AS-IS:
      class OrderTime(DuckDBTransformer):
          queries = ["create", "select", "insert"]

          def transform(self, obj, channel_seq=None, **kwargs):
              orders = OrderList().transform(obj)
              if orders:
                  self.insert_into_table(orders, params=dict(channel_seq=channel_seq))

    TO-BE:
      class OrderTime(DuckDBTransformer):
          scope  = "data.contents"
          fields = { ... }  # OrderList와 동일한 fields 선언

    → channel_seq는 **kwargs로 전달 → params=kwargs 자동 언팩.
    → queries 선언 불필요.

    단, scope/fields가 Order와 동일하다면 내부 _InlineSource를 공유하거나
    Order의 scope/fields를 클래스 변수로 참조해도 됨.

  패턴 A — Stock (단순 단일):

    AS-IS:
      class StockList(JsonTransformer):
          dtype     = dict
          scope     = "dsRealTime"
          fields    = ["itemCd", "itemVarcode", ..., "inbDate"]
          on_missing = "raise"

      class Stock(DuckDBTransformer):
          queries = ["create", "select", "insert"]

          def transform(self, obj, **kwargs):
              items = StockList().transform(obj)
              if items:
                  return self.insert_into_table(items)

    TO-BE:
      class Stock(DuckDBTransformer):
          scope      = "dsRealTime"
          fields     = ["itemCd", "itemVarcode", ..., "inbDate"]
          on_missing = "raise"

    → StockList 클래스 삭제. 2줄로 단순화.

  패턴 D — _SearchTransformer (동적 scope):

    AS-IS:
      class _SearchTransformer(DuckDBTransformer):
          queries = ["create", "select", "insert"]

          def transform(self, obj, query, start=1, **kwargs):
              items = SearchItems(path=["items"]).transform(obj)
              if items:
                  self.insert_into_table(items, params=dict(keyword=query, start=start-1))

    TO-BE:
      class _SearchTransformer(DuckDBTransformer):
          fields = [...]  # SearchItems의 fields

          def _parse_source(self, obj, **kwargs):
              from linkmerce.common.transform import JsonTransformer
              class _Src(JsonTransformer):
                  scope = "items"
                  fields = _SearchTransformer.fields
              return _Src().transform(obj)

          def transform(self, obj, query, start=1, **kwargs):
              items = self._parse_source(obj)
              if items:
                  self.insert_into_table(items, params=dict(keyword=query, start=start-1))

    → queries/set_tables/create_table은 자동, transform() params 변환 부분만 유지.

  패턴 E (비표준) — 직접 구현 유지:
    - coupang/advertising: Campaign(이중 소스), ProductAdReport(Excel)
    - coupang/wing: ProductOption(by= 분기), ProductDownload(Excel), RocketSettlementDownload(binary)
    - sabangnet/admin: OrderDownload(Excel), OrderStatus(Excel), Option/OptionDownload
    - sabangnet/admin: AddProductGroup, AddProduct
    - smartstore/api: Option(이중 소스)
    - smartstore/brand: BrandPrice, ProductCatalog, AggregatedSales
    - naver/openapi: ShoppingRank
    - searchad/manage: ExposureRank
    - searchad/api: Ad(멀티 TSV), Keyword(max_rank), TimeContract, BrandNewContract
    - searchad/gfa: _PerformanceReport
    - naver/main: Search (HTML + 직접 SQL)

T-2-8. fields 선언 현황 (완료된 JsonTransformer 서브클래스)
------------------------------------------------------------
  scope/fields 선언이 이미 완료된 클래스 목록. T-2 적용 시
  해당 클래스를 삭제하고 DuckDBTransformer로 속성을 이관한다.

  클래스                파일                          scope / fields
  -------------------- ---------------------------   ---------------------------------
  StockList            cj/eflexs/stock               scope="dsRealTime", 16 fields
  InventoryList        ecount/api/inventory          scope="Data.Result", 2 fields
  ProductList          ecount/api/product            scope="Data.Result", 10 fields
  AdResults            google/api/ads                scope="0.results", 8 fields
  AdList               google/api/ads                scope="0.results", 5 fields
  AssetViewList        google/api/ads                scope="0.results", 6 fields
  AdObjects            meta/api/ads                  scope="data", 19 fields
  OrderList (sbn)      sabangnet/admin/order         scope="data.orderList", 29 fields
  ProductList (sbn)    sabangnet/admin/order         scope="data.list", 7 fields
  SkuList              sabangnet/admin/order         scope="data", 8 fields
  ProductList          sabangnet/admin/product       scope="data.list"
  OptionList           sabangnet/admin/product       scope="data.optionList"
  KeywordList          searchad/api/keyword          scope="keywordList", 7 fields
  AdList               searchad/manage/exposure      scope="adList"
  Content              searchad/gfa/adreport         scope="content", 12 fields
  MarketingChannelList smartstore/api/bizdata        scope="rows", 10 fields
  OrderList            smartstore/api/order          scope="data.contents", 중첩 dict
  OrderStatusList      smartstore/api/order          scope="data.lastChangeStatuses"
  ProductList          smartstore/api/product        scope="contents", 20 fields
  OptionList           smartstore/api/product        scope="originProduct.detailAttribute.optionInfo"
  SupplementList       smartstore/api/product        scope="...supplementProducts"
  CatalogItems         smartstore/brand/catalog      scope="data.contents", 30 fields
  PageViewItems        smartstore/brand/pageview     scope="data.storePageView.items"
  SalesList            smartstore/brand/sales        scope="data.contents"
  SearchItems          naver/openapi/search          scope="items", 22 fields
  ProductList          coupang/wing/product          scope="data.productList", 19 fields
  OptionList           coupang/wing/product          scope="data", 10 fields
  RocketInventorylist  coupang/wing/product          scope="viProperties", 9 fields
  RocketSettlementList coupang/wing/settlement       scope="settlementStatusReports", 6 fields
  CampaignList         coupang/advertising/adreport  scope="campaigns", 12 fields
  AdgroupList          coupang/advertising/adreport  scope="campaigns", 8 fields
  CreativeList         coupang/advertising/adreport  scope="adGroup.videoAds"
  CafeArticleJson      naver/main/search             scope="result"


================================================================================
HTML. HtmlTransformer → DuckDB 연동
================================================================================

배경
----
  현재 naver/main/search/transform.py에는 HtmlTransformer와 DuckDBTransformer가
  혼재한다. Search 클래스는 HtmlTransformer 파싱 결과를 직접 SQL로 삽입하는
  비표준 패턴을 사용한다. CafeTab, CafeArticle은 DuckDBTransformer이며
  CafeList(HtmlTransformer) 파싱 결과를 내부에서 호출한다.

  이 패턴을 T-2와 일관되게 만들기 위한 연동 레이어를 설계한다.

html_source 속성 (DuckDBTransformer)
--------------------------------------
  html_source: type[HtmlTransformer] | None = None
    → transform()에서 obj(BeautifulSoup)를 파싱할 HtmlTransformer 클래스.
    → DuckDBTransformer가 HTML을 직접 입력받는 경우에 사용.
    → None이면 적용하지 않음 (기존 방식 유지).

  html_source_selector: str | None = None
    → html_source 인스턴스화 시 selector= 인자로 전달.

동작
----
  html_source 선언 시 _parse_source()가 다음과 같이 동작:
    def _parse_source(self, obj, **kwargs):
        return self.html_source(selector=self.html_source_selector).transform(obj)

  → 이후 table_keys/insert_into_table 흐름은 JSON과 동일.

Before/After (CafeTab):

  AS-IS:
    class CafeList(HtmlTransformer):
        selector = "ul.keyword_list > li"
        mapping = {"keyword": "a", "count": "em"}

    class CafeTab(DuckDBTransformer):
        queries = ["create", "select", "insert"]

        def transform(self, obj, **kwargs):
            items = CafeList().transform(obj)
            if items:
                return self.insert_into_table(items)

  TO-BE:
    class CafeTab(DuckDBTransformer):
        html_source          = CafeList
        html_source_selector = "ul.keyword_list > li"

  → CafeList 삭제 여부는 선택적. selector/mapping이 간단한 경우 DuckDBTransformer에
    html_source_selector + mapping 속성으로 통합 가능.

비표준 유지 (E 패턴):
  Search: HtmlTransformer 파싱 + 직접 SQL 실행 → 직접 구현 유지.


================================================================================
EXT. extensions/postgresql.py — PostgreSQLClient
================================================================================

배경
----
  Python → DuckDB(변환) → PostgreSQL(출력) 파이프라인.
  PostgreSQL은 출력 대상(sink)이므로 BigQueryClient와 동일한 방식으로
  extensions/postgresql.py 에 분리한다.
  DuckDB 변환 결과(list[dict] 또는 Parquet)를 PostgreSQL로 적재하는
  클라이언트 레이어만 구현하면 된다.

설계 방향
---------
  class PostgreSQLClient(Connection):
      """psycopg2 또는 psycopg3 기반 PostgreSQL 연결 클라이언트."""

      def __init__(self, host, port, dbname, user, password, **kwargs):
          ...

      def get_connection(self) -> psycopg2.connection: ...
      def set_connection(self, **kwargs): ...
      def close(self): ...

      def execute(self, query: str, **params): ...
      def fetch_all_to_json(self, query: str) -> list[dict]: ...
      def fetch_all_to_csv(self, query: str) -> list[tuple]: ...

      ############################ Load Table ###########################

      def load_table_from_json(
              self,
              table: str,
              values: list[dict],
              write: Literal["append","truncate"] = "append",
          ) -> None:
          """executemany로 list[dict]를 테이블에 삽입한다."""
          ...

      def load_table_from_parquet(
              self,
              table: str,
              data: bytes,
              write: Literal["append","truncate"] = "append",
          ) -> None:
          """DuckDB로 Parquet을 읽어 PostgreSQL에 COPY한다."""
          ...

      def copy_table_from_duckdb(
              self,
              source_query: str,
              target_table: str,
              duckdb_conn: DuckDBConnection,
          ) -> None:
          """DuckDB 쿼리 결과를 PostgreSQL로 직접 복사한다."""
          ...

DuckDB → PostgreSQL 연동 패턴
------------------------------
  # 방법 1: DuckDB 결과를 list[dict]로 받아 PostgreSQL에 삽입
  with Stock(db_info=...) as transformer:
      transformer.transform(response)
      rows = transformer.fetch_all_to_json()

  with PostgreSQLClient(...) as pg:
      pg.load_table_from_json("stock", rows)

  # 방법 2: DuckDB 결과를 Parquet으로 내보내고 PostgreSQL에 적재
  parquet = transformer.fetch_all_to_parquet()
  pg.load_table_from_parquet("stock", parquet)

  중첩 struct (product.identifier 등)은 PostgreSQL 모델에서 플랫닝된
  별도 스키마를 사용한다. DuckDB 모델(models.sql)은 그대로 유지.


================================================================================
구현 단계
================================================================================

T-2 구현 (우선순위 1 — BinderException 해소 + 선언적 추상화)

  T-2-0. common/transform.py — DuckDBTransformer 클래스 속성 추가
          scope, fields, on_missing, defaults = None / "raise" / None (기본값)
          table_keys, table_prefix, json_source_scope = None (기본값)
          html_source, html_source_selector = None (기본값)

  T-2-1. common/transform.py — _parse_source() 구현
          scope/fields 선언 시 내부적으로 JsonTransformer 인스턴스 생성·호출
          html_source 선언 시 HtmlTransformer 인스턴스 생성·호출
          scope/fields/html_source 모두 None이면 None 반환

  T-2-2. common/transform.py — queries 자동 계산
          __init_subclass__ 시점에 table_keys 기반 자동 설정
          서브클래스에서 queries 직접 선언한 경우 우선

  T-2-3. common/transform.py — set_tables() 기본 구현 추가
          table_keys + table_prefix 설정 시 자동 테이블 등록

  T-2-4. common/transform.py — create_table() 기본 구현 추가
          table_keys 설정 시 자동 루프 실행

  T-2-5. common/transform.py — transform() 기본 구현 추가
          scope 또는 fields 설정 시 _parse_source() → insert_into_table() 자동 분기
          scope/fields/html_source 모두 None이면 transform()은 추상으로 유지

  T-2-6. core/*/transform.py — 패턴 A/B/C 적용
          단계: Stock(A), Inventory(A), Product(ecount/A), Order(sbn/A),
                ProductMapping(A), Campaign(gfa/A), CafeTab(A), CafeArticle(A),
                SkuMapping(B), MarketingChannel(B), OrderTime(B), OrderStatus(B),
                AdSet(gfa/B), Creative(gfa/B), RocketInventory(B), RocketOption(B),
                RocketSettlement(B), _AdTransformer(google/B), _AdTransformer(meta/B),
                Order(smartstore/C)
          → 각 파일에서 JsonTransformer 서브클래스 삭제,
            DuckDBTransformer에 scope/fields 이관

  T-2-7. core/*/transform.py — 패턴 D 적용
          _SearchTransformer, BrandCatalog, BrandProduct, _PageView, _SalesTransformer
          → _parse_source() 오버라이드로 동적 scope 처리

  T-2-8. core/naver/main/search/transform.py — HTML 연동
          CafeTab, CafeArticle에 html_source 속성 적용

---

HTML 구현 (우선순위 2 — HtmlTransformer 연동 완성)

  HTML-1. common/transform.py — html_source 속성 추가 (T-2-0에서 함께 진행)
  HTML-2. naver/main/search/transform.py — CafeTab, CafeArticle 적용
  HTML-3. Search 클래스는 비표준(직접 구현) 유지

---

EXT 구현 (우선순위 3 — PostgreSQL 출력 지원)

  EXT-1. extensions/postgresql.py 신설
          PostgreSQLClient 클래스 구현
          psycopg2 의존성 추가 (pyproject.toml)

  EXT-2. load_table_from_json() 구현
          executemany 기반 배치 삽입

  EXT-3. load_table_from_parquet() 구현
          DuckDB Parquet 출력 → PostgreSQL COPY

  EXT-4. copy_table_from_duckdb() 구현
          DuckDB 쿼리 결과를 PostgreSQL로 직접 전송


================================================================================
의존관계 및 영향 범위
================================================================================

  변경 대상                영향
  -----------------------  -------------------------------------------------------
  common/transform.py      DuckDBTransformer에 신규 속성 및 메서드 추가.
                           기존 서브클래스는 속성 미선언 시 동작 불변 (하위 호환 완전 보장).
  core/*/transform.py      JsonTransformer 서브클래스 삭제 + DuckDBTransformer에 속성 이관.
                           패턴 E(비표준) 클래스는 변경 없음.
  extensions/postgresql.py 신규 파일. BigQueryClient와 동일한 인터페이스.
  pyproject.toml           psycopg2 의존성 추가 (EXT 구현 시).
  utils/                   변경 없음 (기존 nested.select_values 등 그대로 활용).
  core/*/models.sql        변경 없음. DuckDB 전용 SQL 구문 그대로 유지.

  하위 호환성:
    - scope/fields/table_keys 기본값 None → 미선언 시 기존 transform() 구현 유지
    - JsonTransformer는 내부 유틸리티로 유지 (공개 인터페이스로 사용 불필요)
    - HtmlTransformer 변경 없음
