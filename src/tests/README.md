# linkmerce 테스트 안내

해당 테스트는 `linkmerce` 패키지의 `Extractor`와 `DuckDBTransformer`의 동작을 검증하는 테스트 모음이다.

아래 3가지 테스트를 실행할 수 있다.
1. `test_extract.py` - `Extractor.extract()` 결과를 저장한다.
2. `test_transform.py` - `DuckDBTransformer.transform()` 결과를 저장한다.
3. `test_load.py` - DuckDB 소스 테이블을 BigQuery/PostgreSQL 타겟 테이블에 적재한다.

## 테스트 실행

1. `pytest src/tests/test_extract.py -m extract -v -s`
2. `pytest src/tests/test_transform.py -m transform -v -s`
3. `pytest src/tests/test_load.py -m load -v -s`

특정 도메인만 실행할 때는 마커를 조합한다.
> 예시: `pytest src/tests/test_transform.py -m "transform and smartstore_api" -v -s`

적재 테스트는 백엔드별 마커로 분리할 수 있다.
1. `pytest src/tests/test_load.py -m "load and load_bigquery" -v -s`
2. `pytest src/tests/test_load.py -m "load and load_postgres" -v -s`

⚠️ `test_transform.py` 테스트를 실행하기 위해선 `test_extract.py` 실행 결과가 먼저 저장되어야 한다.

⚠️ `test_load.py` 테스트는 Extract/Transform 결과에 의존하지 않는 독립적인 테스트다.

## Extract/Transform 테스트

모든 테스트 실행 결과는 `src/tests/results/` 경로 아래에 `linkmerce.core` 모듈명 및 클래스명 경로로 조합한 위치에 저장한다.
1. `test_extract.py` 테스트 실행 시 `{module}/{Extractor}/` 하위 경로를 생성한다.   
    테스트 실행 결과는 해당 하위 경로에 `extract.{ext}` 파일로 저장한다.
2. `test_transform.py` 테스트 실행 시 상위 `{module}/{Extractor}/{DuckDBTransformer}` 하위 경로를 생성한다.   
    테스트는 변환 기능을 수행하는 `parse()`와 테이블 적재 기능을 수행하는 `bulk_insert()` 두 부분으로 나눠진다.
    - `parse()` 실행 결과는 하위 `ResponseTransformer` 클래스명을 하위 경로로 생성하며,   
        그 아래에 `/transform.{ext}` 파일로 변환 결과를 저장한다.
    - `bulk_insert()` 실행 후 모든 테이블에 대한 `SELECT *` 조회 결과를 `{table_key}.{ext}` 파일로 저장한다.

`test_extract.py` 테스트는 코드 내에 HTTP 응답 결과에 대한 확장자를 직접 지정하고,   
`test_transform.py` 테스트는 항상 JSON 형식으로 변환 및 테이블 조회 결과를 저장한다.

다음과 같은 확장자를 지원하며, 저장 시 옵션을 같이 안내한다.
1. `.json` - `indent=2, ensure_ascii=False, default=str`
2. `.html` - `BeautifulSoup` 객체의 `prettify()` 메서드로 문자열 변환
3. `.xlsx` - `bytes` 객체를 `mode="wb"` 옵션으로 파일 쓰기
4. `.csv`, `.tsv`, `.txt` - 문자열을 `mode="w", encoding="utf-8"` 모드로 파일 쓰기
5. 그 외엔 확장자 없이 `mode="wb"` 옵션으로 저장한다.

💡 하나의 테스트가 여러 개의 실행 결과를 저장해야 하는 경우 `extract_{index}.{ext}` 형식으로 구분한다.

## Load 테스트

적재 테스트는 [소스 데이터, 소스 테이블, 타겟 테이블]에 대한 연결 정보를 설정에서 읽어 다음 흐름을 검증한다.
1. 소스 데이터 파일을 DuckDB의 소스 테이블로 적재한다.
2. 원본 테이블 스키마를 기준으로 타겟 테이블 또는 타겟 임시 테이블을 생성한다.
3. `*_table_from_duckdb` 메서드 실행 시나리오를 실행한다.
4. 개별 테스트가 끝나면 생성한 테이블을 정리하고, 전체 테스트가 끝나면 모든 DB 연결을 닫는다.

## 패키지 구조

### 표기 규칙

- `/` 로 끝나는 노드는 모듈 디렉터리
- `─x` 로 표시된 노드는 하위 노드를 포함해 테스트에서 제외할 클래스 또는 보조 모듈
- 모듈이 아닌 노드는 `클래스(부모클래스)::모듈` 형식으로 표기
- 트리는 `Extractor -> ResponseTransformer -> DuckDBTransformer` 연결을 토대로 확장된다.
- `DuckDBTransformer`가 기본형 `ResponseTransformer`를 직접 파서로 사용할 때는 `>>` 로 축약 표기한다.
    1. `json`: `JsonTransformer`
    2. `html`: `HtmlTransformer`
    3. `excel`: `ExcelTransformer`

### CJ

```bash
cj/
└── eflexs/
    ├─x CjEflexs(Extractor)::common
    └── stock/
        └── Stock(CjEflexs)::extract
            └── Stock(DuckDBTransformer)::transform >> json
```

### Coupang

```bash
coupang/
├── advertising/
│   ├─x CoupangAds(Extractor)::common
│   ├─x CoupangLogin(LoginHandler)::common
│   └── adreport/
│       ├── Campaign(CoupangAds)::extract
│       │   └── Campaign(DuckDBTransformer)::transform
│       │       ├── CampaignParser(JsonTransformer)::transform
│       │       └── AdgroupParser(JsonTransformer)::transform
│       ├── Creative(CoupangAds)::extract
│       │   └── Creative(DuckDBTransformer)::transform >> json
│       ├─x _AdReport(CoupangAds)::extract
│       ├── ProductAdReport(_AdReport)::extract
│       │       └── ProductAdReport(DuckDBTransformer)::transform >> excel
│       └── NewCustomerAdReport(_AdReport)::extract
│               └── NewCustomerAdReport(DuckDBTransformer)::transform >> excel
└── wing/
    ├─x CoupangWing(Extractor)::common
    ├─x CoupangSupplierHub(CoupangWing)::common
    ├─x CoupangLogin(LoginHandler)::common
    ├── product/
    │   ├── ProductOption(CoupangWing)::extract
    │   │   └── ProductOption(DuckDBTransformer)::transform
    │   │       └── ProductOptionParser(JsonTransformer)::transform
    │   ├── ProductDetail(CoupangWing)::extract
    │   │   └── ProductDetail(DuckDBTransformer)::transform >> json
    │   ├── ProductDownload(ProductOption)::extract
    │   │   └── ProductDownload(DuckDBTransformer)::transform
    │   │       └── VendorInventoryItemParser(ExcelTransformer)::transform
    │   └── RocketInventory(CoupangWing)::extract
    │       ├── RocketInventory(DuckDBTransformer)::transform >> json
    │       └── RocketOption(DuckDBTransformer)::transform >> json
    └── settlement/
        ├─x Summary(CoupangWing)::extract
        ├── RocketSettlement(CoupangWing)::extract
        │   └── RocketSettlement(DuckDBTransformer)::transform >> json
        └── RocketSettlementDownload(RocketSettlement)::extract
            └── RocketSettlementDownload(DuckDBTransformer)::transform
                ├── RocketSalesParser(ExcelTransformer)::transform
                └── RocketShippingParser(ExcelTransformer)::transform
```

### Ecount

```bash
ecount/
└── api/
    ├─x EcountApi(Extractor)::common
    ├─x EcountRequestApi(EcountApi)::common
    ├─x EcountTestApi(EcountApi)::common
    ├── inventory/
    │   └── Inventory(EcountApi)::extract
    │       └── Inventory(DuckDBTransformer)::transform >> json
    └── product/
        └── Product(EcountApi)::extract
            └── Product(DuckDBTransformer)::transform >> json
```

### Google

```bash
google/
└── api/
    ├─x GoogleApi(Extractor)::common
    └── ads/
        ├─x GoogleAds(GoogleApi)::extract
        │   └─x _CommonParser(JsonTransformer)::transform
        ├── Campaign(GoogleAds)::extract
        │   └── Campaign(DuckDBTransformer)::transform >> json
        ├── AdGroup(GoogleAds)::extract
        │   └── AdGroup(DuckDBTransformer)::transform >> json
        ├── Ad(GoogleAds)::extract
        │   └── Ad(DuckDBTransformer)::transform
        │       └── AdParser(_CommonParser)::transform
        ├── Insight(GoogleAds)::extract
        │   └── Insight(DuckDBTransformer)::transform >> json
        ├── Asset(GoogleAds)::extract
        │   └── Asset(DuckDBTransformer)::transform
        │       └── AssetParser(_CommonParser)::transform
        └── AssetView(Insight)::extract
            └── AssetView(DuckDBTransformer)::transform
                └── AssetViewParser(_CommonParser)::transform
```

### Meta

```bash
meta/
└── api/
    ├─x MetaApi(Extractor)::common
    └── ads/
        ├─x MetaAds(MetaApi)::extract
        ├─x _AdObjects(MetaAds)::extract
        ├── Campaigns(_AdObjects)::extract
        │   └── Campaigns(DuckDBTransformer)::transform >> json
        ├── Adsets(_AdObjects)::extract
        │   └── Adsets(DuckDBTransformer)::transform >> json
        ├── Ads(_AdObjects)::extract
        │   └── Ads(DuckDBTransformer)::transform >> json
        └── Insights(MetaAds)::extract
            └── Insights(DuckDBTransformer)::transform >> json
```

### Naver

```bash
naver/
├── main/
│   └── search/
│       ├── Search(Extractor)::extract
│       │   └── Search(DuckDBTransformer)::transform
│       │       └── SearchSectionParser(HtmlTransformer)::transform
│       │           ├─x PowerLink(HtmlTransformer)::transform
│       │           ├─x RelatedKeywords(HtmlTransformer)::transform
│       │           ├─x Shopping(HtmlTransformer)::transform
│       │           ├─x NewShopping(Shopping)::transform
│       │           ├─x _PropsTransformer(JsonTransformer)::transform
│       │           ├─x _ContentsPropsTransformer(_PropsTransformer)::transform
│       │           ├─x IntentBlock(_ContentsPropsTransformer)::transform
│       │           ├─x Web(_ContentsPropsTransformer)::transform
│       │           ├─x Review(Web)::transform
│       │           ├─x Image(_PropsTransformer)::transform
│       │           ├─x Video(_PropsTransformer)::transform
│       │           ├─x RelatedQuery(_PropsTransformer)::transform
│       │           └─x AiBriefing(_PropsTransformer)::transform
│       ├── SearchTab(Extractor)::extract
│       │   └── CafeTab(DuckDBTransformer)::transform
│       │       └── CafeParser(HtmlTransformer)::transform
│       └── CafeArticle(Extractor)::extract
│           └── CafeArticle(DuckDBTransformer)::transform
│               └── CafeArticleParser(JsonTransformer)::transform
└── openapi/
    ├─x NaverOpenApi(Extractor)::common
    └── search/
        ├─x _SearchExtractor(NaverOpenApi)::extract
        ├── BlogSearch(_SearchExtractor)::extract
        │   └── BlogSearch(DuckDBTransformer)::transform
        │       └── SearchParser(JsonTransformer)::transform
        ├── NewsSearch(_SearchExtractor)::extract
        │   └── NewsSearch(DuckDBTransformer)::transform
        │       └── SearchParser(JsonTransformer)::transform
        ├── BookSearch(_SearchExtractor)::extract
        │   └── BookSearch(DuckDBTransformer)::transform
        │       └── SearchParser(JsonTransformer)::transform
        ├── CafeSearch(_SearchExtractor)::extract
        │   └── CafeSearch(DuckDBTransformer)::transform
        │       └── SearchParser(JsonTransformer)::transform
        ├── KiNSearch(_SearchExtractor)::extract
        │   └── KiNSearch(DuckDBTransformer)::transform
        │       └── SearchParser(JsonTransformer)::transform
        ├── ImageSearch(_SearchExtractor)::extract
        │   └── ImageSearch(DuckDBTransformer)::transform
        │       └── SearchParser(JsonTransformer)::transform
        └── ShopSearch(_SearchExtractor)::extract
            ├── ShopSearch(DuckDBTransformer)::transform
            │   └── SearchParser(JsonTransformer)::transform
            └── ShopRank(ShopSearch)::transform
                └── SearchParser(JsonTransformer)::transform
```

### SabangNet

```bash
sabangnet/
└── admin/
    ├─x SabangnetAdmin(Extractor)::common
    ├─x SabangnetLogin(LoginHandler)::common
    ├── order/
    │   ├── Order(SabangnetAdmin)::extract
    │   │   └── Order(DuckDBTransformer)::transform >> json
    │   ├── OrderDownload(Order)::extract
    │   │   └── OrderDownload(DuckDBTransformer)::transform >> excel
    │   ├── OrderStatus(OrderDownload)::extract
    │   │   └── OrderStatus(DuckDBTransformer)::transform >> excel
    │   ├── ProductMapping(SabangnetAdmin)::extract
    │   │   └── ProductMapping(DuckDBTransformer)::transform >> json
    │   └── SkuMapping(SabangnetAdmin)::extract
    │       └── SkuMapping(DuckDBTransformer)::transform >> json
    └── product/
        ├── Product(SabangnetAdmin)::extract
        │   └── Product(DuckDBTransformer)::transform >> json
        ├── Option(SabangnetAdmin)::extract
        │   └── Option(DuckDBTransformer)::transform >> json
        ├── OptionDownload(SabangnetAdmin)::extract
        │   └── OptionDownload(DuckDBTransformer)::transform
        │       └── OptionParser(ExcelTransformer)::transform
        ├── AddProductGroup(SabangnetAdmin)::extract
        │   └── AddProductGroup(DuckDBTransformer)::transform >> json
        └── AddProduct(SabangnetAdmin)::extract
            └── AddProduct(DuckDBTransformer)::transform >> json
```

### SearchAd

```bash
searchad/
├── api/
│   ├─x NaverSearchAdApi(Extractor)::common
│   ├── adreport/
│   │   ├─x _ReportsDownload(NaverSearchAdApi)::extract
│   │   ├─x _MasterReport(_ReportsDownload)::extract
│   │   ├── Campaign(_MasterReport)::extract
│   │   │   └── Campaign(DuckDBTransformer)::transform
│   │   │       └── TsvTransformer(ResponseTransformer)::transform
│   │   ├── Adgroup(_MasterReport)::extract
│   │   │   └── Adgroup(DuckDBTransformer)::transform
│   │   │       └── TsvTransformer(ResponseTransformer)::transform
│   │   ├─x Ad(_MasterReport)::extract
│   │   ├─x ContentsAd(_MasterReport)::extract
│   │   ├─x ShoppingProduct(_MasterReport)::extract
│   │   ├─x ProductGroup(_MasterReport)::extract
│   │   ├─x ProductGroupRel(_MasterReport)::extract
│   │   ├─x BrandAd(_MasterReport)::extract
│   │   ├─x BrandThumbnailAd(_MasterReport)::extract
│   │   ├─x BrandBannerAd(_MasterReport)::extract
│   │   ├── MasterAd(_MasterReport)::extract
│   │   │   └── MasterAd(DuckDBTransformer)::transform
│   │   │       ├── Ad(TsvTransformer)::transform
│   │   │       ├── ContentsAd(TsvTransformer)::transform
│   │   │       ├── ShoppingProduct(TsvTransformer)::transform
│   │   │       ├── ProductGroup(TsvTransformer)::transform
│   │   │       ├── ProductGroupRel(TsvTransformer)::transform
│   │   │       ├── BrandAd(TsvTransformer)::transform
│   │   │       ├── BrandThumbnailAd(TsvTransformer)::transform
│   │   │       └── BrandBannerAd(TsvTransformer)::transform
│   │   ├── Media(_MasterReport)::extract
│   │   │   └── Media(DuckDBTransformer)::transform
│   │   │       └── TsvTransformer(ResponseTransformer)::transform
│   │   ├─x _StatReport(_ReportsDownload)::extract
│   │   ├─x AdStat(_StatReport)::extract
│   │   ├─x AdConversion(_StatReport)::extract
│   │   └── AdvancedReport(_StatReport)::extract
│   │       └── AdvancedReport(DuckDBTransformer)::transform
│   │           ├── AdStat(TsvTransformer)::transform
│   │           └── AdConversion(TsvTransformer)::transform
│   ├── contract/
│   │   ├── TimeContract(NaverSearchAdApi)::extract
│   │   │   └── TimeContract(DuckDBTransformer)::transform >> json
│   │   └── BrandNewContract(NaverSearchAdApi)::extract
│   │       └── BrandNewContract(DuckDBTransformer)::transform >> json
│   └── keyword/
│       └── Keyword(NaverSearchAdApi)::extract
│           └── Keyword(DuckDBTransformer)::transform >> json
├── center/
│   ├─x SearchAdCenter(Extractor)::common
│   ├── adreport/
│   │   ├─x AdvancedReport(SearchAdCenter)::extract
│   │   └── DailyReport(AdvancedReport)::extract
│   │       └── DailyReport(DuckDBTransformer)::transform
│   │           └── AdvancedReport(ExcelTransformer)::transform
│   └── exposure/
│       └── ExposureDiagnosis(SearchAdCenter)::extract
│           ├── ExposureDiagnosis(DuckDBTransformer)::transform >> json
│           └── ExposureRank(ExposureDiagnosis)::transform >> json
└── gfa/
    ├─x SearchAdGfa(Extractor)::common
    └── adreport/
        ├─x _MasterReport(SearchAdGfa)::extract
        ├── Campaign(_MasterReport)::extract
        │   └── Campaign(DuckDBTransformer)::transform >> json
        ├── AdSet(_MasterReport)::extract
        │   └── AdSet(DuckDBTransformer)::transform >> json
        ├── Creative(_MasterReport)::extract
        │   └── Creative(DuckDBTransformer)::transform >> json
        └── PerformanceReport(SearchAdGfa)::extract
            ├── CampaignReport(DuckDBTransformer)::transform
            │   └── ZipCsvTransformer(ExcelTransformer)::transform
            └── CreativeReport(DuckDBTransformer)::transform
                └── ZipCsvTransformer(ExcelTransformer)::transform
```

### SmartStore

```bash
smartstore/
├── api/
│   ├─x SmartstoreApi(Extractor)::common
│   ├─x SmartstoreTestApi(SmartstoreApi)::common
│   ├── product/
│   │   ├── Product(SmartstoreApi)::extract
│   │   │   └── Product(DuckDBTransformer)::transform
│   │   │       └── ProductParser(JsonTransformer)::transform
│   │   └── Option(SmartstoreApi)::extract
│   │       └── Option(DuckDBTransformer)::transform
│   │           ├── OptionSimpleParser(JsonTransformer)::transform
│   │           ├── OptionCombParser(JsonTransformer)::transform
│   │           └── SupplementParser(JsonTransformer)::transform
│   ├── order/
│   │   ├── Order(SmartstoreApi)::extract
│   │   │   ├── Order(DuckDBTransformer)::transform >> json
│   │   │   └── OrderTime(Order)::transform >> json
│   │   └── OrderStatus(SmartstoreApi)::extract
│   │       └── OrderStatus(DuckDBTransformer)::transform >> json
│   └── bizdata/
│       └── MarketingChannel(SmartstoreApi)::extract
│           └── MarketingChannel(DuckDBTransformer)::transform >> json
├── hcenter/
│   ├─x PartnerCenter(Extractor)::common
│   ├─x PartnerCenterLogin(SmartstoreCenterLogin)::common
│   ├── catalog/
│   │   ├─x _CatalogProduct(PartnerCenter)::extract
│   │   ├── BrandCatalog(_CatalogProduct)::extract
│   │   │   └── BrandCatalog(DuckDBTransformer)::transform >> json
│   │   └── BrandProduct(_CatalogProduct)::extract
│   │       ├── BrandProduct(DuckDBTransformer)::transform >> json
│   │       ├── BrandPrice(BrandProduct)::transform >> json
│   │       └── ProductCatalog(BrandProduct)::transform >> json
│   ├── pageview/
│   │   ├─x _PageView(PartnerCenter)::extract
│   │   ├── PageViewByDevice(_PageView)::extract
│   │   │   └── PageViewByDevice(DuckDBTransformer)::transform
│   │   │       └── PageViewParser(JsonTransformer)::transform
│   │   └── PageViewByUrl(_PageView)::extract
│   │       ├── PageViewByUrl(DuckDBTransformer)::transform
│   │       │   └── PageViewParser(JsonTransformer)::transform
│   │       └── PageViewByProduct(DuckDBTransformer)::transform
│   │           └── PageViewParser(JsonTransformer)::transform
│   └── sales/
│       ├─x _Sales(PartnerCenter)::extract
│       ├── StoreSales(_Sales)::extract
│       │   └── StoreSales(DuckDBTransformer)::transform >> json
│       ├── CategorySales(_Sales)::extract
│       │   └── CategorySales(DuckDBTransformer)::transform >> json
│       └── ProductSales(_Sales)::extract
│           ├── ProductSales(DuckDBTransformer)::transform >> json
│           └── AggregatedSales(ProductSales)::transform >> json
└─x sscenter/
    └─x SmartstoreCenterLogin(LoginHandler)::common
```

## 테스트 예시

예를 들어, `coupang_ads` 마커를 가지는 `TestCoupangAds` 클래스의
`test_campaign` 테스트를 실행하면 다음과 같은 결과를 생성한다.

```bash
src/
└── tests/
    └── results/
        └── coupang/
            └── advertising/
                └── adreport/
                    └── Campaign/
                        ├── Campaign/
                        │   ├── CampaignParser/
                        │   │   └── transform.json
                        │   ├── AdgroupParser/
                        │   │   └── transform.json
                        │   ├── campaign.json
                        │   └── adgroup.json
                        └── extract.json
```

`Campaign(DuckDBTransformer)` 클래스에는 2개의 파서와 테이블이 할당되어 있기 때문에   
각각에 대한 `parse()` 실행 결과 및 `SELECT *` 조회 결과를 저장한다.

```python
class Campaign(DuckDBTransformer):
    extractor = "Campaign"
    tables = {"campaign": "coupang_campaign", "adgroup": "coupang_adgroup"}
    parser = {"campaign": CampaignParser, "adgroup": AdgroupParser}
```

또 다른 예시로, `smartstore_api` 마커를 가지는 `TestSmartstoreApi` 클래스의
`test_order` 테스트를 실행하면 다음과 같은 결과를 생성한다.

```bash
src/
└── tests/
    └── results/
        └── smartstore/
            └── api/
                └── order/
                    └── Order/
                        ├── Order/
                        │   ├── json/
                        │   │   └── transform.json
                        │   ├── order.json
                        │   ├── product_order.json
                        │   ├── delivery.json
                        │   └── option.json
                        └── extract.json
```

`Order(DuckDBTransformer)` 클래스는 1개의 파서와 4개의 테이블이 할당되어 있다.

```python
class Order(DuckDBTransformer):
    extractor = "Order"
    tables = {table: f"smartstore_{table}" for table in ["order", "product_order", "delivery", "option"]}
    parser = "json"
```

이어서 동일한 클래스의 `test_order_time` 테스트를 실행하면 다음과 같이 결과가 추가된다.

```bash
src/
└── tests/
    └── results/
        └── smartstore/
            └── api/
                └── order/
                    └── Order/
                        ├── Order/
                        │   ├── json/
                        │   │   └── transform.json
                        │   ├── order.json
                        │   ├── product_order.json
                        │   ├── delivery.json
                        │   └── option.json
                        ├── OrderTime/
                        │   ├── json/
                        │   │   └── transform.json
                        │   └── table.json
                        └── extract.json
```

`OrderTime(Order)` 클래스는 `Order(DuckDBTransformer)` 클래스와 동일한 Extractor를 공유하므로   
같은 경로 아래에 `OrderTime/` 하위 경로가 추가된다.

```python
class OrderTime(Order):
    extractor = "Order"
    tables = {"table": "smartstore_order_time"}
    parser = "json"
```
