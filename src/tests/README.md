# 테스트 안내

- `/` 로 끝나는 노드는 모듈
- 가지가 `─x` 로 표시된 노드는 하위 노드를 포함해 테스트 제외
- 모듈이 아닌 노드는 `클래스(부모클래스)::모듈` 형태로 표시
- `Extractor` > (`ResponseTransformer`) > `DuckDBTransformer`로 이어지는 구조를 트리로 표시
- `DuckDBTransformer`가 기본형 `ResponseTransformer`를 사용하면 `>>` 로 한 줄 표시
- 기본형 `ResponseTransformer` 종류:
  - `json`: `JsonTransformer`
  - `html`: `HtmlTransformer`
  - `excel`: `ExcelTransformer`
- 커스텀 `ResponseTransformer` 종류:
  - `csv`: `CsvTransformer`
  - `tsv`: `TsvTransformer`
- [테스트 예시](#테스트-예시)에 따라 테스트 중 중간 결과 저장

# 테스트 대상

## CJ

```bash
cj/
└── eflexs/
    ├─x CjEflexs(Extractor)::common
    └── stock/
        └── Stock(CjEflexs)::extract
            └── Stock(DuckDBTransformer)::transform >> json
```

## Coupang

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

## Ecount

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

## Google

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

## Meta

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

## Naver

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

## SabangNet

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

## SearchAd

```bash
searchad/
├── api/
│   ├── NaverSearchAdApi(Extractor)::common
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
├── gfa/
│   ├── SearchAdGFA(Extractor)::common
│   └── adreport/
│       ├─x _MasterReport(SearchAdGFA)::extract
│       ├── Campaign(_MasterReport)::extract
│       │   └── Campaign(DuckDBTransformer)::transform >> json
│       ├── AdSet(_MasterReport)::extract
│       │   └── AdSet(DuckDBTransformer)::transform >> json
│       ├── Creative(_MasterReport)::extract
│       │   └── Creative(DuckDBTransformer)::transform >> json
│       ├─x PerformanceReport(SearchAdGFA)::extract
│       ├── CampaignReport(PerformanceReport)::extract
│       │   └── CampaignReport(DuckDBTransformer)::transform
│       │       └── CsvTransformer(ExcelTransformer)::transform
│       └── CreativeReport(PerformanceReport)::extract
│           └── CreativeReport(DuckDBTransformer)::transform
│               └── CsvTransformer(ExcelTransformer)::transform
└── manage/
    ├─x SearchAdManager(Extractor)::common
    ├── adreport/
    │   ├─x AdvancedReport(SearchAdManager)::extract
    │   └── DailyReport(AdvancedReport)::extract
    │       └── DailyReport(DuckDBTransformer)::transform
    │           └── AdvancedReport(ExcelTransformer)::transform
    └── exposure/
        └── ExposureDiagnosis(SearchAdManager)::extract
            └── ExposureDiagnosis(DuckDBTransformer)::transform
                ├── ExposureParser(JsonTransformer)::transform
                └── ExposureRank(ExposureDiagnosis)::transform
```

## SmartStore

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
│   │   │   └── BrandCatalog(DuckDBTransformer)::transform
│   │   │       └── CatalogItems(JsonTransformer)::transform
│   │   └── BrandProduct(_CatalogProduct)::extract
│   │       ├── BrandProduct(DuckDBTransformer)::transform
│   │       │   └── CatalogItems(JsonTransformer)::transform
│   │       ├── BrandPrice(BrandProduct)::transform
│   │       │   └── CatalogItems(JsonTransformer)::transform
│   │       └── ProductCatalog(BrandProduct)::transform
│   │           └── CatalogItems(JsonTransformer)::transform
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
│       │   └── StoreSales(DuckDBTransformer)::transform
│       │       └── SalesParser(JsonTransformer)::transform
│       ├── CategorySales(_Sales)::extract
│       │   └── CategorySales(DuckDBTransformer)::transform
│       │       └── SalesParser(JsonTransformer)::transform
│       └── ProductSales(_Sales)::extract
│           ├── ProductSales(DuckDBTransformer)::transform
│           │   └── SalesParser(JsonTransformer)::transform
│           └── AggregatedSales(ProductSales)::transform
│               └── SalesParser(JsonTransformer)::transform
└─x sscenter/
    └─x SmartstoreCenterLogin(LoginHandler)::common
```

# 테스트 예시

`src/tests/results/` 경로 아래에 트리와 동일한 경로로 테스트 결과 생성

- `Extractor`에서 하위 `Transformer`로 HTTP 응답 데이터를 전달하기 전에 `extract.{확장자}` 파일로 중간 저장
  - 확장자는 `ResponseTransformer`를 통해 추정, 또는 반환 객체 타입을 통해 추정, 모르면 텍스트 파일로 저장
  - `.json` 파일 저장 시 `indent=2, ensure_ascii=False, default=str` 설정 저장
    - `JsonTransformer` 또는 반환 객체 타입이 `dict` 또는 `list`인 경우
  - `.html` 파일 저장 시 `BeautifulSoup` 객체의 `prettify()` 메서드로 문자열 변환하여 저장
    - `HtmlTransformer` 또는 반환 객체 타입이 `BeautifulSoup`인 경우
  - `.xlsx` 파일 저장 시 `bytes` 객체를 `mode="wb"`로 저장
    - `ExcelTransformer` 또는 반환 객체 타입이 `bytes`인 경우
  - `.csv` 또는 `.tsv` 파일 저장 시 `str` 객체를 `mode="w", encoding="utf-8"`로 저장
    - 반환 객체 타입이 `str`이고, `CsvTransformer` 또는 `TsvTransformer`인 경우
  - 그 외엔 확장자 없이 저장
    - 반환 객체 타입이 `bytes`인 경우 `mode="wb"`로 저장
    - 반환 객체 타입이 `str`인 경우 `mode="w", encoding="utf-8"`로 저장
- `ResponseTransformer`는 `transform()` 메서드 실행 결과를 `transform.json` 파일로 저장
- `DuckDBTransformer`는 모든 테이블의 `SELECT *` 결과를 각각의 `{테이블키}.json` 파일로 저장

예를 들어, `coupang.advertising.adreport` 모듈에서 `Campaign` 테스트 시 다음과 같은 결과 생성

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

`Campaign(DuckDBTransformer)`는 2개의 테이블이 있으므로 각각의 `SELECT *` 결과를 저장

```python
class Campaign(DuckDBTransformer):
    tables = {"campaign": "coupang_campaign", "adgroup": "coupang_adgroup"}
    parser = {"campaign": CampaignParser, "adgroup": AdgroupParser}
```

또 하나의 예시로, `smartstore.api.order` 모듈에서 `Order` 테스트 시 다음과 같은 결과 생성

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

1. 하나의 `Extractor`에 여러 개의 `DuckDBTransformer`가 연결된 경우 위와 같이 폴더 구분
2. 기본형 `ResponseTransformer`를 사용하면 `json`, `excel` 등 상수명을 폴더로 사용

```python
class Order(DuckDBTransformer):
    tables = {table: f"smartstore_{table}" for table in ["order", "product_order", "delivery", "option"]}
    parser = "json"


class OrderTime(Order):
    tables = {"table": "smartstore_order_time"}
    parser = "json"
```
