"""데이터 변환(Transform) 테스트: `DuckDBTransformer` 동작 검증
- 실행: `pytest src/tests/test_transform.py -m transform -v -s`
    - 특정 도메인의 테스트만 실행 시 마크를 추가한다: `-m "transform and domain"`
- 결과: `src/tests/results/` 하위의 모듈 경로와 대응되는 위치에 `parse()` 및 `bulk_insert()` 결과를 저장한다.
"""
from __future__ import annotations

from tests.conftest import TransformerHarness
import pytest

from typing import TYPE_CHECKING
import datetime as dt

if TYPE_CHECKING:
    from typing import Callable
    from linkmerce.common.transform import DuckDBTransformer
    from linkmerce.utils.nested import KeyPath
    YamlReader = Callable[[KeyPath], dict]
    Harness = Callable[[type[DuckDBTransformer]], TransformerHarness]

pytestmark = pytest.mark.transform


###################################################################
################################ CJ ###############################
###################################################################

class TestCjLogistics:
    """CJ 물류 데이터 변환 테스트.
    - cj.eflexs.stock.Stock"""

    @pytest.mark.skip
    @pytest.mark.cj_logistics
    def test_stock(self, transformer_harness: Harness):
        from linkmerce.core.cj.eflexs.stock.transform import Stock
        transformer_harness(Stock).transform()


###################################################################
########################### Coupang Ads ###########################
###################################################################

class TestCoupangAds:
    """쿠팡 광고 데이터 변환 테스트.
    - coupang.advertising.adreport.Campaign
    - coupang.advertising.adreport.Creative
    - coupang.advertising.adreport.ProductAdReport
    - coupang.advertising.adreport.NewCustomerAdReport"""

    def vendor_id(self, reader: YamlReader) -> str:
        return reader("coupang.advertising.0")["vendor_id"]

    @pytest.mark.skip
    @pytest.mark.coupang_ads
    def test_campaign(self, transformer_harness: Harness, credentials: YamlReader):
        from linkmerce.core.coupang.advertising.adreport.transform import Campaign
        transformer_harness(Campaign).transform(
            vendor_id = self.vendor_id(credentials),
        )

    @pytest.mark.skip
    @pytest.mark.coupang_ads
    def test_creative(self, transformer_harness: Harness, configs: YamlReader, credentials: YamlReader):
        from linkmerce.core.coupang.advertising.adreport.transform import Creative
        transformer_harness(Creative).transform(
            vendor_id = self.vendor_id(credentials),
            map_index = configs("coupang.advertising.creative")["campaign_ids"],
        )

    @pytest.mark.coupang_ads
    def test_product_adreport(self, transformer_harness: Harness, credentials: YamlReader):
        from linkmerce.core.coupang.advertising.adreport.transform import ProductAdReport
        transformer_harness(ProductAdReport).transform(
            vendor_id = self.vendor_id(credentials),
        )

    @pytest.mark.coupang_ads
    def test_new_customer_adreport(self, transformer_harness: Harness, credentials: YamlReader):
        from linkmerce.core.coupang.advertising.adreport.transform import NewCustomerAdReport
        transformer_harness(NewCustomerAdReport).transform(
            vendor_id = self.vendor_id(credentials),
        )


###################################################################
########################## Coupang Wing ###########################
###################################################################

class TestCoupangWing:
    """쿠팡 윙 데이터 변환 테스트.
    - coupang.wing.product.ProductOption
    - coupang.wing.product.ProductDetail
    - coupang.wing.product.ProductDownload
    - coupang.wing.product.RocketInventory
    - coupang.wing.product.RocketOption
    - coupang.wing.settlement.RocketSettlement
    - coupang.wing.settlement.RocketSettlementDownload"""

    def vendor_id(self, reader: YamlReader) -> str:
        return reader("coupang.advertising.0")["vendor_id"]

    @pytest.mark.coupang_wing
    def test_product_option(self, transformer_harness: Harness, options: YamlReader):
        from linkmerce.core.coupang.wing.product.transform import ProductOption
        _configs = options("coupang.wing.product_option")
        transformer_harness(ProductOption).transform(
            is_deleted = _configs.get("is_deleted", False),
        )

    @pytest.mark.coupang_wing
    def test_product_detail(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.coupang.wing.product.transform import ProductDetail
        _configs = configs("coupang.wing.product_detail")
        transformer_harness(ProductDetail).transform(
            map_index = _configs["vendor_inventory_id"],
        )

    @pytest.mark.skip
    @pytest.mark.coupang_wing
    def test_product_download(self, transformer_harness: Harness, options: YamlReader, credentials: YamlReader):
        from linkmerce.core.coupang.wing.product.transform import ProductDownload
        _configs = options("coupang.wing.product_download")
        transformer_harness(ProductDownload).transform(
            vendor_id = self.vendor_id(credentials),
            is_deleted = _configs.get("is_deleted", False),
        )

    @pytest.mark.coupang_wing
    def test_rocket_inventory(self, transformer_harness: Harness, credentials: YamlReader):
        from linkmerce.core.coupang.wing.product.transform import RocketInventory
        transformer_harness(RocketInventory).transform(
            vendor_id = self.vendor_id(credentials),
        )

    @pytest.mark.coupang_wing
    def test_rocket_option(self, transformer_harness: Harness, credentials: YamlReader):
        from linkmerce.core.coupang.wing.product.transform import RocketOption
        transformer_harness(RocketOption).transform(
            vendor_id = self.vendor_id(credentials),
        )

    @pytest.mark.coupang_wing
    def test_rocket_settlement(self, transformer_harness: Harness, credentials: YamlReader):
        from linkmerce.core.coupang.wing.settlement.transform import RocketSettlement
        transformer_harness(RocketSettlement).transform(
            vendor_id = self.vendor_id(credentials),
        )

    @pytest.mark.coupang_wing
    def test_rocket_settlement_download(self, transformer_harness: Harness, credentials: YamlReader):
        from linkmerce.core.coupang.wing.settlement.transform import RocketSettlementDownload
        harness = transformer_harness(RocketSettlementDownload)
        kwargs = dict(vendor_id = self.vendor_id(credentials))
        parser = ["RocketSalesParser", "RocketShippingParser"]

        for i, report_type in enumerate(["CATEGORY_TR", "WAREHOUSING_SHIPPING"]):
            obj, map_index = harness.load_extract(map_index=report_type)
            result = harness.parse(obj, report_type=report_type, skip_dump=True, **kwargs)
            harness.dump_result(result, parser=parser[i])
            harness.bulk_insert(result, report_type=report_type, skip_dump=(i == 0), **kwargs)


###################################################################
############################## Ecount #############################
###################################################################

class TestEcount:
    """이카운트 데이터 변환 테스트.
    - ecount.api.inventory.Inventory
    - ecount.api.product.Product"""

    @pytest.mark.ecount
    def test_inventory(self, transformer_harness: Harness):
        from linkmerce.core.ecount.api.inventory.transform import Inventory
        transformer_harness(Inventory).transform()

    @pytest.mark.ecount
    def test_product(self, transformer_harness: Harness):
        from linkmerce.core.ecount.api.product.transform import Product
        transformer_harness(Product).transform()


###################################################################
########################### Google Ads ############################
###################################################################

class TestGoogleAds:
    """구글 광고 데이터 변환 테스트.
    - google.api.ads.Campaign
    - google.api.ads.AdGroup
    - google.api.ads.Ad
    - google.api.ads.Insight
    - google.api.ads.Asset
    - google.api.ads.AssetView"""

    def customer_id(self, reader: YamlReader) -> str:
        return reader("google.ads_api.0")["customer_id"]

    @pytest.mark.google_ads
    def test_campaign(self, transformer_harness: Harness, credentials: YamlReader):
        from linkmerce.core.google.api.ads.transform import Campaign
        transformer_harness(Campaign).transform(
            customer_id = self.customer_id(credentials),
        )

    @pytest.mark.google_ads
    def test_ad_group(self, transformer_harness: Harness, credentials: YamlReader):
        from linkmerce.core.google.api.ads.transform import AdGroup
        transformer_harness(AdGroup).transform(
            customer_id = self.customer_id(credentials),
        )

    @pytest.mark.google_ads
    def test_ad(self, transformer_harness: Harness, credentials: YamlReader):
        from linkmerce.core.google.api.ads.transform import Ad
        transformer_harness(Ad).transform(
            customer_id = self.customer_id(credentials),
        )

    @pytest.mark.google_ads
    def test_insight(self, transformer_harness: Harness, credentials: YamlReader):
        from linkmerce.core.google.api.ads.transform import Insight
        transformer_harness(Insight).transform(
            customer_id = self.customer_id(credentials),
        )

    @pytest.mark.google_ads
    def test_asset(self, transformer_harness: Harness, credentials: YamlReader):
        from linkmerce.core.google.api.ads.transform import Asset
        transformer_harness(Asset).transform(
            customer_id = self.customer_id(credentials),
        )

    @pytest.mark.google_ads
    def test_asset_view(self, transformer_harness: Harness, credentials: YamlReader):
        from linkmerce.core.google.api.ads.transform import AssetView
        transformer_harness(AssetView).transform(
            customer_id = self.customer_id(credentials),
        )


###################################################################
############################ Meta Ads #############################
###################################################################

class TestMetaAds:
    """메타 광고 데이터 변환 테스트.
    - meta.api.ads.Campaigns
    - meta.api.ads.Adsets
    - meta.api.ads.Ads
    - meta.api.ads.Insights"""

    def account_id(self, reader: YamlReader) -> str:
        return reader("meta.api.campaigns")["account_ids"][0]

    @pytest.mark.meta_ads
    def test_campaigns(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.meta.api.ads.transform import Campaigns
        transformer_harness(Campaigns).transform(
            account_id = self.account_id(configs),
        )

    @pytest.mark.meta_ads
    def test_adsets(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.meta.api.ads.transform import Adsets
        transformer_harness(Adsets).transform(
            account_id = self.account_id(configs),
        )

    @pytest.mark.meta_ads
    def test_ads(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.meta.api.ads.transform import Ads
        transformer_harness(Ads).transform(
            account_id = self.account_id(configs),
        )

    @pytest.mark.meta_ads
    def test_insights(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.meta.api.ads.transform import Insights
        transformer_harness(Insights).transform(
            account_id = self.account_id(configs),
        )


###################################################################
########################## Naver Search ###########################
###################################################################

class TestNaverSearch:
    """네이버 메인 검색 데이터 변환 테스트.
    - naver.main.search.Search
    - naver.main.search.CafeTab
    - naver.main.search.CafeArticle"""

    @pytest.mark.naver_search
    def test_search(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.naver.main.search.transform import Search
        if TYPE_CHECKING:
            class SearchHarness(TransformerHarness, Search):
                ...

        _configs = configs("naver.main.search")
        harness: SearchHarness = transformer_harness(Search)

        query, sep = _configs["query"], _configs.get("sep", '\n')
        obj, _ = harness.load_extract(map_index=query)
        sections = harness.parse(obj, mobile=_configs.get("mobile", True), sep=sep, map_index=query)

        harness.insert_serialized_sections(query, sections)
        summary = harness.summarize_sections(query, sections, sep)
        harness.bulk_insert(summary, map_index=query, render={"summary": harness.tables["summary"]})

    @pytest.mark.naver_search
    def test_cafe_tab(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.naver.main.search.transform import CafeTab
        _configs = configs("naver.main.search_tab")
        transformer_harness(CafeTab).transform(
            query = _configs["query"],
            map_index = _configs["query"],
        )

    @pytest.mark.naver_search
    def test_cafe_article(self, transformer_harness: Harness):
        from linkmerce.core.naver.main.search.transform import CafeArticle
        transformer_harness(CafeArticle).transform()


###################################################################
######################### Naver Open API ##########################
###################################################################

class TestNaverOpenApi:
    """네이버 오픈 API 데이터 변환 테스트.
    - naver.openapi.search.BlogSearch
    - naver.openapi.search.NewsSearch
    - naver.openapi.search.BookSearch
    - naver.openapi.search.CafeSearch
    - naver.openapi.search.KiNSearch
    - naver.openapi.search.ImageSearch
    - naver.openapi.search.ShoppingSearch
    - naver.openapi.search.ShoppingRank"""

    @pytest.mark.naver_open_api
    def test_blog_search(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.naver.openapi.search.transform import BlogSearch
        _configs = configs("naver.openapi.blog_search")
        transformer_harness(BlogSearch).transform(
            query = _configs["query"],
            start = _configs.get("start", 1),
            map_index = _configs["query"],
        )

    @pytest.mark.naver_open_api
    def test_news_search(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.naver.openapi.search.transform import NewsSearch
        _configs = configs("naver.openapi.news_search")
        transformer_harness(NewsSearch).transform(
            query = _configs["query"],
            start = _configs.get("start", 1),
            map_index = _configs["query"],
        )

    @pytest.mark.naver_open_api
    def test_book_search(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.naver.openapi.search.transform import BookSearch
        _configs = configs("naver.openapi.book_search")
        transformer_harness(BookSearch).transform(
            query = _configs["query"],
            start = _configs.get("start", 1),
            map_index = _configs["query"],
        )

    @pytest.mark.naver_open_api
    def test_cafe_search(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.naver.openapi.search.transform import CafeSearch
        _configs = configs("naver.openapi.cafe_search")
        transformer_harness(CafeSearch).transform(
            query = _configs["query"],
            start = _configs.get("start", 1),
            map_index = _configs["query"],
        )

    @pytest.mark.naver_open_api
    def test_kin_search(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.naver.openapi.search.transform import KiNSearch
        _configs = configs("naver.openapi.kin_search")
        transformer_harness(KiNSearch).transform(
            query = _configs["query"],
            start = _configs.get("start", 1),
            map_index = _configs["query"],
        )

    @pytest.mark.naver_open_api
    def test_image_search(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.naver.openapi.search.transform import ImageSearch
        _configs = configs("naver.openapi.image_search")
        transformer_harness(ImageSearch).transform(
            query = _configs["query"],
            start = _configs.get("start", 1),
            map_index = _configs["query"],
        )

    @pytest.mark.naver_open_api
    def test_shopping_search(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.naver.openapi.search.transform import ShoppingSearch
        _configs = configs("naver.openapi.shopping_search")
        transformer_harness(ShoppingSearch).transform(
            query = _configs["query"],
            start = _configs.get("start", 1),
            map_index = _configs["query"],
        )

    @pytest.mark.naver_open_api
    def test_shopping_rank(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.naver.openapi.search.transform import ShoppingRank
        _configs = configs("naver.openapi.shopping_search")
        transformer_harness(ShoppingRank).transform(
            query = _configs["query"],
            start = _configs.get("start", 1),
            map_index = _configs["query"],
        )


###################################################################
########################### SabangNet #############################
###################################################################

class TestSabangNet:
    """사방넷 데이터 변환 테스트.
    - sabangnet.admin.order.Order
    - sabangnet.admin.order.OrderDownload
    - sabangnet.admin.order.OrderStatus
    - sabangnet.admin.order.ProductMapping
    - sabangnet.admin.order.SkuMapping
    - sabangnet.admin.product.Product
    - sabangnet.admin.product.Option
    - sabangnet.admin.product.OptionDownload
    - sabangnet.admin.product.AddProductGroup
    - sabangnet.admin.product.AddProduct"""

    @pytest.mark.sabangnet
    def test_order(self, transformer_harness: Harness):
        from linkmerce.core.sabangnet.admin.order.transform import Order
        transformer_harness(Order).transform()

    @pytest.mark.sabangnet
    def test_order_download(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.sabangnet.admin.order.transform import OrderDownload
        if TYPE_CHECKING:
            class OrderDownloadHarness(TransformerHarness, OrderDownload):
                ...

        _configs = configs("sabangnet.admin.order_download")
        harness: OrderDownloadHarness = transformer_harness(OrderDownload, download_type="order")

        params: dict = _configs["download_no"]
        for i, (name, no) in enumerate(params.items(), start=1):
            harness.set_fields(download_type=name)
            obj, map_index = harness.load_extract(map_index=name)
            result = harness.parse(obj, map_index=map_index)
            harness.bulk_insert(result, skip_dump=(i != len(params)))

    @pytest.mark.skip
    @pytest.mark.sabangnet
    def test_order_status(self, transformer_harness: Harness):
        from linkmerce.core.sabangnet.admin.order.transform import OrderStatus
        harness = transformer_harness(OrderStatus)
        for date_type in ["delivery_confirm_date", "cancel_dt", "rtn_dt", "chng_dt"]:
            harness.transform(date_type=date_type, map_index=date_type)

    @pytest.mark.sabangnet
    def test_product_mapping(self, transformer_harness: Harness):
        from linkmerce.core.sabangnet.admin.order.transform import ProductMapping
        transformer_harness(ProductMapping).transform()

    @pytest.mark.sabangnet
    def test_sku_mapping(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.sabangnet.admin.order.transform import SkuMapping
        _configs = configs("sabangnet.admin.sku_mapping")
        transformer_harness(SkuMapping).transform(
            query = {"shop_id": _configs["shop_id"]},
        )

    @pytest.mark.sabangnet
    def test_product(self, transformer_harness: Harness):
        from linkmerce.core.sabangnet.admin.product.transform import Product
        transformer_harness(Product).transform()

    @pytest.mark.sabangnet
    def test_option(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.sabangnet.admin.product.transform import Option
        _configs = configs("sabangnet.admin.option")
        transformer_harness(Option).transform(
            map_index = _configs["product_id"],
        )

    @pytest.mark.sabangnet
    def test_option_download(self, transformer_harness: Harness):
        from linkmerce.core.sabangnet.admin.product.transform import OptionDownload
        transformer_harness(OptionDownload).transform()

    @pytest.mark.sabangnet
    def test_add_product_group(self, transformer_harness: Harness):
        from linkmerce.core.sabangnet.admin.product.transform import AddProductGroup
        transformer_harness(AddProductGroup).transform()

    @pytest.mark.sabangnet
    def test_add_product(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.sabangnet.admin.product.transform import AddProduct
        if TYPE_CHECKING:
            class AddProductHarness(TransformerHarness, AddProduct):
                ...

        _configs = configs("sabangnet.admin.add_product")
        harness: AddProductHarness = transformer_harness(AddProduct)

        kwargs = dict(map_index=_configs["group_id"])
        obj, _ = harness.load_extract(**kwargs)
        result = harness.parse(obj, **kwargs)
        harness.bulk_insert(result, params={"meta": harness.parse_metadata(obj)}, **kwargs)


###################################################################
########################## SearchAd API ###########################
###################################################################

class TestSearchAdAPI:
    """네이버 검색광고 API 데이터 변환 테스트.
    - searchad.api.adreport.Campaign
    - searchad.api.adreport.Adgroup
    - searchad.api.adreport.Ad
    - searchad.api.contract.TimeContract
    - searchad.api.contract.BrandNewContract
    - searchad.api.keyword.Keyword"""

    @pytest.mark.searchad_api
    def test_campaign(self, transformer_harness: Harness):
        from linkmerce.core.searchad.api.adreport.transform import Campaign
        transformer_harness(Campaign).transform()

    @pytest.mark.searchad_api
    def test_adgroup(self, transformer_harness: Harness):
        from linkmerce.core.searchad.api.adreport.transform import Adgroup
        transformer_harness(Adgroup).transform()

    @pytest.mark.searchad_api
    def test_ad(self, transformer_harness: Harness):
        from linkmerce.core.searchad.api.adreport.transform import Ad, AD_TABLE_KEYS
        if TYPE_CHECKING:
            class AdHarness(TransformerHarness, Ad):
                ...

        harness: AdHarness = transformer_harness(Ad)
        table_parser = harness.table_parser

        for report_type in table_parser.keys():
            tsv_data, _ = harness.load_extract(map_index=report_type)

            table_key, parser = table_parser[report_type]
            result = parser().transform(tsv_data)
            harness.dump_result(result, parser=parser, index=report_type)

            query_key = f"bulk_insert_{table_key}"
            harness.bulk_insert(result, query_key, render=harness.get_table(table_key), skip_dump=True)

        for table_key in AD_TABLE_KEYS[:4]:
            query_key = f"transform_{table_key}"
            harness.insert_into(query_key, render=harness.tables)
        harness.dump_tables()

    @pytest.mark.searchad_api
    def test_time_contract(self, transformer_harness: Harness):
        from linkmerce.core.searchad.api.contract.transform import TimeContract
        transformer_harness(TimeContract).transform()

    @pytest.mark.searchad_api
    def test_brand_new_contract(self, transformer_harness: Harness):
        from linkmerce.core.searchad.api.contract.transform import BrandNewContract
        transformer_harness(BrandNewContract).transform()

    @pytest.mark.searchad_api
    def test_keyword(self, transformer_harness: Harness):
        from linkmerce.core.searchad.api.keyword.transform import Keyword
        transformer_harness(Keyword).transform()


###################################################################
########################## SearchAd GFA ###########################
###################################################################

class TestSearchAdGFA:
    """네이버 GFA 데이터 변환 테스트.
    - searchad.gfa.adreport.Campaign
    - searchad.gfa.adreport.AdSet
    - searchad.gfa.adreport.Creative
    - searchad.gfa.adreport.CampaignReport
    - searchad.gfa.adreport.CreativeReport"""

    def account_no(self, reader: YamlReader):
        return reader("searchad.gfa.0")["account_no"]

    @pytest.mark.searchad_gfa
    def test_campaign(self, transformer_harness: Harness, options: YamlReader):
        from linkmerce.core.searchad.gfa.adreport.transform import Campaign
        _configs = options("searchad.gfa.campaign")
        transformer_harness(Campaign).transform(
            map_index = _configs.get("status", "RUNNABLE"),
        )

    @pytest.mark.searchad_gfa
    def test_ad_set(self, transformer_harness: Harness, options: YamlReader, credentials: YamlReader):
        from linkmerce.core.searchad.gfa.adreport.transform import AdSet
        _configs = options("searchad.gfa.ad_set")
        transformer_harness(AdSet).transform(
            account_no = self.account_no(credentials),
            map_index = _configs.get("status", "ALL"),
        )

    @pytest.mark.searchad_gfa
    def test_creative(self, transformer_harness: Harness, options: YamlReader, credentials: YamlReader):
        from linkmerce.core.searchad.gfa.adreport.transform import Creative
        _configs = options("searchad.gfa.creative")
        transformer_harness(Creative).transform(
            account_no = self.account_no(credentials),
            map_index = _configs.get("status", "ALL"),
        )

    @pytest.mark.searchad_gfa
    def test_campaign_report(self, transformer_harness: Harness, credentials: YamlReader):
        from linkmerce.core.searchad.gfa.adreport.transform import CampaignReport
        transformer_harness(CampaignReport).transform(
            account_no = self.account_no(credentials),
        )

    @pytest.mark.searchad_gfa
    def test_creative_report(self, transformer_harness: Harness, credentials: YamlReader):
        from linkmerce.core.searchad.gfa.adreport.transform import CreativeReport
        transformer_harness(CreativeReport).transform(
            account_no = self.account_no(credentials),
        )


###################################################################
######################## SearchAd Manage ##########################
###################################################################

class TestSearchAdManage:
    """네이버 검색광고 관리 데이터 변환 테스트.
    - searchad.manage.adreport.DailyReport
    - searchad.manage.exposure.ExposureDiagnosis
    - searchad.manage.exposure.ExposureRank"""

    def customer_id(self, reader: YamlReader):
        return reader("searchad.manage.0")["customer_id"]

    @pytest.mark.searchad_manage
    def test_daily_report(self, transformer_harness: Harness, credentials: YamlReader):
        from linkmerce.core.searchad.manage.adreport.transform import DailyReport
        transformer_harness(DailyReport).transform(
            customer_id = self.customer_id(credentials),
        )

    @pytest.mark.searchad_manage
    def test_exposure_diagnosis(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.searchad.manage.exposure.transform import ExposureDiagnosis
        _configs = configs("searchad.manage.exposure_diagnosis")
        transformer_harness(ExposureDiagnosis).transform(
            keyword = _configs["keyword"],
            is_own = _configs.get("is_own"),
            map_index = _configs["keyword"],
        )

    @pytest.mark.searchad_manage
    def test_exposure_rank(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.searchad.manage.exposure.transform import ExposureRank
        _configs = configs("searchad.manage.exposure_diagnosis")
        transformer_harness(ExposureRank).transform(
            keyword = _configs["keyword"],
            is_own = _configs.get("is_own"),
            map_index = _configs["keyword"],
        )


###################################################################
######################### SmartStore API ##########################
###################################################################

class TestSmartstoreApi:
    """스마트스토어 API 데이터 변환 테스트.
    - smartstore.api.product.Product
    - smartstore.api.product.Option
    - smartstore.api.order.Order
    - smartstore.api.order.OrderTime
    - smartstore.api.order.OrderStatus"""

    def channel_seq(self, reader: YamlReader):
        return reader("smartstore.api.0")["channel_seq"]

    @pytest.mark.smartstore_api
    def test_product(self, transformer_harness: Harness, credentials: YamlReader):
        from linkmerce.core.smartstore.api.product.transform import Product
        transformer_harness(Product).transform(
            channel_seq = self.channel_seq(credentials),
        )

    @pytest.mark.smartstore_api
    def test_option(self, transformer_harness: Harness, configs: YamlReader, credentials: YamlReader):
        from linkmerce.core.smartstore.api.product.transform import Option
        _configs = configs("smartstore.api.option")
        transformer_harness(Option).transform(
            product_id = _configs["product_id"],
            channel_seq = self.channel_seq(credentials),
            map_index = _configs["product_id"],
        )

    @pytest.mark.smartstore_api
    def test_order(self, transformer_harness: Harness):
        from linkmerce.core.smartstore.api.order.transform import Order
        transformer_harness(Order).transform()

    @pytest.mark.smartstore_api
    def test_order_time(self, transformer_harness: Harness, credentials: YamlReader):
        from linkmerce.core.smartstore.api.order.transform import OrderTime
        transformer_harness(OrderTime).transform(
            channel_seq = self.channel_seq(credentials),
        )

    @pytest.mark.smartstore_api
    def test_order_status(self, transformer_harness: Harness, credentials: YamlReader):
        from linkmerce.core.smartstore.api.order.transform import OrderStatus
        transformer_harness(OrderStatus).transform(
            channel_seq = self.channel_seq(credentials),
        )


###################################################################
###################### SmartStore Bizdata API #####################
###################################################################

class TestBizdataApi:
    """스마트스토어 API데이터솔루션(통계) 데이터 변환 테스트.
    - smartstore.api.bizdata.MarketingChannel"""

    def channel_seq(self, reader: YamlReader):
        return reader("smartstore.bizdata.0")["channel_seq"]

    @pytest.mark.bizdata_api
    def test_marketing_channel(self, transformer_harness: Harness, credentials: YamlReader, yesterday: dt.date):
        from linkmerce.core.smartstore.api.bizdata.transform import MarketingChannel
        transformer_harness(MarketingChannel).transform(
            channel_seq = self.channel_seq(credentials),
            date = yesterday,
        )


###################################################################
######################## SmartStore Brand #########################
###################################################################

class TestSmartStoreBrand:
    """스마트스토어 브랜드 데이터 변환 테스트.
    - smartstore.brand.catalog.BrandCatalog
    - smartstore.brand.catalog.BrandProduct
    - smartstore.brand.catalog.BrandPrice
    - smartstore.brand.catalog.ProductCatalog
    - smartstore.brand.pageview.PageViewByDevice
    - smartstore.brand.pageview.PageViewByUrl
    - smartstore.brand.pageview.PageViewByProduct
    - smartstore.brand.sales.StoreSales
    - smartstore.brand.sales.CategorySales
    - smartstore.brand.sales.ProductSales
    - smartstore.brand.sales.AggregatedSales"""

    eol_date = dt.date(2026, 2, 26)

    @pytest.mark.smartstore_brand
    def test_brand_catalog(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.smartstore.brand.catalog.transform import BrandCatalog
        _configs = configs("smartstore.brand.brand_catalog")
        transformer_harness(BrandCatalog).transform(
            map_index = _configs["brand_ids"],
        )

    @pytest.mark.smartstore_brand
    def test_brand_product(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.smartstore.brand.catalog.transform import BrandProduct
        _configs = configs("smartstore.brand.brand_product")
        transformer_harness(BrandProduct).transform(
            mall_seq = _configs["mall_seq"],
            map_index = _configs["brand_ids"],
        )

    @pytest.mark.smartstore_brand
    def test_brand_price(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.smartstore.brand.catalog.transform import BrandPrice
        _configs = configs("smartstore.brand.brand_product")
        transformer_harness(BrandPrice).transform(
            mall_seq = _configs["mall_seq"],
            map_index = _configs["brand_ids"],
        )

    @pytest.mark.smartstore_brand
    def test_product_catalog(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.smartstore.brand.catalog.transform import ProductCatalog
        _configs = configs("smartstore.brand.brand_product")
        transformer_harness(ProductCatalog).transform(
            mall_seq = _configs["mall_seq"],
            map_index = _configs["brand_ids"],
        )

    @pytest.mark.smartstore_brand
    def test_page_view_by_device(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.smartstore.brand.pageview.transform import PageViewByDevice
        _configs = configs("smartstore.brand.page_view_by_device")
        transformer_harness(PageViewByDevice).transform(
            mall_seq = _configs["mall_seq"],
            map_index = _configs["mall_seq"],
        )

    @pytest.mark.smartstore_brand
    def test_page_view_by_url(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.smartstore.brand.pageview.transform import PageViewByUrl
        _configs = configs("smartstore.brand.page_view_by_url")
        transformer_harness(PageViewByUrl).transform(
            mall_seq = _configs["mall_seq"],
            map_index = _configs["mall_seq"],
        )

    @pytest.mark.smartstore_brand
    def test_page_view_by_product(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.smartstore.brand.pageview.transform import PageViewByProduct
        _configs = configs("smartstore.brand.page_view_by_url")
        transformer_harness(PageViewByProduct).transform(
            mall_seq = _configs["mall_seq"],
            map_index = _configs["mall_seq"],
        )

    @pytest.mark.smartstore_brand
    def test_store_sales(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.smartstore.brand.sales.transform import StoreSales
        _configs = configs("smartstore.brand.store_sales")
        transformer_harness(StoreSales).transform(
            mall_seq = _configs["mall_seq"],
            end_date = self.eol_date,
            map_index = _configs["mall_seq"],
        )

    @pytest.mark.smartstore_brand
    def test_category_sales(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.smartstore.brand.sales.transform import CategorySales
        _configs = configs("smartstore.brand.category_sales")
        transformer_harness(CategorySales).transform(
            mall_seq = _configs["mall_seq"],
            end_date = self.eol_date,
            map_index = _configs["mall_seq"],
        )

    @pytest.mark.smartstore_brand
    def test_product_sales(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.smartstore.brand.sales.transform import ProductSales
        _configs = configs("smartstore.brand.product_sales")
        transformer_harness(ProductSales).transform(
            mall_seq = _configs["mall_seq"],
            end_date = self.eol_date,
            map_index = _configs["mall_seq"],
        )

    @pytest.mark.smartstore_brand
    def test_aggregated_sales(self, transformer_harness: Harness, configs: YamlReader):
        from linkmerce.core.smartstore.brand.sales.transform import AggregatedSales
        _configs = configs("smartstore.brand.product_sales")
        transformer_harness(AggregatedSales).transform(
            mall_seq = _configs["mall_seq"],
            start_date = self.eol_date,
            end_date = self.eol_date,
            map_index = _configs["mall_seq"],
        )
