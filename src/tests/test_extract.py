"""데이터 추출(Extract) 테스트: `Extractor` 동작 검증
- 실행: `pytest src/tests/test_extract.py -m extract -v -s`
    - 특정 도메인의 테스트만 실행 시 마크를 추가한다: `-m \"extract and domain\" `
- 결과: `src/tests/results/` 하위의 모듈 경로와 대응되는 위치에 `Extractor.extract` 메서드 반환 값을 저장한다.
"""
from __future__ import annotations

import pytest

from typing import TYPE_CHECKING
import datetime as dt

if TYPE_CHECKING:
    from typing import Callable
    from linkmerce.utils.nested import KeyPath
    YamlReader = Callable[[KeyPath], dict]

pytestmark = pytest.mark.extract


###################################################################
################################ CJ ###############################
###################################################################

class TestCjLogistics:
    """CJ 물류 데이터 추출 테스트.
    - cj.eflexs.stock.Stock"""

    def credentials(self, reader: YamlReader) -> dict:
        _credentials = reader("cjlogistics.eflexs")
        return {
            "userid": _credentials["userid"],
            "passwd": _credentials["passwd"],
            "mail_info": _credentials["mail_info"],
        }

    @pytest.mark.skip
    @pytest.mark.cj_logistics
    def test_stock(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.cj.eflexs.stock.extract import Stock
        _configs = configs("cjlogistics.eflexs.stock")
        Stock(
            configs = self.credentials(credentials),
            parser = dump_extract(Stock, format="json"),
        ).extract(
            customer_id = _configs["customer_id"],
            start_date = _configs.get("start_date", ":last_week:"),
            end_date = _configs.get("end_date", ":today:"),
        )


###################################################################
########################### Coupang Ads ###########################
###################################################################

class TestCoupangAds:
    """쿠팡 광고 데이터 추출 테스트.
    - coupang.advertising.adreport.Campaign
    - coupang.advertising.adreport.Creative
    - coupang.advertising.adreport.ProductAdReport
    - coupang.advertising.adreport.NewCustomerAdReport"""

    def cookies(self, reader: YamlReader) -> str:
        return reader("coupang.advertising.0")["cookies"]

    @pytest.mark.skip
    @pytest.mark.coupang_ads
    def test_campaign(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.coupang.advertising.adreport.extract import Campaign
        _configs = options("coupang.advertising.campaign")
        Campaign(
            headers = {"cookies": self.cookies(credentials)},
            parser = dump_extract(Campaign, format="json"),
        ).extract(
            goal_type = _configs.get("goal_type", "SALES"),
            is_deleted = _configs.get("is_deleted", False),
        )

    @pytest.mark.skip
    @pytest.mark.coupang_ads
    def test_creative(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.coupang.advertising.adreport.extract import Creative
        _configs = configs("coupang.advertising.creative")
        Creative(
            headers = {"cookies": self.cookies(credentials)},
            parser = dump_extract(Creative, format="json", map_index="$campaign_ids"),
        ).extract(
            campaign_ids = _configs["campaign_ids"],
        )

    @pytest.mark.coupang_ads
    def test_product_adreport(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable, yesterday: dt.date):
        from linkmerce.core.coupang.advertising.adreport.extract import ProductAdReport
        _configs = options("coupang.advertising.product_adreport")
        ProductAdReport(
            headers = {"cookies": self.cookies(credentials)},
            parser = dump_extract(ProductAdReport, format="xlsx"),
        ).extract(
            start_date = _configs.get("start_date", yesterday),
            end_date = _configs.get("end_date", ":start_date:"),
            date_type = _configs.get("date_type", "daily"),
            report_level = _configs.get("report_level", "vendorItem"),
            campaign_ids = _configs.get("campaign_ids", list()),
            wait_seconds = _configs.get("wait_seconds", 60),
            wait_interval = _configs.get("wait_interval", 1),
        )

    @pytest.mark.coupang_ads
    def test_new_customer_adreport(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable, yesterday: dt.date):
        from linkmerce.core.coupang.advertising.adreport.extract import NewCustomerAdReport
        _configs = options("coupang.advertising.new_customer_adreport")
        NewCustomerAdReport(
            headers = {"cookies": self.cookies(credentials)},
            parser = dump_extract(NewCustomerAdReport, format="xlsx"),
        ).extract(
            start_date = _configs.get("start_date", yesterday),
            end_date = _configs.get("end_date", ":start_date:"),
            date_type = _configs.get("date_type", "daily"),
            report_level = _configs.get("report_level", "vendorItem"),
            campaign_ids = _configs.get("campaign_ids", list()),
            wait_seconds = _configs.get("wait_seconds", 60),
            wait_interval = _configs.get("wait_interval", 1),
        )


###################################################################
######################### Coupang Wing ############################
###################################################################

class TestCoupangWing:
    """쿠팡 윙 데이터 추출 테스트.
    - coupang.wing.product.ProductOption
    - coupang.wing.product.ProductDetail
    - coupang.wing.product.ProductDownload
    - coupang.wing.product.RocketInventory
    - coupang.wing.settlement.RocketSettlement
    - coupang.wing.settlement.RocketSettlementDownload"""

    def cookies(self, reader: YamlReader) -> str:
        return reader("coupang.wing.0")["cookies"]

    @pytest.mark.coupang_wing
    def test_product_option(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.coupang.wing.product.extract import ProductOption
        _configs = options("coupang.wing.product_option")
        ProductOption(
            headers = {"cookies": self.cookies(credentials)},
            parser = dump_extract(ProductOption, format="json"),
        ).extract(
            is_deleted = _configs.get("is_deleted", False),
        )

    @pytest.mark.coupang_wing
    def test_product_detail(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.coupang.wing.product.extract import ProductDetail
        _configs = configs("coupang.wing.product_detail")
        ProductDetail(
            headers = {"cookies": self.cookies(credentials)},
            parser = dump_extract(ProductDetail, format="json", map_index="$vendor_inventory_id"),
        ).extract(
            vendor_inventory_id = _configs["vendor_inventory_id"],
        )

    @pytest.mark.skip
    @pytest.mark.coupang_wing
    def test_product_download(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.coupang.wing.product.extract import ProductDownload
        _configs = options("coupang.wing.product_download")
        ProductDownload(
            headers = {"cookies": self.cookies(credentials)},
            parser = dump_extract(ProductDownload, format="xlsx"),
        ).extract(
            request_type = _configs.get("request_type", "VENDOR_INVENTORY_ITEM"),
            fields = _configs.get("fields", list()),
            is_deleted = _configs.get("is_deleted", False),
            wait_seconds = _configs.get("wait_seconds", 60),
            wait_interval = _configs.get("wait_interval", 1),
        )

    @pytest.mark.coupang_wing
    def test_rocket_inventory(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.coupang.wing.product.extract import RocketInventory
        _configs = options("coupang.wing.rocket_inventory")
        RocketInventory(
            headers = {"cookies": self.cookies(credentials)},
            parser = dump_extract(RocketInventory, format="json"),
        ).extract(
            hidden_status = _configs.get("hidden_status"),
        )

    @pytest.mark.coupang_wing
    def test_rocket_settlement(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable, days_ago: Callable):
        from linkmerce.core.coupang.wing.settlement.extract import RocketSettlement
        _configs = options("coupang.wing.rocket_settlement")
        RocketSettlement(
            headers = {"cookies": self.cookies(credentials)},
            parser = dump_extract(RocketSettlement, format="json"),
        ).extract(
            start_date = _configs.get("start_date", days_ago(7)),
            end_date = _configs.get("end_date", days_ago(1)),
            date_type = _configs.get("date_type", "SALES"),
        )

    @pytest.mark.coupang_wing
    def test_rocket_settlement_download(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable, days_ago: Callable):
        from linkmerce.core.coupang.wing.settlement.extract import RocketSettlementDownload
        _configs = options("coupang.wing.rocket_settlement_download")
        RocketSettlementDownload(
            headers = {"cookies": self.cookies(credentials)},
            parser = dump_extract(RocketSettlementDownload, format="xlsx", map_index="$report_type"),
        ).extract(
            start_date = _configs.get("start_date", days_ago(7)),
            end_date = _configs.get("end_date", days_ago(1)),
            date_type = _configs.get("date_type", "SALES"),
            wait_seconds = _configs.get("wait_seconds", 60),
            wait_interval = _configs.get("wait_interval", 1),
            progress = _configs.get("progress", False),
        )


###################################################################
############################ Ecount ###############################
###################################################################

class TestEcount:
    """이카운트 API 데이터 추출 테스트.
    - ecount.api.inventory.Inventory
    - ecount.api.product.Product"""

    def credentials(self, reader: YamlReader) -> dict:
        _credentials = reader("ecount.api")
        return {
            "com_code": _credentials["com_code"],
            "userid": _credentials["userid"],
            "api_key": _credentials["api_key"],
        }

    @pytest.mark.ecount
    def test_inventory(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.ecount.api.inventory.extract import Inventory
        _configs = options("ecount.api.inventory")
        Inventory(
            configs = self.credentials(credentials),
            parser = dump_extract(Inventory, format="json"),
        ).extract(
            base_date = _configs.get("base_date", ":today:"),
            warehouse_code = _configs.get("warehouse_code"),
            product_code = _configs.get("product_code"),
            zero_yn = _configs.get("zero_yn", False),
            balanced_yn = _configs.get("balanced_yn", False),
            deleted_yn = _configs.get("deleted_yn", False),
            safe_yn = _configs.get("safe_yn", False),
        )

    @pytest.mark.ecount
    def test_product(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.ecount.api.product.extract import Product
        _configs = options("ecount.api.product")
        Product(
            configs = self.credentials(credentials),
            parser = dump_extract(Product, format="json"),
        ).extract(
            product_code = _configs.get("product_code"),
            comma_yn = _configs.get("comma_yn", False),
        )


###################################################################
########################## Google Ads #############################
###################################################################

class TestGoogleAds:
    """구글 광고 API 데이터 추출 테스트.
    - google.api.ads.Campaign
    - google.api.ads.AdGroup
    - google.api.ads.Ad
    - google.api.ads.Insight
    - google.api.ads.Asset
    - google.api.ads.AssetView"""

    def credentials(self, reader: YamlReader, service_account: dict) -> dict:
        _credentials = reader("google.ads_api.0")
        return {
            "customer_id": _credentials["customer_id"],
            "manager_id": _credentials["manager_id"],
            "developer_token": _credentials["developer_token"],
            "service_account": _credentials.get("service_account", service_account),
        }

    @pytest.mark.google_ads
    def test_campaign(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable, service_account: dict):
        from linkmerce.core.google.api.ads.extract import Campaign
        _configs = options("google.api.campaign")
        Campaign(
            configs = self.credentials(credentials, service_account),
            parser = dump_extract(Campaign, format="json"),
        ).extract(
            start_date = _configs.get("start_date"),
            end_date = _configs.get("end_date"),
            date_range = _configs.get("date_range", "LAST_30_DAYS"),
            fields = _configs.get("fields", list()),
        )

    @pytest.mark.google_ads
    def test_ad_group(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable, service_account: dict):
        from linkmerce.core.google.api.ads.extract import AdGroup
        _configs = options("google.api.ad_group")
        AdGroup(
            configs = self.credentials(credentials, service_account),
            parser = dump_extract(AdGroup, format="json"),
        ).extract(
            start_date = _configs.get("start_date"),
            end_date = _configs.get("end_date"),
            date_range = _configs.get("date_range", "LAST_30_DAYS"),
            fields = _configs.get("fields", list()),
        )

    @pytest.mark.google_ads
    def test_ad(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable, service_account: dict):
        from linkmerce.core.google.api.ads.extract import Ad
        _configs = options("google.api.ad")
        Ad(
            configs = self.credentials(credentials, service_account),
            parser = dump_extract(Ad, format="json"),
        ).extract(
            start_date = _configs.get("start_date"),
            end_date = _configs.get("end_date"),
            date_range = _configs.get("date_range", "LAST_30_DAYS"),
            fields = _configs.get("fields", list()),
        )

    @pytest.mark.google_ads
    def test_insight(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable, service_account: dict):
        from linkmerce.core.google.api.ads.extract import Insight
        _configs = options("google.api.insight")
        Insight(
            configs = self.credentials(credentials, service_account),
            parser = dump_extract(Insight, format="json"),
        ).extract(
            start_date = _configs.get("start_date"),
            end_date = _configs.get("end_date"),
            date_freq = _configs.get("date_freq", 'D'),
            date_range = _configs.get("date_range", "LAST_30_DAYS"),
            fields = _configs.get("fields", list()),
        )

    @pytest.mark.google_ads
    def test_asset(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable, service_account: dict):
        from linkmerce.core.google.api.ads.extract import Asset
        _configs = options("google.api.asset")
        Asset(
            configs = self.credentials(credentials, service_account),
            parser = dump_extract(Asset, format="json"),
        ).extract(
            fields = _configs.get("fields", list()),
        )

    @pytest.mark.google_ads
    def test_asset_view(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable, service_account: dict):
        from linkmerce.core.google.api.ads.extract import AssetView
        _configs = options("google.api.asset_view")
        AssetView(
            configs = self.credentials(credentials, service_account),
            parser = dump_extract(AssetView, format="json"),
        ).extract(
            start_date = _configs.get("start_date"),
            end_date = _configs.get("end_date"),
            date_freq = _configs.get("date_freq", 'D'),
            date_range = _configs.get("date_range", "LAST_30_DAYS"),
            fields = _configs.get("fields", list()),
        )


###################################################################
########################### Meta Ads ##############################
###################################################################

class TestMetaAds:
    """메타 광고 API 데이터 추출 테스트.
    - meta.api.ads.Campaigns
    - meta.api.ads.Adsets
    - meta.api.ads.Ads
    - meta.api.ads.Insights"""

    def credentials(self, reader: YamlReader) -> dict:
        _credentials = reader("meta.marketing_api.0")
        return {
            "app_id": _credentials["app_id"],
            "app_secret": _credentials.get("app_secret", str()),
            "access_token": _credentials.get("access_token", str()),
        }

    @pytest.mark.meta_ads
    def test_campaigns(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.meta.api.ads.extract import Campaigns
        _configs = options("meta.api.campaigns")
        Campaigns(
            configs = self.credentials(credentials),
            parser = dump_extract(Campaigns, format="json"),
        ).extract(
            start_date = _configs.get("start_date"),
            end_date = _configs.get("end_date"),
            account_ids = _configs.get("account_ids", list()),
            fields = _configs.get("fields", list()),
        )

    @pytest.mark.meta_ads
    def test_adsets(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.meta.api.ads.extract import Adsets
        _configs = options("meta.api.adsets")
        Adsets(
            configs = self.credentials(credentials),
            parser = dump_extract(Adsets, format="json"),
        ).extract(
            start_date = _configs.get("start_date"),
            end_date = _configs.get("end_date"),
            account_ids = _configs.get("account_ids", list()),
            fields = _configs.get("fields", list()),
        )

    @pytest.mark.meta_ads
    def test_ads(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.meta.api.ads.extract import Ads
        _configs = options("meta.api.ads")
        Ads(
            configs = self.credentials(credentials),
            parser = dump_extract(Ads, format="json"),
        ).extract(
            start_date = _configs.get("start_date"),
            end_date = _configs.get("end_date"),
            account_ids = _configs.get("account_ids", list()),
            fields = _configs.get("fields", list()),
        )

    @pytest.mark.meta_ads
    def test_insights(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable, yesterday: dt.date):
        from linkmerce.core.meta.api.ads.extract import Insights
        _configs = options("meta.api.insights")
        Insights(
            configs = self.credentials(credentials),
            parser = dump_extract(Insights, format="json"),
        ).extract(
            ad_level = _configs.get("ad_level", "ad"),
            start_date = _configs.get("start_date", yesterday),
            end_date = _configs.get("end_date", ":start_date:"),
            date_type = _configs.get("date_type", "daily"),
            account_ids = _configs.get("account_ids", list()),
            fields = _configs.get("fields", list()),
        )


###################################################################
########################## Naver Main #############################
###################################################################

class TestNaverSearch:
    """네이버 통합검색 데이터 추출 테스트.
    - naver.main.search.Search
    - naver.main.search.SearchTab
    - naver.main.search.CafeArticle"""

    @pytest.mark.naver_search
    def test_search(self, configs: YamlReader, dump_extract: Callable):
        from linkmerce.core.naver.main.search.extract import Search
        _configs = configs("naver.main.search")
        Search(
            parser = dump_extract(Search, format="html", map_index="$query"),
        ).extract(
            query = _configs["query"],
            mobile = _configs.get("mobile", True),
            parse_html = _configs.get("parse_html", False),
        )

    @pytest.mark.naver_search
    def test_search_tab(self, configs: YamlReader, dump_extract: Callable):
        from linkmerce.core.naver.main.search.extract import SearchTab
        _configs = configs("naver.main.search_tab")
        SearchTab(
            parser = dump_extract(SearchTab, format="html", map_index="$query"),
        ).extract(
            query = _configs["query"],
            tab_type = _configs.get("tab_type", "cafe"),
            mobile = _configs.get("mobile", True),
        )

    @pytest.mark.naver_search
    def test_cafe_article(self, configs: YamlReader, dump_extract: Callable):
        from linkmerce.core.naver.main.search.extract import CafeArticle
        _configs = configs("naver.main.cafe_article")
        CafeArticle(
            parser = dump_extract(CafeArticle, format="json"),
        ).extract(
            url = _configs["url"],
            domain = _configs.get("domain", "article"),
        )


###################################################################
########################## Naver Open API #########################
###################################################################

class TestNaverOpenApi:
    """네이버 오픈 API 검색 데이터 추출 테스트.
    - naver.openapi.search.BlogSearch
    - naver.openapi.search.NewsSearch
    - naver.openapi.search.BookSearch
    - naver.openapi.search.CafeSearch
    - naver.openapi.search.KiNSearch
    - naver.openapi.search.ImageSearch
    - naver.openapi.search.ShoppingSearch"""

    def credentials(self, reader: YamlReader) -> dict:
        _credentials = reader("naver.openapi.0")
        return {
            "client_id": _credentials["client_id"],
            "client_secret": _credentials["client_secret"],
        }

    @pytest.mark.naver_open_api
    def test_blog_search(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.naver.openapi.search.extract import BlogSearch
        _configs = configs("naver.openapi.blog_search")
        BlogSearch(
            configs = self.credentials(credentials),
            parser = dump_extract(BlogSearch, format="json", map_index="$query"),
        ).extract(
            query = _configs["query"],
            start = _configs.get("start", 1),
            display = _configs.get("display", 100),
            sort = _configs.get("sort", "sim"),
        )

    @pytest.mark.naver_open_api
    def test_news_search(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.naver.openapi.search.extract import NewsSearch
        _configs = configs("naver.openapi.news_search")
        NewsSearch(
            configs = self.credentials(credentials),
            parser = dump_extract(NewsSearch, format="json", map_index="$query"),
        ).extract(
            query = _configs["query"],
            start = _configs.get("start", 1),
            display = _configs.get("display", 100),
            sort = _configs.get("sort", "sim"),
        )

    @pytest.mark.naver_open_api
    def test_book_search(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.naver.openapi.search.extract import BookSearch
        _configs = configs("naver.openapi.book_search")
        BookSearch(
            configs = self.credentials(credentials),
            parser = dump_extract(BookSearch, format="json", map_index="$query"),
        ).extract(
            query = _configs["query"],
            start = _configs.get("start", 1),
            display = _configs.get("display", 100),
            sort = _configs.get("sort", "sim"),
        )

    @pytest.mark.naver_open_api
    def test_cafe_search(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.naver.openapi.search.extract import CafeSearch
        _configs = configs("naver.openapi.cafe_search")
        CafeSearch(
            configs = self.credentials(credentials),
            parser = dump_extract(CafeSearch, format="json", map_index="$query"),
        ).extract(
            query = _configs["query"],
            start = _configs.get("start", 1),
            display = _configs.get("display", 100),
            sort = _configs.get("sort", "sim"),
        )

    @pytest.mark.naver_open_api
    def test_kin_search(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.naver.openapi.search.extract import KiNSearch
        _configs = configs("naver.openapi.kin_search")
        KiNSearch(
            configs = self.credentials(credentials),
            parser = dump_extract(KiNSearch, format="json", map_index="$query"),
        ).extract(
            query = _configs["query"],
            start = _configs.get("start", 1),
            display = _configs.get("display", 100),
            sort = _configs.get("sort", "sim"),
        )

    @pytest.mark.naver_open_api
    def test_image_search(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.naver.openapi.search.extract import ImageSearch
        _configs = configs("naver.openapi.image_search")
        ImageSearch(
            configs = self.credentials(credentials),
            parser = dump_extract(ImageSearch, format="json", map_index="$query"),
        ).extract(
            query = _configs["query"],
            start = _configs.get("start", 1),
            display = _configs.get("display", 100),
            sort = _configs.get("sort", "sim"),
            filter = _configs.get("filter", "all"),
        )

    @pytest.mark.naver_open_api
    def test_shopping_search(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.naver.openapi.search.extract import ShoppingSearch
        _configs = configs("naver.openapi.shopping_search")
        ShoppingSearch(
            configs = self.credentials(credentials),
            parser = dump_extract(ShoppingSearch, format="json", map_index="$query"),
        ).extract(
            query = _configs["query"],
            start = _configs.get("start", 1),
            display = _configs.get("display", 100),
            sort = _configs.get("sort", "sim"),
        )


###################################################################
########################## Sabangnet ##############################
###################################################################

class TestSabangnet:
    """사방넷 데이터 추출 테스트.
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

    def credentials(self, reader: YamlReader) -> dict:
        _credentials = reader("sabangnet.admin")
        return {
            "userid": _credentials["userid"],
            "passwd": _credentials["passwd"],
            "domain": _credentials["domain"],
        }

    @pytest.mark.sabangnet
    def test_order(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable, yesterday: dt.date):
        from linkmerce.core.sabangnet.admin.order.extract import Order
        _configs = options("sabangnet.admin.order")
        Order(
            configs = self.credentials(credentials),
            parser = dump_extract(Order, format="json"),
        ).extract(
            start_date = _configs.get("start_date", yesterday),
            end_date = _configs.get("end_date", ":start_date:"),
            date_type = _configs.get("date_type", "reg_dm"),
            order_status_div = _configs.get("order_status_div", str()),
            order_status = _configs.get("order_status", list()),
            shop_id = _configs.get("shop_id", str()),
            sort_type = _configs.get("sort_type", "ord_no_asc"),
        )

    @pytest.mark.sabangnet
    def test_order_download(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable, yesterday: dt.date):
        from linkmerce.core.sabangnet.admin.order.extract import OrderDownload
        _configs = configs("sabangnet.admin.order_download")
        for name, no in _configs["download_no"].items():
            OrderDownload(
                configs = self.credentials(credentials),
                parser = dump_extract(OrderDownload, format="xlsx", map_index=name),
            ).extract(
                download_no = no,
                start_date = _configs.get("start_date", yesterday),
                end_date = _configs.get("end_date", ":start_date:"),
                date_type = _configs.get("date_type", "reg_dm"),
                order_seq = _configs.get("order_seq", list()),
                order_status_div = _configs.get("order_status_div", str()),
                order_status = _configs.get("order_status", list()),
                shop_id = _configs.get("shop_id", str()),
                sort_type = _configs.get("sort_type", "ord_no_asc"),
            )

    @pytest.mark.skip
    @pytest.mark.sabangnet
    def test_order_status(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable, yesterday: dt.date):
        from linkmerce.core.sabangnet.admin.order.extract import OrderStatus
        _configs = configs("sabangnet.admin.order_status")
        OrderStatus(
            configs = self.credentials(credentials),
            parser = dump_extract(OrderStatus, format="xlsx", map_index="$date_type"),
        ).extract(
            download_no = _configs["download_no"],
            start_date = _configs.get("start_date", yesterday),
            end_date = _configs.get("end_date", ":start_date:"),
            date_type = _configs.get("date_type", ["delivery_confirm_date", "cancel_dt", "rtn_dt", "chng_dt"]),
            order_seq = _configs.get("order_seq", list()),
            order_status_div = _configs.get("order_status_div", str()),
            order_status = _configs.get("order_status", list()),
            shop_id = _configs.get("shop_id", str()),
            sort_type = _configs.get("sort_type", "ord_no_asc"),
        )

    @pytest.mark.sabangnet
    def test_product_mapping(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.sabangnet.admin.order.extract import ProductMapping
        _configs = options("sabangnet.admin.product_mapping")
        ProductMapping(
            configs = self.credentials(credentials),
            parser = dump_extract(ProductMapping, format="json"),
        ).extract(
            start_date = _configs.get("start_date", ":base_date:"),
            end_date = _configs.get("end_date", ":today:"),
            shop_id = _configs.get("shop_id", str()),
        )

    @pytest.mark.sabangnet
    def test_sku_mapping(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.sabangnet.admin.order.extract import SkuMapping
        _configs = configs("sabangnet.admin.sku_mapping")
        SkuMapping(
            configs = self.credentials(credentials),
            parser = dump_extract(SkuMapping, format="json"),
        ).extract(
            query = {
                "product_id_shop": _configs["product_id_shop"],
                "shop_id": _configs["shop_id"],
                "product_id": _configs["product_id"],
            },
        )

    @pytest.mark.sabangnet
    def test_product(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.sabangnet.admin.product.extract import Product
        _configs = options("sabangnet.admin.product")
        Product(
            configs = self.credentials(credentials),
            parser = dump_extract(Product, format="json"),
        ).extract(
            start_date = _configs.get("start_date", ":base_date:"),
            end_date = _configs.get("end_date", ":today:"),
            date_type = _configs.get("date_type", "001"),
            sort_type = _configs.get("sort_type", "001"),
            sort_asc = _configs.get("sort_asc", True),
            is_deleted = _configs.get("is_deleted", False),
            product_status = _configs.get("product_status"),
        )

    @pytest.mark.sabangnet
    def test_option(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.sabangnet.admin.product.extract import Option
        _configs = configs("sabangnet.admin.option")
        Option(
            configs = self.credentials(credentials),
            parser = dump_extract(Option, format="json", map_index="$product_id"),
        ).extract(
            product_id = _configs["product_id"],
        )

    @pytest.mark.sabangnet
    def test_option_download(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.sabangnet.admin.product.extract import OptionDownload
        _configs = options("sabangnet.admin.option_download")
        OptionDownload(
            configs = self.credentials(credentials),
            parser = dump_extract(OptionDownload, format="xlsx"),
        ).extract(
            start_date = _configs.get("start_date", ":base_date:"),
            end_date = _configs.get("end_date", ":today:"),
            date_type = _configs.get("date_type", "prdFstRegsDt"),
            sort_type = _configs.get("sort_type", "prdNo"),
            sort_asc = _configs.get("sort_asc", True),
            is_deleted = _configs.get("is_deleted", False),
            product_status = _configs.get("product_status", list()),
        )

    @pytest.mark.sabangnet
    def test_add_product_group(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.sabangnet.admin.product.extract import AddProductGroup
        _configs = options("sabangnet.admin.add_product_group")
        AddProductGroup(
            configs = self.credentials(credentials),
            parser = dump_extract(AddProductGroup, format="json"),
        ).extract(
            start_date = _configs.get("start_date", ":base_date:"),
            end_date = _configs.get("end_date", ":today:"),
            shop_id = _configs.get("shop_id", str()),
        )

    @pytest.mark.sabangnet
    def test_add_product(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.sabangnet.admin.product.extract import AddProduct
        _configs = configs("sabangnet.admin.add_product")
        AddProduct(
            configs = self.credentials(credentials),
            parser = dump_extract(AddProduct, format="json", map_index="$group_id"),
        ).extract(
            group_id = _configs["group_id"],
        )


###################################################################
######################### SearchAd API ############################
###################################################################

class TestSearchAdApi:
    """네이버 검색광고 API 데이터 추출 테스트.
    - searchad.api.adreport.Campaign
    - searchad.api.adreport.Adgroup
    - searchad.api.adreport.Ad
    - searchad.api.contract.TimeContract
    - searchad.api.contract.BrandNewContract
    - searchad.api.keyword.Keyword"""

    def credentials(self, reader: YamlReader) -> dict:
        _credentials = reader("searchad.api.0")
        return {
            "api_key": _credentials["api_key"],
            "secret_key": _credentials["secret_key"],
            "customer_id": _credentials["customer_id"],
        }

    @pytest.mark.searchad_api
    def test_campaign(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.searchad.api.adreport.extract import Campaign
        _configs = options("searchad.api.campaign")
        Campaign(
            configs = self.credentials(credentials),
            parser = dump_extract(Campaign, format="tsv"),
        ).extract(
            from_date = _configs.get("from_date"),
        )

    @pytest.mark.searchad_api
    def test_adgroup(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.searchad.api.adreport.extract import Adgroup
        _configs = options("searchad.api.adgroup")
        Adgroup(
            configs = self.credentials(credentials),
            parser = dump_extract(Adgroup, format="tsv"),
        ).extract(
            from_date = _configs.get("from_date"),
        )

    @pytest.mark.searchad_api
    def test_ad(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.searchad.api.adreport.extract import Ad
        _configs = options("searchad.api.ad")
        _parser = dump_extract(Ad, format="tsv", map_index="$report_type")

        def custom_dump(response: dict[str, str], *args, **kwargs):
            for report_type, tsv_data in response.items():
                _parser(tsv_data if tsv_data else str(), *args, report_type=report_type, **kwargs)

        Ad(
            configs = self.credentials(credentials),
            parser = custom_dump,
        ).extract(
            from_date = _configs.get("from_date"),
        )

    @pytest.mark.searchad_api
    def test_time_contract(self, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.searchad.api.contract.extract import TimeContract
        TimeContract(
            configs = self.credentials(credentials),
            parser = dump_extract(TimeContract, format="json"),
        ).extract()

    @pytest.mark.searchad_api
    def test_brand_new_contract(self, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.searchad.api.contract.extract import BrandNewContract
        BrandNewContract(
            configs = self.credentials(credentials),
            parser = dump_extract(BrandNewContract, format="json"),
        ).extract()

    @pytest.mark.searchad_api
    def test_keyword(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.searchad.api.keyword.extract import Keyword
        _configs = configs("searchad.api.keyword")
        Keyword(
            configs = self.credentials(credentials),
            parser = dump_extract(Keyword, format="json"),
        ).extract(
            keywords = _configs["keywords"],
            max_rank = _configs.get("max_rank"),
            show_detail = _configs.get("show_detail", True),
        )


###################################################################
######################### SearchAd GFA ############################
###################################################################

class TestSearchAdGfa:
    """네이버 GFA 데이터 추출 테스트.
    - searchad.gfa.adreport.Campaign
    - searchad.gfa.adreport.AdSet
    - searchad.gfa.adreport.Creative
    - searchad.gfa.adreport.CampaignReport
    - searchad.gfa.adreport.CreativeReport"""

    def credentials(self, reader: YamlReader) -> dict:
        _credentials = reader("searchad.gfa.0")
        return dict(
            configs = {"account_no": _credentials["account_no"]},
            headers = {"cookies": _credentials["cookies"]},
        )

    @pytest.mark.searchad_gfa
    def test_campaign(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.searchad.gfa.adreport.extract import Campaign
        _configs = options("searchad.gfa.campaign")
        Campaign(
            **self.credentials(credentials),
            parser = dump_extract(Campaign, format="json", map_index="$status"),
        ).extract(
            status = _configs.get("status", "RUNNABLE"),
        )

    @pytest.mark.searchad_gfa
    def test_ad_set(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.searchad.gfa.adreport.extract import AdSet
        _configs = options("searchad.gfa.ad_set")
        AdSet(
            **self.credentials(credentials),
            parser = dump_extract(AdSet, format="json", map_index="$status"),
        ).extract(
            status = _configs.get("status", "ALL"),
        )

    @pytest.mark.searchad_gfa
    def test_creative(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.searchad.gfa.adreport.extract import Creative
        _configs = options("searchad.gfa.creative")
        Creative(
            **self.credentials(credentials),
            parser = dump_extract(Creative, format="json", map_index="$status"),
        ).extract(
            status = _configs.get("status", "ALL"),
        )

    @pytest.mark.searchad_gfa
    def test_campaign_report(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable, yesterday: dt.date):
        from linkmerce.core.searchad.gfa.adreport.extract import CampaignReport
        _configs = options("searchad.gfa.campaign_report")
        CampaignReport(
            **self.credentials(credentials),
            parser = dump_extract(CampaignReport, format="zip"),
        ).extract(
            start_date = _configs.get("start_date", yesterday),
            end_date = _configs.get("end_date", ":start_date:"),
            date_type = _configs.get("date_type", "DAY"),
            columns = _configs.get("columns", ":default:"),
            wait_seconds = _configs.get("wait_seconds", 60),
            wait_interval = _configs.get("wait_interval", 1),
            progress = _configs.get("progress", True),
        )

    @pytest.mark.searchad_gfa
    def test_creative_report(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable, yesterday: dt.date):
        from linkmerce.core.searchad.gfa.adreport.extract import CreativeReport
        _configs = options("searchad.gfa.creative_report")
        CreativeReport(
            **self.credentials(credentials),
            parser = dump_extract(CreativeReport, format="zip"),
        ).extract(
            start_date = _configs.get("start_date", yesterday),
            end_date = _configs.get("end_date", ":start_date:"),
            date_type = _configs.get("date_type", "DAY"),
            columns = _configs.get("columns", ":default:"),
            wait_seconds = _configs.get("wait_seconds", 60),
            wait_interval = _configs.get("wait_interval", 1),
            progress = _configs.get("progress", True),
        )


###################################################################
######################## SearchAd Manage ##########################
###################################################################

class TestSearchAdManage:
    """네이버 검색광고 데이터 추출 테스트.
    - searchad.manage.adreport.DailyReport
    - searchad.manage.exposure.ExposureDiagnosis"""

    def credentials(self, reader: YamlReader) -> dict:
        _credentials = reader("searchad.manage.0")
        return dict(
            configs = {"customer_id": _credentials["customer_id"]},
            headers = {"cookies": _credentials["cookies"]},
        )

    @pytest.mark.searchad_manage
    def test_daily_report(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable, yesterday: dt.date):
        from linkmerce.core.searchad.manage.adreport.extract import DailyReport
        _configs = configs("searchad.manage.daily_report")
        DailyReport(
            **self.credentials(credentials),
            parser = dump_extract(DailyReport, format="csv"),
        ).extract(
            report_id = _configs["report_id"],
            report_name = _configs["report_name"],
            userid = _configs["userid"],
            start_date = _configs.get("start_date", yesterday),
            end_date = _configs.get("end_date", ":start_date:"),
        )

    @pytest.mark.searchad_manage
    def test_exposure_diagnosis(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.searchad.manage.exposure.extract import ExposureDiagnosis
        _configs = configs("searchad.manage.exposure_diagnosis")
        ExposureDiagnosis(
            **self.credentials(credentials),
            parser = dump_extract(ExposureDiagnosis, format="json", map_index="$keyword"),
        ).extract(
            keyword = _configs["keyword"],
            domain = _configs.get("domain", "search"),
            mobile = _configs.get("mobile", True),
            is_own = _configs.get("is_own"),
        )


###################################################################
######################## SmartStore API ###########################
###################################################################

class TestSmartstoreApi:
    """스마트스토어 커머스 API 데이터 추출 테스트.
    - smartstore.api.product.Product
    - smartstore.api.product.Option
    - smartstore.api.order.Order
    - smartstore.api.order.OrderStatus"""

    def credentials(self, reader: YamlReader) -> dict:
        _credentials = reader("smartstore.api.0")
        return {
            "client_id": _credentials["client_id"],
            "client_secret": _credentials["client_secret"],
        }

    @pytest.mark.smartstore_api
    def test_product(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.smartstore.api.product.extract import Product
        _configs = options("smartstore.api.product")
        Product(
            configs = self.credentials(credentials),
            parser = dump_extract(Product, format="json"),
        ).extract(
            search_keyword = _configs.get("search_keyword", list()),
            keyword_type = _configs.get("keyword_type", "CHANNEL_PRODUCT_NO"),
            status_type = _configs.get("status_type", ["SALE"]),
            period_type = _configs.get("period_type", "PROD_REG_DAY"),
            from_date = _configs.get("from_date"),
            to_date = _configs.get("to_date"),
            max_retries = _configs.get("max_retries", 5),
        )

    @pytest.mark.smartstore_api
    def test_option(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.smartstore.api.product.extract import Option
        _configs = configs("smartstore.api.option")
        Option(
            configs = self.credentials(credentials),
            parser = dump_extract(Option, format="json", map_index="$product_id"),
        ).extract(
            product_id = _configs["product_id"],
            max_retries = _configs.get("max_retries", 5),
        )

    @pytest.mark.smartstore_api
    def test_order(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable, yesterday: dt.date):
        from linkmerce.core.smartstore.api.order.extract import Order
        _configs = options("smartstore.api.order")
        Order(
            configs = self.credentials(credentials),
            parser = dump_extract(Order, format="json"),
        ).extract(
            start_date = _configs.get("start_date", yesterday),
            end_date = _configs.get("end_date", ":start_date:"),
            range_type = _configs.get("range_type", "PAYED_DATETIME"),
            product_order_status = _configs.get("product_order_status", list()),
            claim_status = _configs.get("claim_status", list()),
            place_order_status = _configs.get("place_order_status", list()),
            page_start = _configs.get("page_start", 1),
            max_retries = _configs.get("max_retries", 5),
        )

    @pytest.mark.smartstore_api
    def test_order_status(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable, yesterday: dt.date):
        from linkmerce.core.smartstore.api.order.extract import OrderStatus
        _configs = options("smartstore.api.order_status")
        OrderStatus(
            configs = self.credentials(credentials),
            parser = dump_extract(OrderStatus, format="json"),
        ).extract(
            start_date = _configs.get("start_date", yesterday),
            end_date = _configs.get("end_date", ":start_date:"),
            last_changed_type = _configs.get("last_changed_type"),
            max_retries = _configs.get("max_retries", 5),
        )


###################################################################
###################### SmartStore Bizdata API #####################
###################################################################

class TestBizdataApi:
    """스마트스토어 API데이터솔루션(통계) 데이터 추출 테스트.
    - smartstore.api.bizdata.MarketingChannel"""

    def credentials(self, reader: YamlReader) -> dict:
        _credentials = reader("smartstore.bizdata.0")
        return {
            "client_id": _credentials["client_id"],
            "client_secret": _credentials["client_secret"],
        }

    def channel_seq(self, reader: YamlReader) -> int | str:
        return reader("smartstore.bizdata.0")["channel_seq"]

    @pytest.mark.bizdata_api
    def test_marketing_channel(self, options: YamlReader, credentials: YamlReader, dump_extract: Callable, yesterday: dt.date):
        from linkmerce.core.smartstore.api.bizdata.extract import MarketingChannel
        _configs = options("smartstore.bizdata.marketing_channel")
        MarketingChannel(
            configs = self.credentials(credentials),
            parser = dump_extract(MarketingChannel, format="json"),
        ).extract(
            channel_seq = self.channel_seq(credentials),
            start_date = _configs.get("start_date", yesterday),
            end_date = _configs.get("end_date", ":start_date:"),
            max_retries = _configs.get("max_retries", 5),
        )


###################################################################
################## Naver Shopping Partner Center ##################
###################################################################

class TestPartnerCenter:
    """네이버 쇼핑파트너센터 데이터 추출 테스트.
    - smartstore.hcenter.catalog.BrandCatalog
    - smartstore.hcenter.catalog.BrandProduct
    - smartstore.hcenter.pageview.PageViewByDevice
    - smartstore.hcenter.pageview.PageViewByUrl
    - smartstore.hcenter.sales.StoreSales
    - smartstore.hcenter.sales.CategorySales
    - smartstore.hcenter.sales.ProductSales"""

    eol_date = dt.date(2026, 2, 26)

    def headers(self, reader: YamlReader) -> dict:
        _credentials = reader("smartstore.hcenter")
        return {"cookies": _credentials["cookies"]}

    @pytest.mark.smartstore_hcenter
    def test_brand_catalog(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.smartstore.hcenter.catalog.extract import BrandCatalog
        _configs = configs("smartstore.hcenter.brand_catalog")
        BrandCatalog(
            headers = self.headers(credentials),
            parser = dump_extract(BrandCatalog, format="json", map_index="$brand_ids"),
        ).extract(
            brand_ids = _configs["brand_ids"],
            sort_type = _configs.get("sort_type", "recent"),
            is_brand_catalog = _configs.get("is_brand_catalog"),
            page = _configs.get("page", 0),
            page_size = _configs.get("page_size", 10),
        )

    @pytest.mark.smartstore_hcenter
    def test_brand_product(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.smartstore.hcenter.catalog.extract import BrandProduct
        _configs = configs("smartstore.hcenter.brand_product")
        BrandProduct(
            headers = self.headers(credentials),
            parser = dump_extract(BrandProduct, format="json", map_index="$brand_ids"),
        ).extract(
            brand_ids = _configs["brand_ids"],
            mall_seq = _configs.get("mall_seq"),
            sort_type = _configs.get("sort_type", "recent"),
            is_brand_catalog = _configs.get("is_brand_catalog"),
            page = _configs.get("page", 0),
            page_size = _configs.get("page_size", 10),
        )

    @pytest.mark.smartstore_hcenter
    def test_page_view_by_device(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.smartstore.hcenter.pageview.extract import PageViewByDevice
        _configs = configs("smartstore.hcenter.page_view_by_device")
        PageViewByDevice(
            headers = self.headers(credentials),
            parser = dump_extract(PageViewByDevice, format="json", map_index="$mall_seq"),
        ).extract(
            mall_seq = _configs["mall_seq"],
            start_date = _configs.get("start_date", self.eol_date),
            end_date = _configs.get("end_date", ":start_date:"),
        )

    @pytest.mark.smartstore_hcenter
    def test_page_view_by_url(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.smartstore.hcenter.pageview.extract import PageViewByUrl
        _configs = configs("smartstore.hcenter.page_view_by_url")
        PageViewByUrl(
            headers = self.headers(credentials),
            parser = dump_extract(PageViewByUrl, format="json", map_index="$mall_seq"),
        ).extract(
            mall_seq = _configs["mall_seq"],
            start_date = _configs.get("start_date", self.eol_date),
            end_date = _configs.get("end_date", ":start_date:"),
        )

    @pytest.mark.smartstore_hcenter
    def test_store_sales(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.smartstore.hcenter.sales.extract import StoreSales
        _configs = configs("smartstore.hcenter.store_sales")
        StoreSales(
            headers = self.headers(credentials),
            parser = dump_extract(StoreSales, format="json", map_index="$mall_seq"),
        ).extract(
            mall_seq = _configs["mall_seq"],
            start_date = _configs.get("start_date", self.eol_date),
            end_date = _configs.get("end_date", ":start_date:"),
            date_type = _configs.get("date_type", "daily"),
            page = _configs.get("page", 1),
            page_size = _configs.get("page_size", 1000),
        )

    @pytest.mark.smartstore_hcenter
    def test_category_sales(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.smartstore.hcenter.sales.extract import CategorySales
        _configs = configs("smartstore.hcenter.category_sales")
        CategorySales(
            headers = self.headers(credentials),
            parser = dump_extract(CategorySales, format="json", map_index="$mall_seq"),
        ).extract(
            mall_seq = _configs["mall_seq"],
            start_date = _configs.get("start_date", self.eol_date),
            end_date = _configs.get("end_date", ":start_date:"),
            date_type = _configs.get("date_type", "daily"),
            page = _configs.get("page", 1),
            page_size = _configs.get("page_size", 1000),
        )

    @pytest.mark.smartstore_hcenter
    def test_product_sales(self, configs: YamlReader, credentials: YamlReader, dump_extract: Callable):
        from linkmerce.core.smartstore.hcenter.sales.extract import ProductSales
        _configs = configs("smartstore.hcenter.product_sales")
        ProductSales(
            headers = self.headers(credentials),
            parser = dump_extract(ProductSales, format="json", map_index="$mall_seq"),
        ).extract(
            mall_seq = _configs["mall_seq"],
            start_date = _configs.get("start_date", self.eol_date),
            end_date = _configs.get("end_date", ":start_date:"),
            date_type = _configs.get("date_type", "daily"),
            page = _configs.get("page", 1),
            page_size = _configs.get("page_size", 1000),
        )
